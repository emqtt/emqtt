%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc Topic index for matching topics to topic filters.
%%
%% Works on top of a ordered collection data set, such as ETS ordered_set table.
%% Keys are tuples constructed from parsed topic filters and record IDs,
%% wrapped in a tuple to order them strictly greater than unit tuple (`{}`).
%% Existing table may be used if existing keys will not collide with index keys.
%%
%% Designed to effectively answer questions like:
%% 1. Does any topic filter match given topic?
%% 2. Which records are associated with topic filters matching given topic?
%% 3. Which topic filters match given topic?
%% 4. Which record IDs are associated with topic filters matching given topic?
%%
%% Trie-search algorithm:
%%
%% Given a 3-level topic (e.g. a/b/c), if we leave out '#' for now,
%% all possible subscriptions of a/b/c can be enumerated as below:
%%
%% a/b/c
%% a/b/+
%% a/+/c <--- subscribed
%% a/+/+
%% +/b/c <--- subscribed
%% +/b/+
%% +/+/c
%% +/+/+ <--- start searching upward from here
%%
%% Let's name this search space "Space1".
%% If we brute-force it, the scope would be 8 (2^3).
%% Meaning this has O(2^N) complexity (N being the level of topics).
%%
%% This clearly isn't going to work.
%% Should we then try to enumerate all subscribers instead?
%% If there are also other subscriptions, e.g. "+/x/y" and "+/b/0"
%%
%% a/+/c <--- match of a/b/c
%% +/x/n
%% ...
%% +/x/2
%% +/x/1
%% +/b/c <--- match of a/b/c
%% +/b/1
%% +/b/0
%%
%% Let's name it "Space2".
%%
%% This has O(M * L) complexity (M being the total number of subscriptions,
%% and L being the number of topic levels).
%% This is usually a lot smaller than "Space1", but still not very effective
%% if the collection size is e.g. 1 million.
%%
%% To make it more effective, we'll need to combine the two algorithms:
%% Use the ordered subscription topics' prefixes as starting points to make
%% guesses about whether or not the next word can be a '+', and skip-over
%% to the next possible match.
%%
%% NOTE: A prerequisite of the ordered collection is, it should be able
%% to find the *immediate-next* topic/filter with a given prefix.
%%
%% In the above example, we start from "+/b/0". When comparing "+/b/0"
%% with "a/b/c", we know the matching prefix is "+/b", meaning we can
%% start guessing if the next word is '+' or 'c':
%%   * It can't be '+' because '+' < '0'
%%   * It might be 'c' because 'c' > '0'
%%
%% So, we try to jump to the next topic which has a prefix of "+/b/c"
%% (this effectively means skipping over "+/b/1").
%%
%% After "+/b/c" is found to be a matching filter, we move up:
%%   * The next possible match is "a/+/+" according to Space1
%%   * The next subscription is "+/x/1" according to Space2
%%
%% "a/+/+" is lexicographically greater than "+/x/+", so let's jump to
%% the immediate-next of 'a/+/+', which is "a/+/c", allowing us to skip
%% over all the ones starting with "+/x".
%%
%% If we take '#' into consideration, it's only one extra comparison to see
%% if a filter ends with '#'.
%%
%% In summary, the complexity of this algorithm is O(N * L)
%% N being the number of total matches, and L being the level of the topic.

-module(emqx_trie_search).

-export([make_key/2]).
-export([match/2, matches/3, get_id/1, get_topic/1]).
-export_type([key/1, word/0, nextf/0, opts/0]).

-define(END, '$end_of_table').

-type word() :: binary() | '+' | '#'.
-type base_key() :: {binary() | [word()], {}}.
-type key(ID) :: {binary() | [word()], {ID}}.
-type nextf() :: fun((key(_) | base_key()) -> ?END | key(_)).
-type opts() :: [unique | return_first].

%% @doc Make a search-key for the given topic.
-spec make_key(emqx_types:topic(), ID) -> key(ID).
make_key(Topic, ID) when is_binary(Topic) ->
    Words = words(Topic),
    case emqx_topic:wildcard(Words) of
        true ->
            %% it's a wildcard
            {Words, {ID}};
        false ->
            %% Not a wildcard. We do not split the topic
            %% because they can be found with direct lookups.
            %% it is also more compact in memory.
            {Topic, {ID}}
    end.

%% @doc Extract record ID from the match.
-spec get_id(key(ID)) -> ID.
get_id({_Filter, {ID}}) ->
    ID.

%% @doc Extract topic (or topic filter) from the match.
-spec get_topic(key(_ID)) -> emqx_types:topic().
get_topic({Filter, _ID}) when is_list(Filter) ->
    emqx_topic:join(Filter);
get_topic({Topic, _ID}) ->
    Topic.

-compile({inline, [base/1, move_up/2, match_add/2, compare/3]}).

%% Make the base-key which can be used to locate the desired search target.
base(Prefix) ->
    {Prefix, {}}.

base_init([W = <<"$", _/bytes>> | _]) ->
    base([W]);
base_init(_) ->
    base([]).

%% Move the search target to the key next to the given Base.
move_up(NextF, Base) ->
    NextF(Base).

%% @doc Match given topic against the index and return the first match, or `false` if
%% no match is found.
-spec match(emqx_types:topic(), nextf()) -> false | key(_).
match(Topic, NextF) ->
    try search(Topic, NextF, [return_first]) of
        _ -> false
    catch
        throw:{first, Res} ->
            Res
    end.

%% @doc Match given topic against the index and return _all_ matches.
%% If `unique` option is given, return only unique matches by record ID.
-spec matches(emqx_types:topic(), nextf(), opts()) -> [key(_)].
matches(Topic, NextF, Opts) ->
    search(Topic, NextF, Opts).

%% @doc Entrypoint of the search for a given topic.
search(Topic, NextF, Opts) ->
    Words = words(Topic),
    Base = base_init(Words),
    ORetFirst = proplists:get_bool(return_first, Opts),
    OUnique = proplists:get_bool(unique, Opts),
    Acc0 =
        case ORetFirst of
            true ->
                first;
            false when OUnique ->
                #{};
            false ->
                []
        end,
    Matches =
        case search_new(Words, Base, NextF, Acc0) of
            {Cursor, Acc} ->
                match_topics(Topic, Cursor, NextF, Acc);
            Acc ->
                Acc
        end,
    case is_map(Matches) of
        true ->
            maps:values(Matches);
        false ->
            Matches
    end.

%% The recursive entrypoint of the trie-search algorithm.
%% Always start from the initial words.
search_new(Words0, NewBase, NextF, Acc) ->
    case move_up(NextF, NewBase) of
        ?END ->
            Acc;
        Cursor ->
            search_up(Words0, Cursor, NextF, Acc)
    end.

%% Search to the 'higher' end of ordered collection of topics and topic-filters.
search_up(Words, {Filter, _} = Cursor, NextF, Acc) ->
    case compare(Filter, Words, []) of
        match_full ->
            search_new(Words, Cursor, NextF, match_add(Cursor, Acc));
        match_prefix ->
            search_new(Words, Cursor, NextF, Acc);
        lower ->
            {Cursor, Acc};
        UpFromPrefix ->
            % NOTE
            % This is a seek instruction.
            % If we visualize the `Filter` as `FilterHead ++ [_] ++ FilterTail`, we need to
            % seek to `FilterHead ++ [SeekWord]`. It carries the `FilterTail` because it's
            % much cheaper to return it from `compare/3` than anything more usable.
            search_new(Words, base(UpFromPrefix), NextF, Acc)
    end.

%% Compare the topic words against the current topic-filter.
%%
%% Return values:
%%
%% * match_full:
%%   The curosr is a full-match of the topic.
%%   Collect this record and move the cursor up and match again.
%%
%% * match_prefix:
%%   The cursor is not a full-match of the topic, but only its prefix.
%%
%% * lower:
%%   it's an impossible match to the cursor.
%%   e.g. a/b/c vs +/z
%%   i.e. all possible filters would have sorted *lower* than the curosr
%%   hence should give up searching.
%%
%% * UpFromPrefix:
%%   it's an impossible match to the current cursor.
%%   e.g. a/b/z vs +/+/c
%%   i.e. some filters might be *higher* than the cursor
%%   and the next potential matche should be immediately above UpFromPrefix
%%
compare(NotFilter, _, _) when is_binary(NotFilter) ->
    % All non-wildcards (topics) are sorted higher than wildcards
    lower;
compare(['#'], _Words, _RPrefix) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/+/d/#
    %  or
    % Filter: a/#
    % We matched the topic to a topic filter with wildcard (possibly with pluses).
    % We include it in the result set, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/+/d/# (same topic but a different ID)
    match_full;
compare([], [], _RPrefix) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/+/d
    % We matched the topic to a topic filter exactly (possibly with pluses).
    % We include it in the result set, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/+/d (same topic but a different ID)
    % * a/+/+/d/# (also a match)
    match_full;
compare([], _Words, _RPrefix) ->
    % NOTE
    %  Topic: a/b/c/d
    % Filter: a/+/c
    % We found out that a topic filter is a prefix of the topic (possibly with pluses).
    % We discard it, and now need to try next entry in the table.
    % Closest possible next entries that we must not miss:
    % * a/+/c/# (which is a match)
    % * a/+/c/+ (also a match)
    %
    % The immediate next of 'a/+/c' should be 'a/+/c/#' (if there is one)
    match_prefix;
compare(_Filter, [], _RPrefix) ->
    % NOTE
    %  Topic: a/b/c
    % Filter: a/+/c/d
    lower;
compare(['+'], [_], _RPrefix) ->
    % NOTE
    % Matched the last '+'
    match_full;
compare(['+' | Filter], [W | Words], RPrefix) ->
    case compare(Filter, Words, ['+' | RPrefix]) of
        lower ->
            % NOTE
            %  Topic: a/b/c
            % Filter: +/+/x
            %   Seek: +/b
            lists:reverse([W | RPrefix]);
        Other ->
            Other
    end;
compare([W | Filter], [W | Words], RPrefix) ->
    compare(Filter, Words, [W | RPrefix]);
compare([F | _Filter], [W | _Words], _RPrefix) when W < F ->
    lower;
compare([F | _Filter], [W | _Words], RPrefix) ->
    % NOTE
    %  Topic: a/b/z
    % Filter: +/+/x
    %   Seek: +/+/z
    lists:reverse([W | RPrefix]).

match_add(K = {_Filter, ID}, Acc = #{}) ->
    % NOTE: ensuring uniqueness by record ID
    Acc#{ID => K};
match_add(K, Acc) when is_list(Acc) ->
    [K | Acc];
match_add(K, first) ->
    throw({first, K}).

-spec words(emqx_types:topic()) -> [word()].
words(Topic) when is_binary(Topic) ->
    % NOTE
    % This is almost identical to `emqx_topic:words/1`, but it doesn't convert empty
    % tokens to ''. This is needed to keep ordering of words consistent with what
    % `match_filter/3` expects.
    [word(W) || W <- emqx_topic:tokens(Topic)].

-spec word(binary()) -> word().
word(<<"+">>) -> '+';
word(<<"#">>) -> '#';
word(Bin) -> Bin.

%% match non-wildcard topics
match_topics(Topic, {Topic, _} = Key, NextF, Acc) ->
    %% found a topic match
    match_topics(Topic, NextF(Key), NextF, match_add(Key, Acc));
match_topics(Topic, {F, _}, NextF, Acc) when F < Topic ->
    %% the last key is a filter, try jump to the topic
    match_topics(Topic, NextF(base(Topic)), NextF, Acc);
match_topics(_Topic, _Key, _NextF, Acc) ->
    %% gone pass the topic
    Acc.
