%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_bucket_ref).

%% @doc limiter bucket reference
%% this module is used to manage the bucket reference of the limiter server
%% @end

%% API
-export([
    new/2,
    check/2,
    restore/2,
    available/1
]).

-export_type([bucket_ref/0]).

-type bucket_ref() :: #{
    counter := counters:counters_ref(),
    index := index()
}.

-type index() :: emqx_limiter_server:index().

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec new(counters:counters_ref(), index()) -> bucket_ref().
new(Counter, Index) ->
    #{
        counter => Counter,
        index => Index
    }.

%% @doc check tokens
-spec check(pos_integer(), bucket_ref()) -> boolean().
check(
    Need,
    #{
        counter := Counter,
        index := Index
    }
) ->
    RefToken = counters:get(Counter, Index),
    if
        RefToken >= Need ->
            counters:sub(Counter, Index, Need),
            true;
        true ->
            false
    end.

%% @doc try to restore token when consume failed
-spec restore(non_neg_integer(), bucket_ref()) -> ok.
restore(0, _) ->
    ok;
restore(Inc, #{counter := Counter, index := Index}) ->
    counters:add(Counter, Index, Inc).

%% @doc get the number of tokens currently available
-spec available(bucket_ref()) -> emqx_limiter_decimal:decimal().
available(#{counter := Counter, index := Index}) ->
    counters:get(Counter, Index);
available(infinity) ->
    infinity.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
