%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_delayed).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ start_link/0
        , on_message_publish/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% gen_server callbacks
-export([ enable/0
        , disable/0
        , set_max_delayed_messages/1
        , update_config/1
        , list/1
        , get_delayed_message/1
        , delete_delayed_message/1
        ]).

-record(delayed_message, {key, delayed, msg}).

%% sync ms with record change
-define(QUERY_MS(Id), [{{delayed_message, {'_', Id}, '_', '_'}, [], ['$_']}]).
-define(DELETE_MS(Id), [{{delayed_message, {'$1', Id}, '_', '_'}, [], ['$1']}]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(MAX_INTERVAL, 4294967).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, ordered_set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, delayed_message},
                {attributes, record_info(fields, delayed_message)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, disc_copies).

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------
on_message_publish(Msg = #message{
                            id = Id,
                            topic = <<"$delayed/", Topic/binary>>,
                            timestamp = Ts
                           }) ->
    [Delay, Topic1] = binary:split(Topic, <<"/">>),
    {PubAt, Delayed} = case binary_to_integer(Delay) of
                Interval when Interval < ?MAX_INTERVAL ->
                    {Interval + erlang:round(Ts / 1000), Interval};
                Timestamp ->
                    %% Check malicious timestamp?
                    case (Timestamp - erlang:round(Ts / 1000)) > ?MAX_INTERVAL of
                        true  -> error(invalid_delayed_timestamp);
                        false -> {Timestamp, Timestamp - erlang:round(Ts / 1000)}
                    end
            end,
    PubMsg = Msg#message{topic = Topic1},
    Headers = PubMsg#message.headers,
    case store(#delayed_message{key = {PubAt, Id}, delayed = Delayed, msg = PubMsg}) of
        ok -> ok;
        {error, Error} ->
            ?LOG(error, "Store delayed message fail: ~p", [Error])
    end,
    {stop, PubMsg#message{headers = Headers#{allow_publish => false}}};

on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% Start delayed publish server
%%--------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    Opts = emqx:get_config([delayed], #{}),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Opts], []).

-spec(store(#delayed_message{}) -> ok | {error, atom()}).
store(DelayedMsg) ->
    gen_server:call(?SERVER, {store, DelayedMsg}, infinity).

enable() ->
    gen_server:call(?SERVER, enable).

disable() ->
    gen_server:call(?SERVER, disable).

set_max_delayed_messages(Max) ->
    gen_server:call(?SERVER, {set_max_delayed_messages, Max}).

list(Params) ->
    emqx_mgmt_api:paginate(?TAB, Params, fun format_delayed/1).

format_delayed(Delayed) ->
    format_delayed(Delayed, false).

format_delayed(#delayed_message{key = {ExpectTimeStamp, Id}, delayed = Delayed,
            msg = #message{topic = Topic,
                           from = From,
                           headers = Headers,
                           qos = Qos,
                           timestamp = PublishTimeStamp,
                           payload = Payload}}, WithPayload) ->
    PublishTime = to_rfc3339(PublishTimeStamp div 1000),
    ExpectTime = to_rfc3339(ExpectTimeStamp),
    RemainingTime = ExpectTimeStamp - erlang:system_time(second),
    Result = #{
        msgid => emqx_guid:to_hexstr(Id),
        publish_at => PublishTime,
        delayed_interval => Delayed,
        delayed_remaining => RemainingTime,
        expected_at => ExpectTime,
        topic => Topic,
        qos => Qos,
        from_clientid => From,
        from_username => maps:get(username, Headers, undefined)
    },
    case WithPayload of
        true ->
            Result#{payload => base64:encode(Payload)};
        _ ->
            Result
    end.

to_rfc3339(Timestamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, second}])).

get_delayed_message(Id0) ->
    try emqx_guid:from_hexstr(Id0) of
        Id ->
            case ets:select(?TAB, ?QUERY_MS(Id)) of
                [] ->
                    {error, not_found};
                Rows ->
                    Message = hd(Rows),
                    {ok, format_delayed(Message, true)}
            end
    catch
        error:function_clause -> {error, id_schema_error}
    end.

delete_delayed_message(Id0) ->
    Id = emqx_guid:from_hexstr(Id0),
    case ets:select(?TAB, ?DELETE_MS(Id)) of
        [] ->
            {error, not_found};
        Rows ->
            Timestamp = hd(Rows),
            ekka_mnesia:dirty_delete(?TAB, {Timestamp, Id})
    end.
update_config(Config) ->
    {ok, _} = emqx:update_config([delayed], Config).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------

init([Opts]) ->
    MaxDelayedMessages = maps:get(max_delayed_messages, Opts, 0),
    {ok, ensure_stats_event(
           ensure_publish_timer(#{timer => undefined,
                                  publish_at => 0,
                                  max_delayed_messages => MaxDelayedMessages}))}.

handle_call({set_max_delayed_messages, Max}, _From, State) ->
    {reply, ok, State#{max_delayed_messages => Max}};

handle_call({store, DelayedMsg = #delayed_message{key = Key}},
            _From, State = #{max_delayed_messages := 0}) ->
    ok = ekka_mnesia:dirty_write(?TAB, DelayedMsg),
    emqx_metrics:inc('messages.delayed'),
    {reply, ok, ensure_publish_timer(Key, State)};

handle_call({store, DelayedMsg = #delayed_message{key = Key}},
            _From, State = #{max_delayed_messages := Max}) ->
    Size = mnesia:table_info(?TAB, size),
    case Size >= Max of
        true ->
            {reply, {error, max_delayed_messages_full}, State};
        false ->
            ok = ekka_mnesia:dirty_write(?TAB, DelayedMsg),
            emqx_metrics:inc('messages.delayed'),
            {reply, ok, ensure_publish_timer(Key, State)}
    end;

handle_call(enable, _From, State) ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}),
    {reply, ok, State};

handle_call(disable, _From, State) ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

%% Do Publish...
handle_info({timeout, TRef, do_publish}, State = #{timer := TRef}) ->
    DeletedKeys = do_publish(mnesia:dirty_first(?TAB), os:system_time(seconds)),
    lists:foreach(fun(Key) -> ekka_mnesia:dirty_delete(?TAB, Key) end, DeletedKeys),
    {noreply, ensure_publish_timer(State#{timer := undefined, publish_at := 0})};

handle_info(stats, State = #{stats_fun := StatsFun}) ->
    StatsFun(delayed_count()),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Ensure the stats
ensure_stats_event(State) ->
    StatsFun = emqx_stats:statsfun('delayed.count', 'delayed.max'),
    {ok, StatsTimer} = timer:send_interval(timer:seconds(1), stats),
    State#{stats_fun => StatsFun, stats_timer => StatsTimer}.

%% Ensure publish timer
ensure_publish_timer(State) ->
    ensure_publish_timer(mnesia:dirty_first(?TAB), State).

ensure_publish_timer('$end_of_table', State) ->
    State#{timer := undefined, publish_at := 0};
ensure_publish_timer({Ts, _Id}, State = #{timer := undefined}) ->
    ensure_publish_timer(Ts, os:system_time(seconds), State);
ensure_publish_timer({Ts, _Id}, State = #{timer := TRef, publish_at := PubAt})
    when Ts < PubAt ->
    ok = emqx_misc:cancel_timer(TRef),
    ensure_publish_timer(Ts, os:system_time(seconds), State);
ensure_publish_timer(_Key, State) ->
    State.

ensure_publish_timer(Ts, Now, State) ->
    Interval = max(1, Ts - Now),
    TRef = emqx_misc:start_timer(timer:seconds(Interval), do_publish),
    State#{timer := TRef, publish_at := Now + Interval}.

do_publish(Key, Now) ->
    do_publish(Key, Now, []).

%% Do publish
do_publish('$end_of_table', _Now, Acc) ->
    Acc;
do_publish({Ts, _Id}, Now, Acc) when Ts > Now ->
    Acc;
do_publish(Key = {Ts, _Id}, Now, Acc) when Ts =< Now ->
    case mnesia:dirty_read(?TAB, Key) of
        [] -> ok;
        [#delayed_message{msg = Msg}] ->
            emqx_pool:async_submit(fun emqx:publish/1, [Msg])
    end,
    do_publish(mnesia:dirty_next(?TAB, Key), Now, [Key|Acc]).

-spec(delayed_count() -> non_neg_integer()).
delayed_count() -> mnesia:table_info(?TAB, size).
