%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_alarm).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").

-export([create_tables/0]).
-export([start_link/0]).

%% API
-export([
    activate/1,
    activate/2,
    activate/3,
    deactivate/1,
    deactivate/2,
    deactivate/3,
    ensure_deactivated/1,
    ensure_deactivated/2,
    ensure_deactivated/3,
    delete_all_deactivated_alarms/0,
    get_alarms/0,
    get_alarms/1,
    format/1,
    format/2,
    safe_activate/3,
    safe_deactivate/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% Internal exports (RPC)
-export([
    do_get_alarms/0
]).

%% Internal exports
-export([
    do_start_worker/0
]).

-record(activated_alarm, {
    name :: binary() | atom(),
    details :: map() | list(),
    message :: binary(),
    activate_at :: integer()
}).

-record(deactivated_alarm, {
    activate_at :: integer(),
    name :: binary() | atom(),
    details :: map() | list(),
    message :: binary(),
    deactivate_at :: integer() | infinity
}).

-define(worker, worker).

-record(work, {mod :: module(), fn :: atom(), args :: [term()]}).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(
        ?ACTIVATED_ALARM,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, activated_alarm},
            {attributes, record_info(fields, activated_alarm)}
        ]
    ),
    ok = mria:create_table(
        ?DEACTIVATED_ALARM,
        [
            {type, ordered_set},
            {storage, disc_copies},
            {local_content, true},
            {record_name, deactivated_alarm},
            {attributes, record_info(fields, deactivated_alarm)}
        ]
    ),
    [?ACTIVATED_ALARM, ?DEACTIVATED_ALARM].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

activate(Name) ->
    activate(Name, #{}).

activate(Name, Details) ->
    activate(Name, Details, <<"">>).

activate(Name, Details, Message) ->
    gen_server:call(?MODULE, {activate_alarm, Name, Details, Message}).

safe_activate(Name, Details, Message) ->
    safe_call({activate_alarm, Name, Details, Message}).

-spec ensure_deactivated(binary() | atom()) -> ok.
ensure_deactivated(Name) ->
    ensure_deactivated(Name, no_details).

-spec ensure_deactivated(binary() | atom(), atom() | map()) -> ok.
ensure_deactivated(Name, Data) ->
    ensure_deactivated(Name, Data, <<>>).

-spec ensure_deactivated(binary() | atom(), atom() | map(), iodata()) -> ok.
ensure_deactivated(Name, Data, Message) ->
    %% this duplicates the dirty read in handle_call,
    %% intention is to avoid making gen_server calls when there is no alarm
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [] ->
            ok;
        _ ->
            case deactivate(Name, Data, Message) of
                {error, not_found} -> ok;
                Other -> Other
            end
    end.

-spec deactivate(binary() | atom()) -> ok | {error, not_found}.
deactivate(Name) ->
    deactivate(Name, no_details, <<"">>).

deactivate(Name, Details) ->
    deactivate(Name, Details, <<"">>).

deactivate(Name, Details, Message) ->
    gen_server:call(?MODULE, {deactivate_alarm, Name, Details, Message}).

safe_deactivate(Name) ->
    safe_call({deactivate_alarm, Name, no_details, <<"">>}).

-spec delete_all_deactivated_alarms() -> ok.
delete_all_deactivated_alarms() ->
    gen_server:call(?MODULE, delete_all_deactivated_alarms).

get_alarms() ->
    get_alarms(all).

-spec get_alarms(all | activated | deactivated) -> [map()].
get_alarms(all) ->
    gen_server:call(?MODULE, {get_alarms, all});
get_alarms(activated) ->
    gen_server:call(?MODULE, {get_alarms, activated});
get_alarms(deactivated) ->
    gen_server:call(?MODULE, {get_alarms, deactivated}).

format(Alarm) ->
    format(node(), Alarm).

format(Node, #activated_alarm{name = Name, message = Message, activate_at = At, details = Details}) ->
    Now = erlang:system_time(microsecond),
    %% mnesia db stored microsecond for high frequency alarm
    %% format for dashboard using millisecond
    #{
        node => Node,
        name => Name,
        message => Message,
        %% to millisecond
        duration => (Now - At) div 1000,
        activate_at => to_rfc3339(At),
        details => Details
    };
format(Node, #deactivated_alarm{
    name = Name,
    message = Message,
    activate_at = At,
    details = Details,
    deactivate_at = DAt
}) ->
    #{
        node => Node,
        name => Name,
        message => Message,
        %% to millisecond
        duration => (DAt - At) div 1000,
        activate_at => to_rfc3339(At),
        deactivate_at => to_rfc3339(DAt),
        details => Details
    }.

to_rfc3339(Timestamp) ->
    %% rfc3339 accuracy to millisecond
    emqx_utils_calendar:epoch_to_rfc3339(Timestamp div 1000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = mria:wait_for_tables([?ACTIVATED_ALARM, ?DEACTIVATED_ALARM, ?TRIE]),
    deactivate_all_alarms(),
    process_flag(trap_exit, true),
    State = #{?worker => start_worker()},
    {ok, State, get_validity_period()}.

handle_call({activate_alarm, Name, Details, Message}, _From, State) ->
    case create_activate_alarm(Name, Details, Message) of
        {ok, Alarm} ->
            do_actions(activate, Alarm, emqx:get_config([alarm, actions]), State),
            {reply, ok, State, get_validity_period()};
        Err ->
            {reply, Err, State, get_validity_period()}
    end;
handle_call({deactivate_alarm, Name, Details, Message}, _From, State) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [] ->
            {reply, {error, not_found}, State};
        [Alarm] ->
            deactivate_alarm(Alarm, Details, Message, State),
            {reply, ok, State, get_validity_period()}
    end;
handle_call(delete_all_deactivated_alarms, _From, State) ->
    clear_table(?DEACTIVATED_ALARM),
    {reply, ok, State, get_validity_period()};
handle_call({get_alarms, all}, _From, State) ->
    {atomic, Alarms} =
        mria:ro_transaction(
            mria:local_content_shard(), fun ?MODULE:do_get_alarms/0
        ),
    {reply, Alarms, State, get_validity_period()};
handle_call({get_alarms, activated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?ACTIVATED_ALARM)],
    {reply, Alarms, State, get_validity_period()};
handle_call({get_alarms, deactivated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?DEACTIVATED_ALARM)],
    {reply, Alarms, State, get_validity_period()};
handle_call(Req, From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call_req => Req, from => From}),
    {reply, ignored, State, get_validity_period()}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast_req => Msg}),
    {noreply, State, get_validity_period()}.

handle_info(timeout, State) ->
    Period = get_validity_period(),
    delete_expired_deactivated_alarms(erlang:system_time(microsecond) - Period * 1000),
    {noreply, State, Period};
handle_info({'EXIT', Worker, _}, #{?worker := Worker} = State0) ->
    State = State0#{?worker := start_worker()},
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info_req => Info}),
    {noreply, State, get_validity_period()}.

terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_validity_period() ->
    emqx:get_config([alarm, validity_period]).

create_activate_alarm(Name, Details, Message) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [#activated_alarm{name = Name}] ->
            {error, already_existed};
        [] ->
            Alarm = #activated_alarm{
                name = Name,
                details = Details,
                message = normalize_message(Name, iolist_to_binary(Message)),
                activate_at = erlang:system_time(microsecond)
            },
            ok = mria:dirty_write(?ACTIVATED_ALARM, Alarm),
            {ok, Alarm}
    end.

do_get_alarms() ->
    [
        normalize(Alarm)
     || Alarm <-
            ets:tab2list(?ACTIVATED_ALARM) ++
                ets:tab2list(?DEACTIVATED_ALARM)
    ].

deactivate_alarm(
    #activated_alarm{
        activate_at = ActivateAt,
        name = Name,
        details = Details0,
        message = Msg0
    },
    Details,
    Message,
    State
) ->
    SizeLimit = emqx:get_config([alarm, size_limit]),
    case SizeLimit > 0 andalso (mnesia:table_info(?DEACTIVATED_ALARM, size) >= SizeLimit) of
        true ->
            case mnesia:dirty_first(?DEACTIVATED_ALARM) of
                '$end_of_table' -> ok;
                ActivateAt2 -> mria:dirty_delete(?DEACTIVATED_ALARM, ActivateAt2)
            end;
        false ->
            ok
    end,
    Now = erlang:system_time(microsecond),
    HistoryAlarm = make_deactivated_alarm(
        ActivateAt,
        Name,
        Details0,
        Msg0,
        Now
    ),
    DeActAlarm = make_deactivated_alarm(
        ActivateAt,
        Name,
        Details,
        normalize_message(Name, iolist_to_binary(Message)),
        Now
    ),
    mria:dirty_write(?DEACTIVATED_ALARM, HistoryAlarm),
    mria:dirty_delete(?ACTIVATED_ALARM, Name),
    do_actions(deactivate, DeActAlarm, emqx:get_config([alarm, actions]), State).

make_deactivated_alarm(ActivateAt, Name, Details, Message, DeActivateAt) ->
    #deactivated_alarm{
        activate_at = ActivateAt,
        name = Name,
        details = Details,
        message = Message,
        deactivate_at = DeActivateAt
    }.

deactivate_all_alarms() ->
    lists:foreach(
        fun(
            #activated_alarm{
                name = Name,
                details = Details,
                message = Message,
                activate_at = ActivateAt
            }
        ) ->
            mria:dirty_write(
                ?DEACTIVATED_ALARM,
                #deactivated_alarm{
                    activate_at = ActivateAt,
                    name = Name,
                    details = Details,
                    message = Message,
                    deactivate_at = erlang:system_time(microsecond)
                }
            )
        end,
        ets:tab2list(?ACTIVATED_ALARM)
    ),
    clear_table(?ACTIVATED_ALARM).

%% Delete all records from the given table, ignore result.
clear_table(TableName) ->
    case mria:clear_table(TableName) of
        {aborted, Reason} ->
            ?SLOG(warning, #{
                msg => "fail_to_clear_table",
                table_name => TableName,
                reason => Reason
            });
        {atomic, ok} ->
            ok
    end.

delete_expired_deactivated_alarms(Checkpoint) ->
    delete_expired_deactivated_alarms(mnesia:dirty_first(?DEACTIVATED_ALARM), Checkpoint).

delete_expired_deactivated_alarms('$end_of_table', _Checkpoint) ->
    ok;
delete_expired_deactivated_alarms(ActivatedAt, Checkpoint) ->
    case ActivatedAt =< Checkpoint of
        true ->
            mria:dirty_delete(?DEACTIVATED_ALARM, ActivatedAt),
            NActivatedAt = mnesia:dirty_next(?DEACTIVATED_ALARM, ActivatedAt),
            delete_expired_deactivated_alarms(NActivatedAt, Checkpoint);
        false ->
            ok
    end.

do_actions(_, _, [], _State) ->
    ok;
do_actions(activate, Alarm = #activated_alarm{name = Name, message = Message}, [log | More], State) ->
    ?SLOG(warning, #{
        msg => "alarm_is_activated",
        name => Name,
        message => Message
    }),
    do_actions(activate, Alarm, More, State);
do_actions(deactivate, Alarm = #deactivated_alarm{name = Name}, [log | More], State) ->
    ?SLOG(warning, #{
        msg => "alarm_is_deactivated",
        name => Name
    }),
    do_actions(deactivate, Alarm, More, State);
do_actions(Operation, Alarm, [publish | More], State) ->
    Topic = topic(Operation),
    NormalizedAlarm = normalize(Alarm),
    {ok, Payload} = emqx_utils_json:safe_encode(NormalizedAlarm),
    Message = emqx_message:make(
        ?MODULE,
        0,
        Topic,
        Payload,
        #{sys => true},
        #{properties => #{'Content-Type' => <<"application/json">>}}
    ),
    _ = emqx_broker:safe_publish(Message),
    _ =
        %% We run hooks in a temporary process to avoid blocking the alarm process for long.
        case Operation of
            activate ->
                ActivatedAlarmContext = to_activated_alarm_context(NormalizedAlarm),
                send_job_to_worker(
                    emqx_hooks,
                    run,
                    ['alarm.activated', [ActivatedAlarmContext]],
                    State
                );
            deactivate ->
                DeactivatedAlarmContext = to_deactivated_alarm_context(NormalizedAlarm),
                send_job_to_worker(
                    emqx_hooks,
                    run,
                    ['alarm.deactivated', [DeactivatedAlarmContext]],
                    State
                )
        end,
    do_actions(Operation, Alarm, More, State).

topic(activate) ->
    emqx_topic:systop(<<"alarms/activate">>);
topic(deactivate) ->
    emqx_topic:systop(<<"alarms/deactivate">>).

normalize(#activated_alarm{
    name = Name,
    details = Details,
    message = Message,
    activate_at = ActivateAt
}) ->
    #{
        name => Name,
        details => Details,
        message => Message,
        activate_at => ActivateAt,
        deactivate_at => infinity,
        activated => true
    };
normalize(#deactivated_alarm{
    activate_at = ActivateAt,
    name = Name,
    details = Details,
    message = Message,
    deactivate_at = DeactivateAt
}) ->
    #{
        name => Name,
        details => Details,
        message => Message,
        activate_at => ActivateAt,
        deactivate_at => DeactivateAt,
        activated => false
    }.

normalize_message(Name, <<"">>) when is_binary(Name) ->
    Name;
normalize_message(Name, <<"">>) ->
    iolist_to_binary(io_lib:format("~p", [Name]));
normalize_message(_Name, Message) ->
    Message.

to_activated_alarm_context(NormalizedAlarm) ->
    Ctx0 = maps:with([name, details, message, activate_at], NormalizedAlarm),
    emqx_utils_maps:rename(activate_at, activated_at, Ctx0).

to_deactivated_alarm_context(NormalizedAlarm) ->
    Ctx0 = maps:with([name, details, message, activate_at, deactivate_at], NormalizedAlarm),
    Ctx1 = emqx_utils_maps:rename(activate_at, activated_at, Ctx0),
    emqx_utils_maps:rename(deactivate_at, deactivated_at, Ctx1).

safe_call(Req) ->
    try
        gen_server:call(?MODULE, Req)
    catch
        _:{timeout, _} = Reason ->
            ?SLOG(warning, #{msg => "emqx_alarm_safe_call_timeout", reason => Reason}),
            {error, timeout};
        _:Reason:St ->
            ?SLOG(error, #{
                msg => "emqx_alarm_safe_call_exception",
                reason => Reason,
                stacktrace => St
            }),
            {error, Reason}
    end.

start_worker() ->
    proc_lib:start_link(?MODULE, do_start_worker, []).

do_start_worker() ->
    ok = proc_lib:init_ack(self()),
    worker_loop().

send_job_to_worker(Mod, Fn, Args, State0) ->
    #{?worker := WorkerPid} = State0,
    WorkerPid ! #work{mod = Mod, fn = Fn, args = Args},
    ok.

worker_loop() ->
    receive
        #work{mod = Mod, fn = Fn, args = Args} ->
            try
                apply(Mod, Fn, Args)
            catch
                Kind:Error:Stacktrace ->
                    ?SLOG(warning, #{
                        msg => "alarm_failed_to_run_job",
                        mfa => {Mod, Fn, Args},
                        reason => {Kind, Error},
                        stacktrace => Stacktrace
                    })
            end,
            worker_loop()
    end.
