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

-module(emqx_alarm).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

-include("emqx.hrl").
-include("logger.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

-export([post_config_update/4]).

-export([ start_link/0
        , stop/0
        ]).

-export([format/1]).

%% API
-export([ activate/1
        , activate/2
        , activate/3
        , deactivate/1
        , deactivate/2
        , deactivate/3
        , delete_all_deactivated_alarms/0
        , get_alarms/0
        , get_alarms/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
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

-record(state, {
          timer :: reference()
        }).

-define(ACTIVATED_ALARM, emqx_activated_alarm).

-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = mria:create_table(?ACTIVATED_ALARM,
             [{type, set},
              {storage, disc_copies},
              {local_content, true},
              {record_name, activated_alarm},
              {attributes, record_info(fields, activated_alarm)}]),
    ok = mria:create_table(?DEACTIVATED_ALARM,
             [{type, ordered_set},
              {storage, disc_copies},
              {local_content, true},
              {record_name, deactivated_alarm},
              {attributes, record_info(fields, deactivated_alarm)}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

activate(Name) ->
    activate(Name, #{}).

activate(Name, Details) ->
    activate(Name, Details, <<"">>).

activate(Name, Details, Message) ->
    gen_server:call(?MODULE, {activate_alarm, Name, Details, Message}).

deactivate(Name) ->
    deactivate(Name, no_details, <<"">>).

deactivate(Name, Details) ->
    deactivate(Name, Details, <<"">>).

deactivate(Name, Details, Message) ->
    gen_server:call(?MODULE, {deactivate_alarm, Name, Details, Message}).

delete_all_deactivated_alarms() ->
    gen_server:call(?MODULE, delete_all_deactivated_alarms).

get_alarms() ->
    get_alarms(all).

get_alarms(all) ->
    gen_server:call(?MODULE, {get_alarms, all});

get_alarms(activated) ->
    gen_server:call(?MODULE, {get_alarms, activated});

get_alarms(deactivated) ->
    gen_server:call(?MODULE, {get_alarms, deactivated}).

post_config_update(_, #{validity_period := Period0}, _OldConf, _AppEnv) ->
    ?MODULE ! {update_timer, Period0},
    ok.

format(#activated_alarm{name = Name, message = Message, activate_at = At, details = Details}) ->
    Now = erlang:system_time(microsecond),
    #{
        node => node(),
        name => Name,
        message => Message,
        duration => (Now - At) div 1000, %% to millisecond
        activate_at => to_rfc3339(At),
        details => Details
    };
format(#deactivated_alarm{name = Name, message = Message, activate_at = At, details = Details,
            deactivate_at = DAt}) ->
    #{
        node => node(),
        name => Name,
        message => Message,
        duration => DAt - At,
        activate_at => to_rfc3339(At),
        deactivate_at => to_rfc3339(DAt),
        details => Details
    };
format(_) ->
    {error, unknow_alarm}.

to_rfc3339(Timestamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp div 1000, [{unit, millisecond}])).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = mria:wait_for_tables([?ACTIVATED_ALARM, ?DEACTIVATED_ALARM]),
    deactivate_all_alarms(),
    ok = emqx_config_handler:add_handler([alarm], ?MODULE),
    {ok, #state{timer = ensure_timer(undefined, get_validity_period())}}.

%% suppress dialyzer warning due to dirty read/write race condition.
%% TODO: change from dirty_read/write to transactional.
%% TODO: handle mnesia write errors.
-dialyzer([{nowarn_function, [handle_call/3]}]).
handle_call({activate_alarm, Name, Details, Message}, _From, State) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [#activated_alarm{name = Name}] ->
            {reply, {error, already_existed}, State};
        [] ->
            Alarm = #activated_alarm{name = Name,
                                     details = Details,
                                     message = normalize_message(Name, iolist_to_binary(Message)),
                                     activate_at = erlang:system_time(microsecond)},
            mria:dirty_write(?ACTIVATED_ALARM, Alarm),
            do_actions(activate, Alarm, emqx:get_config([alarm, actions])),
            {reply, ok, State}
    end;

handle_call({deactivate_alarm, Name, Details, Message}, _From, State) ->
    case mnesia:dirty_read(?ACTIVATED_ALARM, Name) of
        [] ->
            {reply, {error, not_found}, State};
        [Alarm] ->
            deactivate_alarm(Alarm, Details, Message),
            {reply, ok, State}
    end;

handle_call(delete_all_deactivated_alarms, _From, State) ->
    clear_table(?DEACTIVATED_ALARM),
    {reply, ok, State};

handle_call({get_alarms, all}, _From, State) ->
    {atomic, Alarms} =
        mria:ro_transaction(
          ?COMMON_SHARD,
          fun() ->
                  [normalize(Alarm) ||
                      Alarm <- ets:tab2list(?ACTIVATED_ALARM)
                          ++ ets:tab2list(?DEACTIVATED_ALARM)]
          end),
    {reply, Alarms, State};

handle_call({get_alarms, activated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?ACTIVATED_ALARM)],
    {reply, Alarms, State};

handle_call({get_alarms, deactivated}, _From, State) ->
    Alarms = [normalize(Alarm) || Alarm <- ets:tab2list(?DEACTIVATED_ALARM)],
    {reply, Alarms, State};

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, _TRef, delete_expired_deactivated_alarm},
       #state{timer = TRef} = State) ->
    Period = get_validity_period(),
    delete_expired_deactivated_alarms(erlang:system_time(microsecond) - Period * 1000),
    {noreply, State#state{timer = ensure_timer(TRef, Period)}};

handle_info({update_timer, Period}, #state{timer = TRef} = State) ->
    ?SLOG(warning, #{msg => "validity_timer_updated", period => Period}),
    {noreply, State#state{timer = ensure_timer(TRef, Period)}};

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok = emqx_config_handler:remove_handler([alarm]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

get_validity_period() ->
    emqx:get_config([alarm, validity_period]).

deactivate_alarm(#activated_alarm{activate_at = ActivateAt, name = Name,
        details = Details0, message = Msg0}, Details, Message) ->
    SizeLimit = emqx:get_config([alarm, size_limit]),
    case SizeLimit > 0 andalso (mnesia:table_info(?DEACTIVATED_ALARM, size) >= SizeLimit) of
        true ->
            case mnesia:dirty_first(?DEACTIVATED_ALARM) of
                '$end_of_table' -> ok;
                ActivateAt2 ->
                    mria:dirty_delete(?DEACTIVATED_ALARM, ActivateAt2)
            end;
        false -> ok
    end,
    HistoryAlarm = make_deactivated_alarm(ActivateAt, Name, Details0, Msg0,
                        erlang:system_time(microsecond)),
    DeActAlarm = make_deactivated_alarm(ActivateAt, Name, Details,
                    normalize_message(Name, iolist_to_binary(Message)),
                    erlang:system_time(microsecond)),
    mria:dirty_write(?DEACTIVATED_ALARM, HistoryAlarm),
    mria:dirty_delete(?ACTIVATED_ALARM, Name),
    do_actions(deactivate, DeActAlarm, emqx:get_config([alarm, actions])).

make_deactivated_alarm(ActivateAt, Name, Details, Message, DeActivateAt) ->
    #deactivated_alarm{
        activate_at = ActivateAt,
        name = Name,
        details = Details,
        message = Message,
        deactivate_at = DeActivateAt}.

deactivate_all_alarms() ->
    lists:foreach(
        fun(#activated_alarm{name = Name,
                             details = Details,
                             message = Message,
                             activate_at = ActivateAt}) ->
            mria:dirty_write(?DEACTIVATED_ALARM,
                #deactivated_alarm{
                    activate_at = ActivateAt,
                    name = Name,
                    details = Details,
                    message = Message,
                    deactivate_at = erlang:system_time(microsecond)})
        end, ets:tab2list(?ACTIVATED_ALARM)),
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

ensure_timer(OldTRef, Period) ->
    _ = case is_reference(OldTRef) of
        true -> erlang:cancel_timer(OldTRef);
        false -> ok
    end,
    emqx_misc:start_timer(Period, delete_expired_deactivated_alarm).

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

do_actions(_, _, []) ->
    ok;
do_actions(activate, Alarm = #activated_alarm{name = Name, message = Message}, [log | More]) ->
    ?SLOG(warning, #{
        msg => "alarm_is_activated",
        name => Name,
        message => Message
    }),
    do_actions(activate, Alarm, More);
do_actions(deactivate, Alarm = #deactivated_alarm{name = Name}, [log | More]) ->
    ?SLOG(warning, #{
        msg => "alarm_is_deactivated",
        name => Name
    }),
    do_actions(deactivate, Alarm, More);
do_actions(Operation, Alarm, [publish | More]) ->
    Topic = topic(Operation),
    {ok, Payload} = encode_to_json(Alarm),
    Message = emqx_message:make(?MODULE, 0, Topic, Payload, #{sys => true},
                  #{properties => #{'Content-Type' => <<"application/json">>}}),
    %% TODO log failed publishes
    _ = emqx_broker:safe_publish(Message),
    do_actions(Operation, Alarm, More).

encode_to_json(Alarm) ->
    emqx_json:safe_encode(normalize(Alarm)).

topic(activate) ->
    emqx_topic:systop(<<"alarms/activate">>);
topic(deactivate) ->
    emqx_topic:systop(<<"alarms/deactivate">>).

normalize(#activated_alarm{name = Name,
                           details = Details,
                           message = Message,
                           activate_at = ActivateAt}) ->
    #{name => Name,
      details => Details,
      message => Message,
      activate_at => ActivateAt,
      deactivate_at => infinity,
      activated => true};
normalize(#deactivated_alarm{activate_at = ActivateAt,
                             name = Name,
                             details = Details,
                             message = Message,
                             deactivate_at = DeactivateAt}) ->
    #{name => Name,
      details => Details,
      message => Message,
      activate_at => ActivateAt,
      deactivate_at => DeactivateAt,
      activated => false}.

normalize_message(Name, <<"">>) ->
    list_to_binary(io_lib:format("~p", [Name]));
normalize_message(_Name, Message) -> Message.
