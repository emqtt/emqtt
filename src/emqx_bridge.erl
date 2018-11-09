%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-import(proplists, [get_value/2, get_value/3]).

-export([start_link/2, start_bridge/1, stop_bridge/1, status/1]).

-export([show_forwards/1, add_forward/2, del_forward/2]).

-export([show_subscriptions/1, add_subscription/3, del_subscription/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {client_pid, options, reconnect_interval, 
                mountpoint, queue, mqueue_type, max_pending_messages,
                forwards = [], subscriptions = []}).

-record(mqtt_msg, {qos = ?QOS0, retain = false, dup = false,
                   packet_id, topic, props, payload}).

start_link(Name, Options) ->
    gen_server:start_link({local, name(Name)}, ?MODULE, [Options], []).

start_bridge(Name) ->
    gen_server:call(name(Name), start_bridge).

stop_bridge(Name) ->
    gen_server:call(name(Name), stop_bridge).

-spec(show_forwards(atom()) -> list()).
show_forwards(Name) ->
    gen_server:call(name(Name), show_forwards).

-spec(add_forward(atom(), binary()) -> ok | {error, already_exists | validate_fail}).
add_forward(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {add_forward, Topic});
        {'EXIT', _Reason} ->
            {error, validate_fail}
    end.

-spec(del_forward(atom(), binary()) -> ok | {error, validate_fail}).
del_forward(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {del_forward, Topic});
        _ ->
            {error, validate_fail}
    end.

-spec(show_subscriptions(atom()) -> list()).
show_subscriptions(Name) ->
    gen_server:call(name(Name), show_subscriptions).

-spec(add_subscription(atom(), binary(), integer()) -> ok | {error, already_exists | validate_fail}).
add_subscription(Name, Topic, QoS) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {add_subscription, Topic, QoS});
        {'EXIT', _Reason} ->
            {error, validate_fail}
    end.

-spec(del_subscription(atom(), binary()) -> ok | {error, validate_fail}).
del_subscription(Name, Topic) ->
    case catch emqx_topic:validate({filter, Topic}) of
        true ->
            gen_server:call(name(Name), {del_subscription, Topic});
        _ ->
            {error, validate_fail}
    end.

status(Pid) ->
    gen_server:call(Pid, status).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Options]) ->
    process_flag(trap_exit, true),
    case get_value(start_type, Options, manual) of
        manual -> ok;
        auto -> erlang:send_after(1000, self(), start)
    end,
    ReconnectInterval = get_value(reconnect_interval, Options, 30000),
    MaxPendingMsg = get_value(max_pending_messages, Options, 10000),
    Mountpoint = format_mountpoint(get_value(mountpoint, Options)),
    MqueueType = get_value(mqueue_type, Options, memory),
    Queue = [],
    {ok, #state{mountpoint           = Mountpoint,
                queue                = Queue,
                mqueue_type          = MqueueType,
                options              = Options,
                reconnect_interval   = ReconnectInterval,
                max_pending_messages = MaxPendingMsg}}.

handle_call(start_bridge, _From, State = #state{client_pid = undefined}) ->
    {noreply, NewState} = handle_info(start, State),
    {reply, #{msg => <<"start bridge successfully">>}, NewState};

handle_call(start_bridge, _From, State) ->
    {reply, #{msg => <<"bridge already started">>}, State};

handle_call(stop_bridge, _From, State = #state{client_pid = undefined}) ->
    {reply, #{msg => <<"bridge not started">>}, State};

handle_call(stop_bridge, _From, State = #state{client_pid = Pid}) ->
    emqx_client:disconnect(Pid),
    {reply, #{msg => <<"stop bridge successfully">>}, State};

handle_call(status, _From, State = #state{client_pid = undefined}) ->
    {reply, #{status => <<"Stopped">>}, State};
handle_call(status, _From, State = #state{client_pid = _Pid})->
    {reply, #{status => <<"Running">>}, State};

handle_call(show_forwards, _From, State = #state{forwards = Forwards}) ->
    {reply, Forwards, State};

handle_call({add_forward, Topic}, _From, State = #state{forwards = Forwards}) ->
    case not lists:member(Topic, Forwards) of
        true ->
            emqx_broker:subscribe(Topic),
            {reply, ok, State#state{forwards = [Topic | Forwards]}};
        false ->
            {reply, {error, already_exists}, State}
    end;

handle_call({del_forward, Topic}, _From, State = #state{forwards = Forwards}) ->
    case lists:member(Topic, Forwards) of
        true ->
            emqx_broker:unsubscribe(Topic),
            {reply, ok, State#state{forwards = lists:delete(Topic, Forwards)}};
        false ->
            {reply, ok, State}
    end;

handle_call(show_subscriptions, _From, State = #state{subscriptions = Subscriptions}) ->
    {reply, Subscriptions, State};

handle_call({add_subscription, Topic, Qos}, _From, State = #state{subscriptions = Subscriptions, client_pid = ClientPid}) ->
    case not lists:keymember(Topic, 1, Subscriptions) of
        true ->
            emqx_client:subscribe(ClientPid, {Topic, Qos}),
            {reply, ok, State#state{subscriptions = [{Topic, Qos} | Subscriptions]}};
        false ->
            {reply, {error, already_exists}, State}
    end;

handle_call({del_subscription, Topic}, _From, State = #state{subscriptions = Subscriptions, client_pid = ClientPid}) ->
    case lists:keymember(Topic, 1, Subscriptions) of
        true ->
            emqx_client:unsubscribe(ClientPid, Topic),
            {reply, ok, State#state{subscriptions = lists:keydelete(Topic, 1, Subscriptions)}};
        false ->
            {reply, ok, State}
    end;

handle_call(Req, _From, State) ->
    emqx_logger:error("[Bridge] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Bridge] unexpected cast: ~p", [Msg]),
    {noreply, State}.

%%----------------------------------------------------------------
%% start message bridge
%%----------------------------------------------------------------
handle_info(start, State = #state{options = Options,
                                  client_pid = undefined}) ->
    case emqx_client:start_link([{owner, self()}|options(Options)]) of
        {ok, ClientPid} ->
            case emqx_client:connect(ClientPid) of
                {ok, _} ->
                    emqx_logger:info("[Bridge] connected to remote sucessfully"),
                    Subs = subscribe_remote_topics(ClientPid, get_value(subscriptions, Options, [])),
                    Forwards = subscribe_local_topics(get_value(forwards, Options, [])),
                    {noreply, State#state{client_pid = ClientPid,
                                          subscriptions = Subs,
                                          forwards = Forwards}};
                {error, Reason} ->
                    emqx_logger:error("[Bridge] connect to remote failed! error: ~p", [Reason]),
                    {noreply, State#state{client_pid = ClientPid}}
            end;
        {error, Reason} ->
            emqx_logger:error("[Bridge] start failed! error: ~p", [Reason]),
            {noreply, State}
    end;

%%----------------------------------------------------------------
%% received local node message
%%----------------------------------------------------------------
handle_info({dispatch, _, #message{topic = Topic, payload = Payload, flags = #{retain := Retain}}},
             State = #state{client_pid = Pid, mountpoint = Mountpoint, queue = Queue,
                            mqueue_type = MqueueType, max_pending_messages = MaxPendingMsg}) ->
    Msg = #mqtt_msg{qos     = 1,
                    retain  = Retain,
                    topic   = mountpoint(Mountpoint, Topic),
                    payload = Payload},
    case emqx_client:publish(Pid, Msg) of
        {ok, PkgId} ->
            {noreply, State#state{queue = store(MqueueType, {PkgId, Msg}, Queue, MaxPendingMsg)}};
        {error, Reason} ->
            emqx_logger:error("[Bridge] Publish fail:~p", [Reason]),
            {noreply, State}
    end;

%%----------------------------------------------------------------
%% received remote node message
%%----------------------------------------------------------------
handle_info({publish, #{qos := QoS, dup := Dup, retain := Retain, topic := Topic,
                        properties := Props, payload := Payload}}, State) ->
    NewMsg0 = emqx_message:make(bridge, QoS, Topic, Payload),
    NewMsg1 = emqx_message:set_headers(Props, emqx_message:set_flags(#{dup => Dup, retain => Retain}, NewMsg0)),
    emqx_broker:publish(NewMsg1),
    {noreply, State};

%%----------------------------------------------------------------
%% received remote puback message
%%----------------------------------------------------------------
handle_info({puback, #{packet_id := PkgId}}, State = #state{queue = Queue, mqueue_type = MqueueType}) ->
    % lists:keydelete(PkgId, 1, Queue)
    {noreply, State#state{queue = delete(MqueueType, PkgId, Queue)}};

handle_info({'EXIT', Pid, normal}, State = #state{client_pid = Pid}) ->
    emqx_logger:warning("[Bridge] stop ~p", [normal]),
    {noreply, State#state{client_pid = undefined}};

handle_info({'EXIT', Pid, Reason}, State = #state{client_pid = Pid,
                                                  reconnect_interval = ReconnectInterval}) ->
    emqx_logger:error("[Bridge] stop ~p", [Reason]),
    erlang:send_after(ReconnectInterval, self(), start),
    {noreply, State#state{client_pid = undefined}};

handle_info(Info, State) ->
    emqx_logger:error("[Bridge] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

subscribe_remote_topics(ClientPid, Subscriptions) ->
    [begin emqx_client:subscribe(ClientPid, {bin(Topic), Qos}), {bin(Topic), Qos} end
        || {Topic, Qos} <- Subscriptions, emqx_topic:validate({filter, bin(Topic)})].

subscribe_local_topics(Topics) ->
    [begin emqx_broker:subscribe(bin(Topic)), bin(Topic) end
        || Topic <- Topics, emqx_topic:validate({filter, bin(Topic)})].

proto_ver(mqttv3) -> v3;
proto_ver(mqttv4) -> v4;
proto_ver(mqttv5) -> v5.
address(Address) ->
    case string:tokens(Address, ":") of
        [Host] -> {Host, 1883};
        [Host, Port] -> {Host, list_to_integer(Port)}
    end.
options(Options) ->
    options(Options, []).
options([], Acc) ->
    Acc;
options([{username, Username}| Options], Acc) ->
    options(Options, [{username, Username}|Acc]);
options([{proto_ver, ProtoVer}| Options], Acc) ->
    options(Options, [{proto_ver, proto_ver(ProtoVer)}|Acc]);
options([{password, Password}| Options], Acc) ->
    options(Options, [{password, Password}|Acc]);
options([{keepalive, Keepalive}| Options], Acc) ->
    options(Options, [{keepalive, Keepalive}|Acc]);
options([{client_id, ClientId}| Options], Acc) ->
    options(Options, [{client_id, ClientId}|Acc]);
options([{clean_start, CleanStart}| Options], Acc) ->
    options(Options, [{clean_start, CleanStart}|Acc]);
options([{address, Address}| Options], Acc) ->
    {Host, Port} = address(Address),
    options(Options, [{host, Host}, {port, Port}|Acc]);
options([{ssl, Ssl}| Options], Acc) ->
    options(Options, [{ssl, Ssl}|Acc]);
options([{ssl_opts, SslOpts}| Options], Acc) ->
    options(Options, [{ssl_opts, SslOpts}|Acc]);
options([_Option | Options], Acc) ->
    options(Options, Acc).

name(Id) ->
    list_to_atom(lists:concat([?MODULE, "_", Id])).

bin(L) -> iolist_to_binary(L).

mountpoint(undefined, Topic) ->
    Topic;
mountpoint(Prefix, Topic) ->
    <<Prefix/binary, Topic/binary>>.

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(bin(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

store(memory, Data, Queue, MaxPendingMsg) when length(Queue) =< MaxPendingMsg ->
    [Data | Queue];
store(memory, _Data, Queue, _MaxPendingMsg) ->
    lager:error("Beyond max pending messages"),
    Queue;
store(disk, Data, Queue, _MaxPendingMsg)->
    [Data | Queue].

delete(memory, PkgId, Queue) ->
    lists:keydelete(PkgId, 1, Queue);
delete(disk, PkgId, Queue) ->
    lists:keydelete(PkgId, 1, Queue).
