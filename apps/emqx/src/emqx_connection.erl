%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module interacts with the transport layer of MQTT
%% Transport:
%%   - TCP connection
%%   - TCP/TLS connection
%%   - QUIC Stream
%%
%% for WebSocket @see emqx_ws_connection.erl
-module(emqx_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_external_trace.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-elvis([{elvis_style, used_ignored_variable, disable}]).
-elvis([{elvis_style, invalid_dynamic_call, #{ignore => [emqx_connection]}}]).

%% API
-export([
    start_link/3,
    stop/1
]).

-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    async_set_keepalive/3,
    async_set_keepalive/5,
    async_set_socket_options/2
]).

-export([
    call/2,
    call/3,
    cast/2
]).

%% Callback
-export([init/4]).

%% Sys callbacks
-export([
    system_continue/3,
    system_terminate/4,
    system_code_change/4,
    system_get_state/1
]).

%% Internal callback
-export([wakeup_from_hib/2, recvloop/2, get_state/1]).

%% Export for CT
-export([set_field/3]).

-export_type([
    state/0,
    parser/0
]).

-record(state, {
    %% TCP/TLS Transport
    transport :: esockd:transport(),
    %% TCP/TLS Socket
    socket :: esockd:socket() | emqx_quic_stream:socket(),
    %% Peername of the connection
    peername :: emqx_types:peername(),
    %% Sockname of the connection
    sockname :: emqx_types:peername(),
    %% Sock State
    sockstate :: emqx_types:sockstate(),
    %% Packet parser / serializer
    parser :: parser(),
    serialize :: emqx_frame:serialize_opts(),
    %% Channel State
    channel :: emqx_channel:channel(),
    %% GC State
    gc_state :: option(emqx_gc:gc_state()),
    %% Stats Timer
    %% When `disabled` stats are never reported.
    %% When `paused` stats are not reported until complete CONNECT packet received.
    %% Connection starts with `paused` by default.
    stats_timer :: disabled | paused | option(reference()),
    %% Idle Timer
    idle_timer :: option(reference()),
    %% Hibernate connection process if inactive for
    hibernate_after :: integer() | infinity,
    %% Zone name
    zone :: atom(),
    %% Listener Type and Name
    listener :: {Type :: atom(), Name :: atom()},

    %% QUIC conn shared state
    quic_conn_ss :: option(map()),

    %% Extra field for future hot-upgrade support
    extra = []
}).

-type parser() ::
    %% Special, slightly better optimized "complete-frames" parser.
    %% Expected to be enabled when `{packet, mqtt}` is used for MQTT framing.
    {frame, emqx_frame:parse_state_initial()}
    %% Bytestream parser.
    | _Stream :: emqx_frame:parse_state().

-opaque state() :: #state{}.

-define(ACTIVE_N, 10).

-define(INFO_KEYS, [
    socktype,
    peername,
    sockname,
    sockstate
]).

-define(SOCK_STATS, [
    recv_oct,
    recv_cnt,
    send_oct,
    send_cnt,
    send_pend
]).

-define(ENABLED(X), (X =/= undefined)).

-define(LOG(Level, Data), ?SLOG(Level, (Data)#{tag => "MQTT"})).

-dialyzer({no_match, [info/2]}).
-dialyzer(
    {nowarn_function, [
        init/4,
        init_state/3,
        run_loop/2,
        system_terminate/4,
        system_code_change/4
    ]}
).
-dialyzer({no_missing_calls, [handle_msg/2]}).

-ifndef(BUILD_WITHOUT_QUIC).
-spec start_link
    (esockd:transport(), esockd:socket(), emqx_channel:opts()) ->
        {ok, pid()};
    (
        emqx_quic_stream,
        {ConnOwner :: pid(), quicer:connection_handle(), quicer:new_conn_props()},
        emqx_quic_connection:cb_state()
    ) ->
        {ok, pid()}.
-else.
-spec start_link(esockd:transport(), esockd:socket(), emqx_channel:opts()) -> {ok, pid()}.
-endif.

start_link(Transport, Socket, Options) ->
    Args = [self(), Transport, Socket, Options],
    CPid = proc_lib:spawn_link(?MODULE, init, Args),
    {ok, CPid}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
-spec info(pid() | state()) -> emqx_types:infos().
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

-spec info([atom()] | atom() | tuple(), pid() | state()) -> term().
info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, #state{transport = Transport, socket = Socket}) ->
    Transport:type(Socket);
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(stats_timer, #state{stats_timer = StatsTimer}) ->
    StatsTimer;
info({channel, Info}, #state{channel = Channel}) ->
    emqx_channel:info(Info, Channel).

%% @doc Get stats of the connection/channel.
-spec stats(pid() | state()) -> emqx_types:stats().
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{
    transport = Transport,
    socket = Socket,
    channel = Channel
}) ->
    SockStats =
        case Transport:getstat(Socket, ?SOCK_STATS) of
            {ok, Ss} -> Ss;
            {error, _} -> []
        end,
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_utils:proc_stats(),
    lists:append([SockStats, ChanStats, ProcStats]).

%% @doc Set TCP keepalive socket options to override system defaults.
%% Idle: The number of seconds a connection needs to be idle before
%%       TCP begins sending out keep-alive probes (Linux default 7200).
%% Interval: The number of seconds between TCP keep-alive probes
%%           (Linux default 75).
%% Probes: The maximum number of TCP keep-alive probes to send before
%%         giving up and killing the connection if no response is
%%         obtained from the other end (Linux default 9).
%%
%% NOTE: This API sets TCP socket options, which has nothing to do with
%%       the MQTT layer's keepalive (PINGREQ and PINGRESP).
async_set_keepalive(Idle, Interval, Probes) ->
    async_set_keepalive(os:type(), self(), Idle, Interval, Probes).

async_set_keepalive(OS, Pid, Idle, Interval, Probes) ->
    case emqx_schema:tcp_keepalive_opts(OS, Idle, Interval, Probes) of
        {ok, Options} ->
            async_set_socket_options(Pid, Options);
        {error, {unsupported_os, OS}} ->
            ?LOG(warning, #{
                msg => "Unsupported operation: set TCP keepalive",
                os => OS
            }),
            ok
    end.

%% @doc Set custom socket options.
%% This API is made async because the call might be originated from
%% a hookpoint callback (otherwise deadlock).
%% If failed to set, the error message is logged.
async_set_socket_options(Pid, Options) ->
    cast(Pid, {async_set_socket_options, Options}).

cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

call(Pid, Req) ->
    call(Pid, Req, infinity).
call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

init(Parent, Transport, RawSocket, Options) ->
    case Transport:wait(RawSocket) of
        {ok, Socket} ->
            run_loop(Parent, init_state(Transport, Socket, Options));
        {error, Reason} ->
            ok = Transport:fast_close(RawSocket),
            exit_on_sock_error(Reason)
    end.

init_state(
    Transport,
    Socket,
    #{zone := Zone, listener := Listener} = Opts
) ->
    {ok, Peername} = Transport:ensure_ok_or_exit(peername, [Socket]),
    {ok, Sockname} = Transport:ensure_ok_or_exit(sockname, [Socket]),
    Peercert = Transport:ensure_ok_or_exit(peercert, [Socket]),
    PeerSNI = Transport:ensure_ok_or_exit(peersni, [Socket]),
    ConnInfo = #{
        socktype => Transport:type(Socket),
        peername => Peername,
        sockname => Sockname,
        peercert => Peercert,
        peersni => PeerSNI,
        conn_mod => ?MODULE,
        sock => Socket
    },

    FrameOpts = #{
        strict_mode => emqx_config:get_zone_conf(Zone, [mqtt, strict_mode]),
        max_size => emqx_config:get_zone_conf(Zone, [mqtt, max_packet_size])
    },
    Parser = init_parser(Transport, Socket, FrameOpts),
    Serialize = emqx_frame:initial_serialize_opts(FrameOpts),
    %% Init Channel
    Channel = emqx_channel:init(ConnInfo, Opts),
    GcState =
        case emqx_config:get_zone_conf(Zone, [force_gc]) of
            #{enable := false} -> undefined;
            GcPolicy -> emqx_gc:init(GcPolicy)
        end,
    StatsTimer =
        case emqx_config:get_zone_conf(Zone, [stats, enable]) of
            true -> paused;
            false -> disabled
        end,

    #state{
        transport = Transport,
        socket = Socket,
        peername = Peername,
        sockname = Sockname,
        sockstate = idle,
        parser = Parser,
        serialize = Serialize,
        channel = Channel,
        gc_state = GcState,
        stats_timer = StatsTimer,
        hibernate_after = maps:get(hibernate_after, Opts, get_zone_idle_timeout(Zone)),
        zone = Zone,
        listener = Listener,
        %% for quic streams to inherit
        quic_conn_ss = maps:get(conn_shared_state, Opts, undefined),
        extra = []
    }.

run_loop(
    Parent,
    State = #state{
        transport = Transport,
        socket = Socket,
        peername = Peername,
        listener = Listener,
        zone = Zone
    }
) ->
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    ShutdownPolicy = emqx_config:get_zone_conf(Zone, [force_shutdown]),
    emqx_utils:tune_heap_size(ShutdownPolicy),
    case activate_socket(State) of
        {ok, NState} ->
            ok = set_tcp_keepalive(Listener),
            IdleTimeout = get_zone_idle_timeout(Zone),
            IdleTimer = start_timer(IdleTimeout, idle_timeout),
            hibernate(Parent, NState#state{idle_timer = IdleTimer});
        {error, Reason} ->
            ok = Transport:fast_close(Socket),
            exit_on_sock_error(Reason)
    end.

-spec exit_on_sock_error(any()) -> no_return().
exit_on_sock_error(Reason) when
    Reason =:= einval;
    Reason =:= enotconn;
    Reason =:= closed
->
    erlang:exit(normal);
exit_on_sock_error(timeout) ->
    erlang:exit({shutdown, ssl_upgrade_timeout});
exit_on_sock_error(Reason) ->
    erlang:exit({shutdown, Reason}).

%%--------------------------------------------------------------------
%% Recv Loop

recvloop(
    Parent,
    State = #state{
        hibernate_after = HibernateAfterMs,
        zone = Zone
    }
) ->
    HibernateTimeout =
        case HibernateAfterMs of
            infinity -> infinity;
            _ -> HibernateAfterMs
        end,
    receive
        Msg ->
            handle_recv(Msg, Parent, State)
    after HibernateTimeout ->
        case emqx_olp:backoff_hibernation(Zone) of
            true ->
                recvloop(Parent, State);
            false ->
                _ = try_set_chan_stats(State),
                hibernate(Parent, cancel_stats_timer(State))
        end
    end.

handle_recv({system, From, Request}, Parent, State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_recv({'EXIT', Parent, Reason}, Parent, State) ->
    %% FIXME: it's not trapping exit, should never receive an EXIT
    terminate(Reason, State);
handle_recv(Msg, Parent, State) ->
    case process_msg([Msg], ensure_stats_timer(State)) of
        {ok, NewState} ->
            ?MODULE:recvloop(Parent, NewState);
        {stop, Reason, NewSate} ->
            terminate(Reason, NewSate)
    end.

hibernate(Parent, State) ->
    proc_lib:hibernate(?MODULE, wakeup_from_hib, [Parent, State]).

%% Maybe do something here later.
wakeup_from_hib(Parent, State) ->
    ?MODULE:recvloop(Parent, State).

%%--------------------------------------------------------------------
%% Ensure/cancel stats timer

-compile({inline, [ensure_stats_timer/1]}).
ensure_stats_timer(State = #state{stats_timer = undefined}) ->
    Timeout = get_zone_idle_timeout(State#state.zone),
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) ->
    %% Either already active, disabled, or paused.
    State.

-compile({inline, [resume_stats_timer/1]}).
resume_stats_timer(State = #state{stats_timer = paused}) ->
    State#state{stats_timer = undefined};
resume_stats_timer(State = #state{stats_timer = disabled}) ->
    State.

-compile({inline, [cancel_stats_timer/1]}).
cancel_stats_timer(State = #state{stats_timer = TRef}) when is_reference(TRef) ->
    ?tp(debug, cancel_stats_timer, #{}),
    ok = emqx_utils:cancel_timer(TRef),
    State#state{stats_timer = undefined};
cancel_stats_timer(State) ->
    State.

-compile({inline, [get_zone_idle_timeout/1]}).
get_zone_idle_timeout(Zone) ->
    emqx_channel:get_mqtt_conf(Zone, idle_timeout).

%%--------------------------------------------------------------------
%% Process next Msg

process_msg([], State) ->
    {ok, State};
process_msg([Msg | More], State) ->
    try handle_msg(Msg, State) of
        ok ->
            process_msg(More, State);
        {ok, NState} ->
            process_msg(More, NState);
        {ok, Msgs, NState} ->
            process_msg(append_msg(More, Msgs), NState);
        {stop, Reason, NState} ->
            {stop, Reason, NState};
        {stop, Reason} ->
            {stop, Reason, State}
    catch
        exit:normal ->
            {stop, normal, State};
        exit:shutdown ->
            {stop, shutdown, State};
        exit:{shutdown, _} = Shutdown ->
            {stop, Shutdown, State};
        Exception:Context:Stack ->
            {stop,
                #{
                    exception => Exception,
                    context => Context,
                    stacktrace => Stack
                },
                State}
    end.

-compile({inline, [append_msg/2]}).
append_msg([], Msgs) when is_list(Msgs) ->
    Msgs;
append_msg([], Msg) ->
    [Msg];
append_msg(Q, Msgs) when is_list(Msgs) ->
    lists:append(Q, Msgs);
append_msg(Q, Msg) ->
    lists:append(Q, [Msg]).

%%--------------------------------------------------------------------
%% Handle a Msg
handle_msg({'$gen_call', From, Req}, State) ->
    case handle_call(From, Req, State) of
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {ok, NState};
        {stop, Reason, Reply, NState} ->
            gen_server:reply(From, Reply),
            stop(Reason, NState)
    end;
handle_msg({'$gen_cast', Req}, State) ->
    NewState = handle_cast(Req, State),
    {ok, NewState};
handle_msg({Inet, _Sock, Data}, State) when Inet == tcp; Inet == ssl ->
    Oct = iolist_size(Data),
    inc_counter(incoming_bytes, Oct),
    ok = emqx_metrics:inc('bytes.received', Oct),
    on_bytes_in(Oct, Data, State);
handle_msg({quic, Data, _Stream, #{len := Len}}, State) when is_binary(Data) ->
    inc_counter(incoming_bytes, Len),
    ok = emqx_metrics:inc('bytes.received', Len),
    on_bytes_in(Len, Data, State);
handle_msg(
    {incoming, Packet = ?CONNECT_PACKET(ConnPkt)},
    State = #state{idle_timer = IdleTimer}
) ->
    ok = emqx_utils:cancel_timer(IdleTimer),
    NState = State#state{
        serialize = emqx_frame:serialize_opts(ConnPkt),
        idle_timer = undefined
    },
    handle_incoming(Packet, NState);
handle_msg({incoming, Packet}, State) ->
    ?TRACE("MQTT", "mqtt_packet_received", #{packet => Packet}),
    handle_incoming(Packet, State);
handle_msg({outgoing, Packets}, State) ->
    handle_outgoing(Packets, State);
handle_msg({Error, _Sock, Reason}, State) when
    Error == tcp_error; Error == ssl_error
->
    handle_info({sock_error, Reason}, State);
handle_msg({Closed, _Sock}, State) when
    Closed == tcp_closed; Closed == ssl_closed
->
    handle_info({sock_closed, Closed}, close_socket(State));
handle_msg({Passive, _Sock}, State) when
    Passive == tcp_passive; Passive == ssl_passive; Passive =:= quic_passive
->
    Pubs = emqx_pd:reset_counter(incoming_pubs),
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    %% Run GC and Check OOM
    NState1 = check_oom(Pubs, Bytes, run_gc(Pubs, Bytes, State)),
    handle_info(activate_socket, NState1);
handle_msg(
    Deliver = {deliver, _Topic, _Msg},
    #state{listener = {Type, Listener}} = State
) ->
    ActiveN = get_active_n(Type, Listener),
    Delivers = [Deliver | emqx_utils:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);
handle_msg({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);
handle_msg({connack, ConnAck}, State) ->
    handle_outgoing(ConnAck, State);
handle_msg({close, Reason}, State) ->
    %% @FIXME here it could be close due to appl error.
    ?TRACE("SOCKET", "socket_force_closed", #{reason => Reason}),
    handle_info({sock_closed, Reason}, close_socket(State));
handle_msg(
    {event, connected},
    State = #state{
        channel = Channel,
        parser = Parser,
        serialize = Serialize,
        quic_conn_ss = QSS
    }
) ->
    QSS =/= undefined andalso
        emqx_quic_connection:activate_data_streams(
            maps:get(conn_pid, QSS),
            {get_parser_state(Parser), Serialize, Channel}
        ),
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
    {ok, resume_stats_timer(State)};
handle_msg({event, disconnected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    {ok, State};
handle_msg({event, _Other}, State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientId is yet unknown (i.e. connect packet is not received yet)
        undefined -> ok;
        ClientId -> emqx_cm:insert_channel_info(ClientId, info(State), stats(State))
    end,
    {ok, State};
handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);
handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);
handle_msg(Msg, State) ->
    handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

-spec terminate(any(), state()) -> no_return().
terminate(
    Reason,
    State = #state{
        channel = Channel,
        transport = Transport,
        socket = Socket
    }
) ->
    try
        Channel1 = emqx_channel:set_conn_state(disconnected, Channel),
        emqx_congestion:cancel_alarms(Socket, Transport, Channel1),
        emqx_channel:terminate(Reason, Channel1),
        close_socket_ok(State),
        ?TRACE("SOCKET", "emqx_connection_terminated", #{reason => Reason})
    catch
        E:C:S ->
            ?tp(warning, unclean_terminate, #{exception => E, context => C, stacktrace => S})
    end,
    ?tp(info, terminate, #{reason => Reason}),
    maybe_raise_exception(Reason).

%% close socket, discard new state, always return ok.
close_socket_ok(State) ->
    _ = close_socket(State),
    ok.

%% tell truth about the original exception
-spec maybe_raise_exception(any()) -> no_return().
maybe_raise_exception(#{
    exception := Exception,
    context := Context,
    stacktrace := Stacktrace
}) ->
    erlang:raise(Exception, Context, Stacktrace);
maybe_raise_exception({shutdown, normal}) ->
    ok;
maybe_raise_exception(normal) ->
    ok;
maybe_raise_exception(shutdown) ->
    ok;
maybe_raise_exception(Reason) ->
    exit(Reason).

%%--------------------------------------------------------------------
%% Sys callbacks

system_continue(Parent, _Debug, State) ->
    ?MODULE:recvloop(Parent, State).

system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State).

system_code_change(State, _Mod, _OldVsn, _Extra) ->
    {ok, State}.

system_get_state(State) -> {ok, State}.

%%--------------------------------------------------------------------
%% Handle call

handle_call(_From, info, State) ->
    {reply, info(State), State};
handle_call(_From, stats, State) ->
    {reply, stats(State), State};
handle_call(_From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            {reply, Reply, State#state{channel = NChannel}};
        {shutdown, Reason, Reply, NChannel} ->
            shutdown(Reason, Reply, State#state{channel = NChannel});
        {shutdown, Reason, Reply, OutPacket, NChannel} ->
            NState = State#state{channel = NChannel},
            {ok, NState2} = handle_outgoing(OutPacket, NState),
            NState3 = graceful_shutdown_transport(Reason, NState2),
            shutdown(Reason, Reply, NState3)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(_TRef, idle_timeout, State) ->
    shutdown(idle_timeout, State);
handle_timeout(
    _TRef,
    emit_stats,
    State = #state{
        channel = Channel,
        transport = Transport,
        socket = Socket
    }
) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    emqx_congestion:maybe_alarm_conn_congestion(Socket, Transport, Channel),
    {ok, State#state{stats_timer = undefined}};
handle_timeout(
    TRef,
    keepalive,
    State = #state{
        channel = Channel
    }
) ->
    case emqx_channel:info(conn_state, Channel) of
        disconnected ->
            {ok, State};
        _ ->
            with_channel(handle_timeout, [TRef, keepalive], State)
    end;
handle_timeout(TRef, Msg, State) ->
    with_channel(handle_timeout, [TRef, Msg], State).

try_set_chan_stats(State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientID is not yet known, nothing to report.
        undefined -> false;
        ClientId -> emqx_cm:set_chan_stats(ClientId, stats(State))
    end.

%%--------------------------------------------------------------------
%% Parse incoming data
-compile({inline, [on_bytes_in/3]}).
on_bytes_in(Oct, Data, State) ->
    ?LOG(debug, #{
        msg => "raw_bin_received",
        size => Oct,
        bin => binary_to_list(binary:encode_hex(Data)),
        type => "hex"
    }),
    {Packets, NState} = parse_incoming(Data, State),
    {ok, next_incoming_msgs(Packets), NState}.

%% @doc: return a reversed Msg list
-compile({inline, [next_incoming_msgs/1]}).
next_incoming_msgs([Packet]) ->
    {incoming, Packet};
next_incoming_msgs(Packets) ->
    Fun = fun(Packet, Acc) -> [{incoming, Packet} | Acc] end,
    lists:foldl(Fun, [], Packets).

parse_incoming(Data, State = #state{parser = Parser}) ->
    try
        run_parser(Data, Parser, State)
    catch
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?LOG(info, #{
                msg => "frame_parse_error",
                reason => Reason,
                at_state => describe_parser_state(Parser),
                input_bytes => Data
            }),
            NState = update_state_on_parse_error(Parser, Reason, State),
            {[{frame_error, Reason}], NState};
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "frame_parse_failed",
                at_state => describe_parser_state(Parser),
                input_bytes => Data,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {[{frame_error, Reason}], State}
    end.

init_parser(Transport, Socket, FrameOpts) ->
    {ok, SocketOpts} = Transport:getopts(Socket, [packet]),
    case lists:keyfind(packet, 1, SocketOpts) of
        {packet, mqtt} ->
            %% Enable whole-frame packet parser.
            {frame, emqx_frame:initial_parse_state(FrameOpts)};
        _Otherwise ->
            %% Go with regular streaming parser.
            emqx_frame:initial_parse_state(FrameOpts)
    end.

update_state_on_parse_error(
    ParseState0,
    #{proto_ver := ProtoVer, parse_state := ParseState},
    State
) ->
    Serialize = emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE),
    case ParseState0 of
        {frame, _Options} -> NParseState = {frame, ParseState};
        _StreamParseState -> NParseState = ParseState
    end,
    State#state{serialize = Serialize, parser = NParseState};
update_state_on_parse_error(_, _, State) ->
    State.

run_parser(Data, {frame, Options}, State) ->
    run_frame_parser(Data, Options, State);
run_parser(Data, ParseState, State) ->
    run_stream_parser(Data, [], ParseState, State).

-compile({inline, [run_stream_parser/4]}).
run_stream_parser(<<>>, Acc, NParseState, State) ->
    {Acc, State#state{parser = NParseState}};
run_stream_parser(Data, Acc, ParseState, State) ->
    case emqx_frame:parse(Data, ParseState) of
        {Packet, Rest, NParseState} ->
            run_stream_parser(Rest, [Packet | Acc], NParseState, State);
        {more, NParseState} ->
            {Acc, State#state{parser = NParseState}}
    end.

-compile({inline, [run_frame_parser/3]}).
run_frame_parser(Data, Options, State) ->
    case emqx_frame:parse_complete(Data, Options) of
        Packet when is_tuple(Packet) ->
            {[Packet], State};
        [Packet, NOptions] ->
            NState = State#state{parser = {frame, NOptions}},
            {[Packet], NState}
    end.

describe_parser_state(ParseState) ->
    emqx_frame:describe_state(ParseState).

get_parser_state({frame, Options}) ->
    Options;
get_parser_state(ParseState) ->
    ParseState.

%%--------------------------------------------------------------------
%% Handle incoming packet

handle_incoming(Packet, #state{quic_conn_ss = QSS} = State) when is_record(Packet, mqtt_packet) ->
    QSS =/= undefined andalso
        emqx_quic_connection:step_cnt(
            maps:get(cnts_ref, QSS),
            control_packet,
            1
        ),
    ok = inc_incoming_stats(Packet),
    with_channel(handle_in, [Packet], State);
handle_incoming(FrameError, State) ->
    with_channel(handle_in, [FrameError], State).

%%--------------------------------------------------------------------
%% With Channel

with_channel(Fun, Args, State = #state{channel = Channel}) ->
    case erlang:apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok ->
            {ok, State};
        {ok, NChannel} ->
            {ok, State#state{channel = NChannel}};
        {ok, Replies, NChannel} ->
            {ok, next_msgs(Replies), State#state{channel = NChannel}};
        {shutdown, Reason, NChannel} ->
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            {ok, NState2} = handle_outgoing(Packet, NState),
            shutdown(Reason, NState2)
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State = #state{channel = _Channel}) ->
    Res = do_handle_outgoing(Packets, State),
    _ = ?EXT_TRACE_OUTGOING_STOP(
        emqx_external_trace:basic_attrs(_Channel),
        Packets
    ),
    Res.

do_handle_outgoing(Packets, State) when is_list(Packets) ->
    send([serialize_and_inc_stats(State, Packet) || Packet <- Packets], State);
do_handle_outgoing(Packet, State) ->
    send(serialize_and_inc_stats(State, Packet), State).

serialize_and_inc_stats(#state{serialize = Serialize}, Packet) ->
    try emqx_frame:serialize_pkt(Packet, Serialize) of
        <<>> ->
            ?LOG(warning, #{
                msg => "packet_is_discarded",
                reason => "frame_is_too_large",
                packet => emqx_packet:format(Packet, hidden)
            }),
            ok = emqx_metrics:inc('delivery.dropped.too_large'),
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = inc_outgoing_stats({error, message_too_large}),
            <<>>;
        Data ->
            ?TRACE("MQTT", "mqtt_packet_sent", #{packet => Packet}),
            ok = inc_outgoing_stats(Packet),
            Data
    catch
        %% Maybe Never happen.
        throw:{?FRAME_SERIALIZE_ERROR, Reason} ->
            ?LOG(info, #{
                reason => Reason,
                input_packet => Packet
            }),
            erlang:error({?FRAME_SERIALIZE_ERROR, Reason});
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                input_packet => Packet,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            erlang:error(?FRAME_SERIALIZE_ERROR)
    end.

%%--------------------------------------------------------------------
%% Send data

-spec send(iodata(), state()) -> {ok, state()}.
send(IoData, #state{transport = Transport, socket = Socket} = State) ->
    Oct = iolist_size(IoData),
    emqx_metrics:inc('bytes.sent', Oct),
    inc_counter(outgoing_bytes, Oct),
    case Transport:send(Socket, IoData) of
        ok ->
            %% NOTE: for Transport=emqx_quic_stream, it's actually an
            %% async_send, sent/1 should technically be called when
            %% {quic, send_complete, _Stream, true | false} is received,
            %% but it is handled early for simplicity
            sent(State);
        Error = {error, _Reason} ->
            %% Defer error handling
            %% so it's handled the same as tcp_closed or ssl_closed
            self() ! {inet_reply, Socket, Error},
            {ok, State}
    end.

%% Some bytes sent
sent(#state{listener = {Type, Listener}} = State) ->
    %% Run GC and check OOM after certain amount of messages or bytes sent.
    case emqx_pd:get_counter(outgoing_pubs) > get_active_n(Type, Listener) of
        true ->
            Pubs = emqx_pd:reset_counter(outgoing_pubs),
            Bytes = emqx_pd:reset_counter(outgoing_bytes),
            {ok, check_oom(Pubs, Bytes, run_gc(Pubs, Bytes, State))};
        false ->
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info(activate_socket, State = #state{sockstate = OldSst}) ->
    case activate_socket(State) of
        {ok, NState = #state{sockstate = NewSst}} ->
            case OldSst =/= NewSst of
                true -> {ok, {event, NewSst}, NState};
                false -> {ok, NState}
            end;
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;
handle_info({sock_error, Reason}, State) ->
    case Reason =/= closed andalso Reason =/= einval of
        true -> ?SLOG(warning, #{msg => "socket_error", reason => Reason});
        false -> ok
    end,
    handle_info({sock_closed, Reason}, close_socket(State));
%% handle QUIC control stream events
handle_info({quic, Event, Handle, Prop}, State) when is_atom(Event) ->
    case emqx_quic_stream:Event(Handle, Prop, State) of
        {{continue, Msgs}, NewState} ->
            {ok, Msgs, NewState};
        Other ->
            Other
    end;
handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Handle Info

handle_cast(
    {async_set_socket_options, Opts},
    State = #state{
        transport = Transport,
        socket = Socket
    }
) ->
    case Transport:setopts(Socket, Opts) of
        ok ->
            ?tp(debug, "custom_socket_options_successfully", #{opts => Opts});
        {error, einval} ->
            %% socket is already closed, ignore this error
            ?tp(debug, "socket already closed", #{reason => socket_already_closed}),
            ok;
        Err ->
            %% other errors
            ?tp(error, "failed_to_set_custom_socket_option", #{reason => Err})
    end,
    State;
handle_cast(Req, State) ->
    ?tp(error, "received_unknown_cast", #{cast => Req}),
    State.

%%--------------------------------------------------------------------
%% Run GC and Check OOM

run_gc(Pubs, Bytes, State = #state{gc_state = GcSt, zone = Zone}) ->
    case
        ?ENABLED(GcSt) andalso not emqx_olp:backoff_gc(Zone) andalso
            emqx_gc:run(Pubs, Bytes, GcSt)
    of
        false -> State;
        {_IsGC, GcSt1} -> State#state{gc_state = GcSt1}
    end.

check_oom(Pubs, Bytes, State = #state{zone = Zone}) ->
    ShutdownPolicy = emqx_config:get_zone_conf(Zone, [force_shutdown]),
    case emqx_utils:check_oom(ShutdownPolicy) of
        {shutdown, Reason} ->
            %% triggers terminate/2 callback immediately
            ?tp(warning, check_oom_shutdown, #{
                policy => ShutdownPolicy,
                incoming_pubs => Pubs,
                incoming_bytes => Bytes,
                shutdown => Reason
            }),
            erlang:exit({shutdown, Reason});
        Result ->
            ?tp(debug, check_oom_ok, #{
                policy => ShutdownPolicy,
                incoming_pubs => Pubs,
                incoming_bytes => Bytes,
                result => Result
            }),
            ok
    end,
    State.

%%--------------------------------------------------------------------
%% Activate Socket
%% TODO: maybe we could keep socket passive for receiving socket closed event.
-compile({inline, [activate_socket/1]}).
activate_socket(
    #state{
        transport = Transport,
        sockstate = SockState,
        socket = Socket,
        listener = {Type, Listener}
    } = State
) when
    SockState =/= closed
->
    ActiveN = get_active_n(Type, Listener),
    case Transport:setopts(Socket, [{active, ActiveN}]) of
        ok -> {ok, State#state{sockstate = running}};
        Error -> Error
    end;
activate_socket(State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Close Socket

close_socket(State = #state{sockstate = closed}) ->
    State;
close_socket(State = #state{transport = emqx_quic_stream, sockstate = read_aborted}) ->
    wait_for_quic_stream_close(State);
close_socket(State = #state{transport = Transport, socket = Socket}) ->
    ok = Transport:fast_close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline, [inc_incoming_stats/1]}).
inc_incoming_stats(Packet = ?PACKET(Type)) ->
    inc_counter(recv_pkt, 1),
    case Type =:= ?PUBLISH of
        true ->
            inc_counter(recv_msg, 1),
            inc_qos_stats(recv_msg, Packet),
            inc_counter(incoming_pubs, 1);
        false ->
            ok
    end,
    emqx_metrics:inc_recv(Packet).

-compile({inline, [inc_outgoing_stats/1]}).
inc_outgoing_stats({error, message_too_large}) ->
    inc_counter('send_msg.dropped', 1),
    inc_counter('send_msg.dropped.too_large', 1);
inc_outgoing_stats(Packet = ?PACKET(Type)) ->
    inc_counter(send_pkt, 1),
    case Type of
        ?PUBLISH ->
            inc_counter(send_msg, 1),
            inc_counter(outgoing_pubs, 1),
            inc_qos_stats(send_msg, Packet);
        _ ->
            ok
    end,
    emqx_metrics:inc_sent(Packet).

inc_qos_stats(Type, Packet) ->
    case inc_qos_stats_key(Type, emqx_packet:qos(Packet)) of
        undefined ->
            ignore;
        Key ->
            inc_counter(Key, 1)
    end.

inc_qos_stats_key(send_msg, ?QOS_0) -> 'send_msg.qos0';
inc_qos_stats_key(send_msg, ?QOS_1) -> 'send_msg.qos1';
inc_qos_stats_key(send_msg, ?QOS_2) -> 'send_msg.qos2';
inc_qos_stats_key(recv_msg, ?QOS_0) -> 'recv_msg.qos0';
inc_qos_stats_key(recv_msg, ?QOS_1) -> 'recv_msg.qos1';
inc_qos_stats_key(recv_msg, ?QOS_2) -> 'recv_msg.qos2';
%% for bad qos
inc_qos_stats_key(_, _) -> undefined.

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline, [next_msgs/1]}).
next_msgs(Packet) when is_record(Packet, mqtt_packet) ->
    {outgoing, Packet};
next_msgs(Event) when is_tuple(Event) ->
    Event;
next_msgs(More) when is_list(More) ->
    More.

-compile({inline, [shutdown/2, shutdown/3]}).
shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

shutdown(Reason, Reply, State) ->
    stop({shutdown, Reason}, Reply, State).

-compile({inline, [stop/2, stop/3]}).
stop(Reason, State) ->
    {stop, Reason, State}.

stop(Reason, Reply, State) ->
    {stop, Reason, Reply, State}.

inc_counter(Key, Inc) ->
    _ = emqx_pd:inc_counter(Key, Inc),
    ok.

set_tcp_keepalive({quic, _Listener}) ->
    ok;
set_tcp_keepalive({Type, Id}) ->
    Conf = emqx_config:get_listener_conf(Type, Id, [tcp_options, keepalive], "none"),
    case Conf of
        "none" ->
            ok;
        Value ->
            %% the value is already validated by schema, so we do not validate it again.
            {Idle, Interval, Probes} = emqx_schema:parse_tcp_keepalive(Value),
            async_set_keepalive(Idle, Interval, Probes)
    end.

-spec graceful_shutdown_transport(atom(), state()) -> state().
graceful_shutdown_transport(
    Reason,
    S = #state{
        transport = emqx_quic_stream,
        socket = Socket
    }
) when Reason =:= takenover; Reason =:= kicked; Reason =:= discarded ->
    _ = emqx_quic_stream:abort_read(Socket, Reason),
    S#state{sockstate = read_aborted};
graceful_shutdown_transport(_Reason, S = #state{transport = Transport, socket = Socket}) ->
    _ = Transport:shutdown(Socket, read_write),
    S#state{sockstate = closed}.

%% @doc wait for stream close or stream read shutdown complete
%%      this also ensures write shutdown are completed.
%%      see emqx_quic_stream:abort_read/2
%% @end
-spec wait_for_quic_stream_close(state()) -> state().
wait_for_quic_stream_close(
    State = #state{
        transport = emqx_quic_stream,
        socket = {quic, _Conn, Stream, _},
        sockstate = read_aborted
    }
) ->
    ok = emqx_quic_stream:wait_for_close(Stream),
    State#state{sockstate = closed}.

start_timer(Time, Msg) ->
    emqx_utils:start_timer(Time, Msg).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, State) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, state)),
    setelement(Pos + 1, State, Value).

get_state(Pid) ->
    State = sys:get_state(Pid),
    maps:from_list(
        lists:zip(
            record_info(fields, state),
            tl(tuple_to_list(State))
        )
    ).

get_active_n(quic, _Listener) ->
    ?ACTIVE_N;
get_active_n(Type, Listener) ->
    emqx_config:get_listener_conf(Type, Listener, [tcp_options, active_n]).
