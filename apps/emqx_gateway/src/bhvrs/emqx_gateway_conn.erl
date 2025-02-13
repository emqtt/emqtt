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

%% @doc The behavior abstract for TCP based gateway conn
-module(emqx_gateway_conn).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/3,
    stop/1
]).

-export([
    info/1,
    stats/1
]).

-export([
    call/2,
    call/3,
    cast/2
]).

%% Callback
-export([init/6]).

%% Sys callbacks
-export([
    system_continue/3,
    system_terminate/4,
    system_code_change/4,
    system_get_state/1
]).

%% Internal callback
-export([wakeup_from_hib/2, recvloop/2]).

%% for channel module
-export([keepalive_stats/1]).

-record(state, {
    %% TCP/SSL/UDP/DTLS Wrapped Socket
    socket :: {esockd_transport, esockd:socket()} | {udp, _, _} | {esockd_udp_proxy, _, _},
    %% Peername of the connection
    peername :: emqx_types:peername(),
    %% Sockname of the connection
    sockname :: emqx_types:peername(),
    %% Sock State
    sockstate :: emqx_types:sockstate(),
    %% The {active, N} option
    active_n :: pos_integer(),
    %% Limiter
    limiter :: option(emqx_limiter:limiter()),
    %% Limit Timer
    limit_timer :: option(reference()),
    %% Parse State
    parse_state :: emqx_gateway_frame:parse_state(),
    %% Serialize options
    serialize :: emqx_gateway_frame:serialize_options(),
    %% Channel State
    channel :: emqx_gateway_channel:channel(),
    %% GC State
    gc_state :: option(emqx_gc:gc_state()),
    %% Stats Timer
    stats_timer :: disabled | option(reference()),
    %% Idle Timeout
    idle_timeout :: integer(),
    %% Idle Timer
    idle_timer :: option(reference()),
    %% OOM Policy
    oom_policy :: option(emqx_types:oom_policy()),
    %% Frame Module
    frame_mod :: atom(),
    %% Channel Module
    chann_mod :: atom(),
    %% Listener Tag
    listener :: listener() | undefined
}).

-type listener() :: {GwName :: atom(), LisType :: atom(), LisName :: atom()}.
-type state() :: #state{}.

-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(ENABLED(X), (X =/= undefined)).

-dialyzer(
    {nowarn_function, [
        system_terminate/4,
        handle_call/3,
        handle_msg/2,
        shutdown/3,
        stop/3,
        parse_incoming/3
    ]}
).

%% udp
start_link(Socket = {udp, _SockPid, _Sock}, Peername, Options) ->
    Args = [self(), Socket, Peername, Options] ++ callback_modules(Options),
    {ok, proc_lib:spawn_link(?MODULE, init, Args)};
%% tcp/ssl/dtls
start_link(esockd_transport, Sock, Options) ->
    Socket = {esockd_transport, Sock},
    Args = [self(), Socket, undefined, Options] ++ callback_modules(Options),
    {ok, proc_lib:spawn_link(?MODULE, init, Args)};
start_link(Socket = {esockd_udp_proxy, _ProxyId, _Sock}, Peername, Options) ->
    Args = [self(), Socket, Peername, Options] ++ callback_modules(Options),
    {ok, proc_lib:spawn_link(?MODULE, init, Args)}.

callback_modules(Options) ->
    [
        maps:get(frame_mod, Options),
        maps:get(chann_mod, Options)
    ].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
-spec info(pid() | state()) -> emqx_types:infos().
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{chann_mod = ChannMod, channel = Channel}) ->
    ChanInfo = ChannMod:info(Channel),
    SockInfo = maps:from_list(
        info(?INFO_KEYS, State)
    ),
    ChanInfo#{sockinfo => SockInfo}.

info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, #state{socket = Socket}) ->
    esockd_type(Socket);
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(active_n, #state{active_n = ActiveN}) ->
    ActiveN.

-spec stats(pid() | state()) -> emqx_types:stats().
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{
    socket = Socket,
    chann_mod = ChannMod,
    channel = Channel
}) ->
    SockStats =
        case esockd_getstat(Socket, ?SOCK_STATS) of
            {ok, Ss} -> Ss;
            {error, _} -> []
        end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = ChannMod:stats(Channel),
    ProcStats = emqx_utils:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

call(Pid, Req) ->
    call(Pid, Req, infinity).

call(Pid, Req, Timeout) ->
    gen_server:call(Pid, Req, Timeout).

cast(Pid, Req) ->
    gen_server:cast(Pid, Req).

stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% Wrapped funcs
%%--------------------------------------------------------------------

esockd_peername({udp, _SockPid, _Sock}, Peername) ->
    Peername;
esockd_peername({esockd_transport, Sock}, _Peername) ->
    {ok, Peername} = esockd_transport:ensure_ok_or_exit(peername, [Sock]),
    Peername;
esockd_peername({esockd_udp_proxy, _ProxyId, _Sock}, Peername) ->
    Peername.

esockd_wait(Socket = {udp, _SockPid, _Sock}) ->
    {ok, Socket};
esockd_wait(Socket = {esockd_udp_proxy, _ProxyId, _Sock}) ->
    {ok, Socket};
esockd_wait({esockd_transport, Sock}) ->
    case esockd_transport:wait(Sock) of
        {ok, NSock} -> {ok, {esockd_transport, NSock}};
        R = {error, _} -> R
    end.

esockd_close({udp, _SockPid, _Sock}) ->
    %% nothing to do for udp socket
    %%gen_udp:close(Sock);
    ok;
esockd_close({esockd_transport, Sock}) ->
    esockd_transport:fast_close(Sock);
esockd_close({esockd_udp_proxy, ProxyId, _Sock}) ->
    esockd_udp_proxy:close(ProxyId).

esockd_ensure_ok_or_exit(peercert, {udp, _SockPid, _Sock}) ->
    nossl;
esockd_ensure_ok_or_exit(Fun, {udp, _SockPid, Sock}) ->
    esockd_transport:ensure_ok_or_exit(Fun, [Sock]);
esockd_ensure_ok_or_exit(Fun, {esockd_transport, Socket}) ->
    esockd_transport:ensure_ok_or_exit(Fun, [Socket]);
esockd_ensure_ok_or_exit(Fun, {esockd_udp_proxy, _ProxyId, Sock}) ->
    esockd_transport:ensure_ok_or_exit(Fun, [Sock]).

esockd_type({udp, _, _}) ->
    udp;
esockd_type({esockd_transport, Socket}) ->
    esockd_transport:type(Socket);
esockd_type({esockd_udp_proxy, _ProxyId, Sock}) when is_port(Sock) ->
    udp;
esockd_type({esockd_udp_proxy, _ProxyId, _Sock}) ->
    ssl.

esockd_setopts({udp, _, _}, _) ->
    ok;
esockd_setopts({esockd_transport, Socket}, Opts) ->
    %% FIXME: DTLS works??
    esockd_transport:setopts(Socket, Opts);
esockd_setopts({esockd_udp_proxy, _ProxyId, Socket}, Opts) ->
    esockd_transport:setopts(Socket, Opts).

esockd_getstat({udp, _SockPid, Sock}, Stats) ->
    inet:getstat(Sock, Stats);
esockd_getstat({esockd_transport, Sock}, Stats) ->
    esockd_transport:getstat(Sock, Stats);
esockd_getstat({esockd_udp_proxy, _ProxyId, Sock}, Stats) ->
    esockd_transport:getstat(Sock, Stats).

esockd_send(Data, #state{
    socket = {udp, _SockPid, Sock},
    peername = {Ip, Port}
}) ->
    gen_udp:send(Sock, Ip, Port, Data);
esockd_send(Data, #state{socket = {esockd_transport, Sock}}) ->
    esockd_transport:send(Sock, Data);
esockd_send(Data, #state{socket = {esockd_udp_proxy, ProxyId, _Sock}}) ->
    esockd_udp_proxy:send(ProxyId, Data).

keepalive_stats(recv) ->
    emqx_pd:get_counter(recv_pkt);
keepalive_stats(send) ->
    emqx_pd:get_counter(send_pkt).

is_datadram_socket({esockd_transport, _}) -> false;
is_datadram_socket({udp, _, _}) -> true;
is_datadram_socket({esockd_udp_proxy, _ProxyId, Sock}) -> erlang:is_port(Sock).

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

init(Parent, WrappedSock, Peername0, Options, FrameMod, ChannMod) ->
    case esockd_wait(WrappedSock) of
        {ok, NWrappedSock} ->
            Peername = esockd_peername(NWrappedSock, Peername0),
            run_loop(
                Parent,
                init_state(
                    NWrappedSock,
                    Peername,
                    Options,
                    FrameMod,
                    ChannMod
                )
            );
        {error, Reason} ->
            ok = esockd_close(WrappedSock),
            exit_on_sock_error(Reason)
    end.

init_state(WrappedSock, Peername, Options, FrameMod, ChannMod) ->
    {ok, Sockname} = esockd_ensure_ok_or_exit(sockname, WrappedSock),
    Peercert = esockd_ensure_ok_or_exit(peercert, WrappedSock),
    ConnInfo = #{
        socktype => esockd_type(WrappedSock),
        peername => Peername,
        sockname => Sockname,
        peercert => Peercert,
        conn_mod => ?MODULE
    },
    ActiveN = emqx_gateway_utils:active_n(Options),
    %% FIXME: TODO
    %%Limiter = emqx_limiter:init(Options),
    Limiter = undefined,
    FrameOpts = emqx_gateway_utils:frame_options(Options),
    ParseState = FrameMod:initial_parse_state(FrameOpts),
    Serialize = FrameMod:serialize_opts(),
    Channel = ChannMod:init(ConnInfo, Options),
    GcState = emqx_gateway_utils:init_gc_state(Options),
    StatsTimer = emqx_gateway_utils:stats_timer(Options),
    IdleTimeout = emqx_gateway_utils:idle_timeout(Options),
    OomPolicy = emqx_gateway_utils:oom_policy(Options),
    IdleTimer = emqx_utils:start_timer(IdleTimeout, idle_timeout),
    #state{
        socket = WrappedSock,
        peername = Peername,
        sockname = Sockname,
        sockstate = idle,
        active_n = ActiveN,
        limiter = Limiter,
        parse_state = ParseState,
        serialize = Serialize,
        channel = Channel,
        gc_state = GcState,
        stats_timer = StatsTimer,
        idle_timeout = IdleTimeout,
        idle_timer = IdleTimer,
        oom_policy = OomPolicy,
        frame_mod = FrameMod,
        chann_mod = ChannMod,
        listener = maps:get(listener, Options, undefined)
    }.

run_loop(
    Parent,
    State = #state{
        socket = Socket,
        peername = Peername,
        oom_policy = OomPolicy
    }
) ->
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    _ = emqx_utils:tune_heap_size(OomPolicy),
    case activate_socket(State) of
        {ok, NState} ->
            hibernate(Parent, NState);
        {error, Reason} ->
            ok = esockd_close(Socket),
            exit_on_sock_error(Reason)
    end.

-spec exit_on_sock_error(atom()) -> no_return().
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

recvloop(Parent, State = #state{idle_timeout = IdleTimeout}) ->
    receive
        Msg ->
            handle_recv(Msg, Parent, State)
    after IdleTimeout + 100 ->
        hibernate(Parent, cancel_stats_timer(State))
    end.

handle_recv({system, From, Request}, Parent, State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_recv({'EXIT', Parent, Reason}, Parent, State) ->
    %% FIXME: it's not trapping exit, should never receive an EXIT
    terminate(Reason, State);
handle_recv(Msg, Parent, State = #state{idle_timeout = IdleTimeout}) ->
    case process_msg([Msg], ensure_stats_timer(IdleTimeout, State)) of
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

ensure_stats_timer(Timeout, State = #state{stats_timer = undefined}) ->
    State#state{stats_timer = emqx_utils:start_timer(Timeout, emit_stats)};
ensure_stats_timer(_Timeout, State) ->
    State.

cancel_stats_timer(State = #state{stats_timer = TRef}) when
    is_reference(TRef)
->
    ok = emqx_utils:cancel_timer(TRef),
    State#state{stats_timer = undefined};
cancel_stats_timer(State) ->
    State.

%%--------------------------------------------------------------------
%% Process next Msg

process_msg([], State) ->
    {ok, State};
process_msg([Msg | More], State) ->
    try
        case handle_msg(Msg, State) of
            ok ->
                process_msg(More, State);
            {ok, NState} ->
                process_msg(More, NState);
            {ok, Msgs, NState} ->
                process_msg(append_msg(More, Msgs), NState);
            {stop, Reason, NState} ->
                {stop, Reason, NState}
        end
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
        {noreply, NState} ->
            {ok, NState};
        {noreply, Msgs, NState} ->
            {ok, next_msgs(Msgs), NState};
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {ok, NState};
        {reply, Reply, Msgs, NState} ->
            gen_server:reply(From, Reply),
            {ok, next_msgs(Msgs), NState};
        {stop, Reason, Reply, NState} ->
            gen_server:reply(From, Reply),
            stop(Reason, NState)
    end;
handle_msg({'$gen_cast', Req}, State) ->
    with_channel(handle_cast, [Req], State);
handle_msg({datagram, _SockPid, Data}, State) ->
    parse_incoming(Data, State);
handle_msg(
    {{esockd_udp_proxy, _ProxyId, _Socket} = NSock, Data, Packets},
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    ?SLOG(debug, #{msg => "received_udp_proxy_data", data => Data}),
    Oct = iolist_size(Data),
    inc_counter(incoming_bytes, Oct),
    Ctx = ChannMod:info(ctx, Channel),
    ok = emqx_gateway_ctx:metrics_inc(Ctx, 'bytes.received', Oct),

    NState = State#state{socket = NSock},
    {ok, next_incoming_msgs(Packets), NState};
handle_msg({Inet, _Sock, Data}, State) when
    Inet == tcp;
    Inet == ssl
->
    parse_incoming(Data, State);
handle_msg(
    {incoming, Packet},
    State = #state{idle_timer = IdleTimer}
) ->
    IdleTimer /= undefined andalso
        emqx_utils:cancel_timer(IdleTimer),
    NState = State#state{idle_timer = undefined},
    handle_incoming(Packet, NState);
handle_msg({outgoing, Data}, State) ->
    handle_outgoing(Data, State);
handle_msg({Error, _Sock, Reason}, State) when
    Error == tcp_error; Error == ssl_error
->
    handle_info({sock_error, Reason}, State);
handle_msg({Closed, _Sock}, State) when
    Closed == tcp_closed; Closed == ssl_closed
->
    handle_info({sock_closed, Closed}, close_socket(State));
%% TODO: udp_passive???
handle_msg({Passive, _Sock}, State) when
    Passive == tcp_passive; Passive == ssl_passive
->
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    Pubs = emqx_pd:reset_counter(incoming_pkt),
    %% Ensure Rate Limit
    NState = ensure_rate_limit(State),
    %% Run GC and Check OOM
    NState1 = check_oom(run_gc(Pubs, Bytes, NState)),
    handle_info(activate_socket, NState1);
handle_msg(
    Deliver = {deliver, _Topic, _Msg},
    State = #state{active_n = ActiveN}
) ->
    Delivers = [Deliver | emqx_utils:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);
handle_msg({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);
handle_msg({close, Reason}, State) ->
    ?tp(debug, force_socket_close, #{reason => Reason}),
    handle_info({sock_closed, Reason}, close_socket(State));
handle_msg(udp_proxy_closed, State) ->
    ?tp(debug, udp_proxy_closed, #{reason => normal}),
    handle_info({sock_closed, normal}, close_socket(State));
handle_msg(
    {event, connected},
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    Ctx = ChannMod:info(ctx, Channel),
    ClientId = ChannMod:info(clientid, Channel),
    emqx_gateway_ctx:insert_channel_info(
        Ctx,
        ClientId,
        info(State),
        stats(State)
    );
handle_msg(
    {event, disconnected},
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    Ctx = ChannMod:info(ctx, Channel),
    ClientId = ChannMod:info(clientid, Channel),
    emqx_gateway_ctx:set_chan_info(Ctx, ClientId, info(State)),
    emqx_gateway_ctx:connection_closed(Ctx, ClientId),
    {ok, State};
handle_msg(
    {event, _Other},
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    Ctx = ChannMod:info(ctx, Channel),
    ClientId = ChannMod:info(clientid, Channel),
    emqx_gateway_ctx:set_chan_info(Ctx, ClientId, info(State)),
    emqx_gateway_ctx:set_chan_stats(Ctx, ClientId, stats(State)),
    {ok, State};
handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);
handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);
handle_msg(Msg, State) ->
    handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

-spec terminate(atom(), state()) -> no_return().
terminate(
    Reason,
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    _ = ChannMod:terminate(Reason, Channel),
    _ = close_socket(State),
    ClientId =
        try ChannMod:info(clientid, Channel) of
            Id -> Id
        catch
            _:_ -> undefined
        end,
    ?tp(debug, conn_process_terminated, #{reason => Reason, clientid => ClientId}),
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
handle_call(
    From,
    Req,
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    case ChannMod:handle_call(Req, From, Channel) of
        {noreply, NChannel} ->
            {noreply, State#state{channel = NChannel}};
        {noreply, Msgs, NChannel} ->
            {noreply, Msgs, State#state{channel = NChannel}};
        {reply, Reply, NChannel} ->
            {reply, Reply, State#state{channel = NChannel}};
        {reply, Reply, Msgs, NChannel} ->
            {reply, Reply, Msgs, State#state{channel = NChannel}};
        {shutdown, Reason, Reply, NChannel} ->
            shutdown(Reason, Reply, State#state{channel = NChannel});
        {shutdown, Reason, Reply, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            {ok, NState1} = handle_outgoing(Packet, NState),
            shutdown(Reason, Reply, NState1)
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(_TRef, idle_timeout, State) ->
    shutdown(idle_timeout, State);
handle_timeout(_TRef, limit_timeout, State) ->
    NState = State#state{
        sockstate = idle,
        limit_timer = undefined
    },
    handle_info(activate_socket, NState);
handle_timeout(
    TRef,
    Keepalive,
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) when
    Keepalive == keepalive;
    Keepalive == keepalive_send
->
    StatVal =
        case Keepalive of
            keepalive -> keepalive_stats(recv);
            keepalive_send -> keepalive_stats(send)
        end,
    case ChannMod:info(conn_state, Channel) of
        disconnected ->
            {ok, State};
        _ ->
            handle_timeout(TRef, {Keepalive, StatVal}, State)
    end;
handle_timeout(
    _TRef,
    emit_stats,
    State =
        #state{chann_mod = ChannMod, channel = Channel}
) ->
    Ctx = ChannMod:info(ctx, Channel),
    ClientId = ChannMod:info(clientid, Channel),
    emqx_gateway_ctx:set_chan_stats(Ctx, ClientId, stats(State)),
    {ok, State#state{stats_timer = undefined}};
handle_timeout(TRef, Msg, State) ->
    with_channel(handle_timeout, [TRef, Msg], State).

%%--------------------------------------------------------------------
%% Parse incoming data

parse_incoming(
    Data,
    State = #state{
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    ?SLOG(debug, #{msg => "received_data", data => Data}),
    Oct = iolist_size(Data),
    inc_counter(incoming_bytes, Oct),
    Ctx = ChannMod:info(ctx, Channel),
    ok = emqx_gateway_ctx:metrics_inc(Ctx, 'bytes.received', Oct),
    {Packets, NState} = parse_incoming(Data, [], State),
    {ok, next_incoming_msgs(Packets), NState}.

parse_incoming(<<>>, Packets, State) ->
    {Packets, State};
parse_incoming(Data, Packets, State) ->
    #state{frame_mod = FrameMod, parse_state = ParseState} = State,
    try FrameMod:parse(Data, ParseState) of
        {more, NParseState} ->
            {Packets, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            parse_incoming(Rest, [Packet | Packets], NState)
    catch
        error:Reason:Stack ->
            ?SLOG(error, #{
                msg => "parse_frame_failed",
                at_state => ParseState,
                input_bytes => Data,
                reason => Reason,
                stacktrace => Stack
            }),
            {[{frame_error, Reason} | Packets], State}
    end.

next_incoming_msgs([Packet]) ->
    {incoming, Packet};
next_incoming_msgs(Packets) ->
    [{incoming, Packet} || Packet <- lists:reverse(Packets)].

%%--------------------------------------------------------------------
%% Handle incoming packet
handle_incoming(Packet, State) ->
    #state{channel = Channel, frame_mod = FrameMod, chann_mod = ChannMod} = State,
    Ctx = ChannMod:info(ctx, Channel),
    ok = inc_incoming_stats(Ctx, FrameMod, Packet),
    do_handle_incoming(Packet, FrameMod, State).

do_handle_incoming({frame_error, Reason}, _FrameMod, State) ->
    with_channel(handle_frame_error, [Reason], State);
do_handle_incoming(Packet, FrameMod, State) ->
    ?SLOG(debug, #{msg => "packet_received", packet => FrameMod:format(Packet)}),
    with_channel(handle_in, [Packet], State).

%%--------------------------------------------------------------------
%% With Channel

with_channel(Fun, Args, State = #state{chann_mod = ChannMod, channel = Channel}) ->
    case erlang:apply(ChannMod, Fun, Args ++ [Channel]) of
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
            {ok, NState1} = handle_outgoing(Packet, NState),
            shutdown(Reason, NState1)
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(_Packets = [], State) ->
    {ok, State};
handle_outgoing(
    Packets,
    State = #state{socket = Socket}
) when is_list(Packets) ->
    case is_datadram_socket(Socket) of
        false ->
            send(
                lists:map(serialize_and_inc_stats_fun(State), Packets),
                State
            );
        _ ->
            NState = lists:foldl(
                fun(Packet, State0) ->
                    {ok, State1} = handle_outgoing(Packet, State0),
                    State1
                end,
                State,
                Packets
            ),
            {ok, NState}
    end;
handle_outgoing(Packet, State) ->
    send((serialize_and_inc_stats_fun(State))(Packet), State).

serialize_and_inc_stats_fun(#state{
    frame_mod = FrameMod,
    chann_mod = ChannMod,
    serialize = Serialize,
    channel = Channel
}) ->
    Ctx = ChannMod:info(ctx, Channel),
    fun(Packet) ->
        try
            Data = FrameMod:serialize_pkt(Packet, Serialize),
            ?SLOG(debug, #{
                msg => "send_packet",
                %% XXX: optimize it, less cpu comsuption?
                packet => FrameMod:format(Packet)
            }),
            ok = inc_outgoing_stats(Ctx, FrameMod, Packet),
            Data
        catch
            _:too_large ->
                ?SLOG(warning, #{
                    msg => "packet_too_large_discarded",
                    packet => FrameMod:format(Packet)
                }),
                ok = emqx_gateway_ctx:metrics_inc(Ctx, 'delivery.dropped.too_large'),
                ok = emqx_gateway_ctx:metrics_inc(Ctx, 'delivery.dropped'),
                <<>>;
            _:Reason ->
                ?SLOG(warning, #{
                    msg => "packet_serialize_failure",
                    reason => Reason,
                    packet => FrameMod:format(Packet)
                }),
                ok = emqx_gateway_ctx:metrics_inc(Ctx, 'delivery.dropped'),
                <<>>
        end
    end.

%%--------------------------------------------------------------------
%% Send data

-spec send(iodata(), state()) -> {ok, state()}.
send(
    IoData,
    State = #state{
        socket = Socket,
        chann_mod = ChannMod,
        channel = Channel
    }
) ->
    ?SLOG(debug, #{msg => "send_data", data => IoData}),
    Ctx = ChannMod:info(ctx, Channel),
    Oct = iolist_size(IoData),
    ok = emqx_gateway_ctx:metrics_inc(Ctx, 'bytes.sent', Oct),
    inc_counter(outgoing_bytes, Oct),
    case esockd_send(IoData, State) of
        ok ->
            sent(State);
        Error = {error, _Reason} ->
            %% Send an inet_reply to defer handling the error
            self() ! {inet_reply, Socket, Error},
            {ok, State}
    end.

sent(#state{active_n = ActiveN} = State) ->
    case emqx_pd:get_counter(outgoing_pkt) > ActiveN of
        true ->
            Pubs = emqx_pd:reset_counter(outgoing_pkt),
            Bytes = emqx_pd:reset_counter(outgoing_bytes),
            {ok, check_oom(run_gc(Pubs, Bytes, State))};
        false ->
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info(activate_socket, State = #state{sockstate = OldSst}) ->
    case activate_socket(State) of
        {ok, NState = #state{sockstate = NewSst}} ->
            if
                OldSst =/= NewSst ->
                    {ok, {event, NewSst}, NState};
                true ->
                    {ok, NState}
            end;
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;
handle_info({sock_error, Reason}, State) ->
    ?SLOG(debug, #{
        msg => "gateway_sock_error",
        reason => Reason
    }),
    handle_info({sock_closed, Reason}, close_socket(State));
handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Ensure rate limit

%% ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
%%     case ?ENABLED(Limiter) andalso emqx_limiter:check(Stats, Limiter) of
%%         false ->
%%             State;
%%         {ok, Limiter1} ->
%%             State#state{limiter = Limiter1};
%%         {pause, Time, Limiter1} ->
%%             %% XXX: which limiter reached?
%%             ?SLOG(warning, #{
%%                 msg => "reach_rate_limit",
%%                 pause => Time
%%             }),
%%             TRef = emqx_utils:start_timer(Time, limit_timeout),
%%             State#state{
%%                 sockstate = blocked,
%%                 limiter = Limiter1,
%%                 limit_timer = TRef
%%             }
%%     end.

%% TODO
%% Why do we need this?
%% Why not use the esockd connection limiter (based on emqx_htb_limiter) directly?
ensure_rate_limit(State) ->
    State.

%%--------------------------------------------------------------------
%% Run GC and Check OOM

run_gc(Pubs, Bytes, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Pubs, Bytes, GcSt) of
        false -> State;
        {_IsGC, GcSt1} -> State#state{gc_state = GcSt1}
    end.

check_oom(State = #state{oom_policy = OomPolicy}) ->
    case ?ENABLED(OomPolicy) andalso emqx_utils:check_oom(OomPolicy) of
        {shutdown, Reason} ->
            %% triggers terminate/2 callback immediately
            erlang:exit({shutdown, Reason});
        _Other ->
            ok
    end,
    State.

%%--------------------------------------------------------------------
%% Activate Socket

activate_socket(State = #state{sockstate = closed}) ->
    {ok, State};
activate_socket(State = #state{sockstate = blocked}) ->
    {ok, State};
activate_socket(
    State = #state{
        socket = Socket,
        active_n = N
    }
) ->
    %% FIXME: Works on dtls/udp ???
    %%        How to handle buffer?
    case esockd_setopts(Socket, [{active, N}]) of
        ok -> {ok, State#state{sockstate = running}};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Close Socket

close_socket(State = #state{sockstate = closed}) ->
    State;
close_socket(State = #state{socket = Socket}) ->
    ok = esockd_close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

inc_incoming_stats(Ctx, FrameMod, Packet) ->
    do_inc_incoming_stats(FrameMod:type(Packet), Ctx, FrameMod, Packet).

%% If a mailformed packet is received, the type of the packet is undefined.
do_inc_incoming_stats(undefined, _Ctx, _FrameMod, _Packet) ->
    ok;
do_inc_incoming_stats(Type, Ctx, FrameMod, Packet) ->
    inc_counter(recv_pkt, 1),
    case FrameMod:is_message(Packet) of
        true ->
            inc_counter(recv_msg, 1),
            inc_counter(incoming_pubs, 1);
        false ->
            ok
    end,
    Name = list_to_atom(lists:concat(["packets.", Type, ".received"])),
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

inc_outgoing_stats(Ctx, FrameMod, Packet) ->
    inc_counter(send_pkt, 1),
    case FrameMod:is_message(Packet) of
        true ->
            inc_counter(send_msg, 1),
            inc_counter(outgoing_pubs, 1);
        false ->
            ok
    end,
    Name = list_to_atom(
        lists:concat(["packets.", FrameMod:type(Packet), ".sent"])
    ),
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

%%--------------------------------------------------------------------
%% Helper functions

next_msgs(Event) when is_tuple(Event) ->
    Event;
next_msgs(More) when is_list(More) ->
    More.

shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

shutdown(Reason, Reply, State) ->
    stop({shutdown, Reason}, Reply, State).

stop(Reason, State) ->
    {stop, Reason, State}.

stop(Reason, Reply, State) ->
    {stop, Reason, Reply, State}.

inc_counter(Name, Value) ->
    _ = emqx_pd:inc_counter(Name, Value),
    ok.
