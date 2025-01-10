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

%% MQTT/WS|WSS Connection
-module(emqx_ws_connection).

-include("emqx_cm.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% API
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    call/2,
    call/3
]).

%% WebSocket callbacks
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    websocket_close/2,
    terminate/3
]).

%% Export for CT
-export([set_field/3]).

-import(
    emqx_utils,
    [
        maybe_apply/2,
        start_timer/2
    ]
).

-record(state, {
    %% Peername of the ws connection
    peername :: emqx_types:peername(),
    %% Sockname of the ws connection
    sockname :: emqx_types:peername(),
    %% Sock state
    sockstate :: emqx_types:sockstate(),
    %% MQTT Piggyback
    mqtt_piggyback :: single | multiple,
    %% Parse State
    parse_state :: emqx_frame:parse_state(),
    %% Serialize options
    serialize :: emqx_frame:serialize_opts(),
    %% Channel
    channel :: emqx_channel:channel(),
    %% GC State
    gc_state :: option(emqx_gc:gc_state()),
    %% Postponed Packets|Cmds|Events
    %% Order is reversed: most recent entry is the first element.
    postponed :: list(emqx_types:packet() | ws_cmd() | tuple()),
    %% Stats Timer
    stats_timer :: paused | disabled | option(reference()),
    %% Idle Timer
    idle_timer :: option(reference()),
    %% Zone name
    zone :: atom(),
    %% Listener Type and Name
    listener :: {Type :: atom(), Name :: atom()},

    %% Extra field for future hot-upgrade support
    extra = []
}).

-type state() :: #state{}.

-type ws_cmd() :: {active, boolean()} | close.

-define(INFO_KEYS, [socktype, peername, sockname, sockstate]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).

-define(ENABLED(X), (X =/= undefined)).

-define(LOG(Level, Data), ?SLOG(Level, ?MAPPEND(Data, #{tag => "MQTT"}))).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

-type info() :: atom() | {channel, _Info}.

-spec info(pid() | state()) -> emqx_types:infos().
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_channel:info(Channel),
    SockInfo = maps:from_list(
        info(?INFO_KEYS, State)
    ),
    ChanInfo#{sockinfo => SockInfo}.

-spec info
    (info(), state()) -> _Value;
    (info(), state()) -> [{atom(), _Value}].
info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, _State) ->
    ws;
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(channel, #state{channel = Channel}) ->
    emqx_channel:info(Channel);
info(gc_state, #state{gc_state = GcSt}) ->
    maybe_apply(fun emqx_gc:info/1, GcSt);
info(postponed, #state{postponed = Postponed}) ->
    Postponed;
info(stats_timer, #state{stats_timer = TRef}) ->
    TRef;
info(idle_timer, #state{idle_timer = TRef}) ->
    TRef;
info({channel, Info}, #state{channel = Channel}) ->
    emqx_channel:info(Info, Channel).

-spec stats(pid() | state()) -> emqx_types:stats().
stats(WsPid) when is_pid(WsPid) ->
    call(WsPid, stats);
stats(#state{channel = Channel}) ->
    SockStats = emqx_pd:get_counters(?SOCK_STATS),
    ChanStats = emqx_channel:stats(Channel),
    ProcStats = emqx_utils:proc_stats(),
    lists:append([SockStats, ChanStats, ProcStats]).

%% kick|discard|takeover
-spec call(pid(), Req :: term()) -> Reply :: term().
call(WsPid, Req) ->
    call(WsPid, Req, 5000).

call(WsPid, Req, Timeout) when is_pid(WsPid) ->
    Mref = erlang:monitor(process, WsPid),
    WsPid ! {call, {self(), Mref}, Req},
    receive
        {Mref, Reply} ->
            ok = emqx_pmon:demonitor(Mref),
            Reply;
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after Timeout ->
        ok = emqx_pmon:demonitor(Mref),
        exit(timeout)
    end.

%%--------------------------------------------------------------------
%% WebSocket callbacks
%%--------------------------------------------------------------------

init(Req, #{listener := {Type, Listener}} = Opts) ->
    %% WS Transport Idle Timeout
    WsOpts = #{
        compress => get_ws_opts(Type, Listener, compress),
        deflate_opts => get_ws_opts(Type, Listener, deflate_opts),
        max_frame_size => get_ws_opts(Type, Listener, max_frame_size),
        idle_timeout => get_ws_opts(Type, Listener, idle_timeout),
        validate_utf8 => get_ws_opts(Type, Listener, validate_utf8)
    },
    case check_origin_header(Req, Opts) of
        {error, Reason} ->
            ?SLOG(error, #{msg => "invalid_origin_header", reason => Reason}),
            {ok, cowboy_req:reply(403, Req), WsOpts};
        ok ->
            parse_sec_websocket_protocol(Req, Opts, WsOpts)
    end.

parse_sec_websocket_protocol(Req, #{listener := {Type, Listener}} = Opts, WsOpts) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            case get_ws_opts(Type, Listener, fail_if_no_subprotocol) of
                true ->
                    {ok, cowboy_req:reply(400, Req), WsOpts};
                false ->
                    {cowboy_websocket, Req, [Req, Opts], WsOpts}
            end;
        Subprotocols ->
            SupportedSubprotocols = get_ws_opts(Type, Listener, supported_subprotocols),
            NSupportedSubprotocols = [
                list_to_binary(Subprotocol)
             || Subprotocol <- SupportedSubprotocols
            ],
            case pick_subprotocol(Subprotocols, NSupportedSubprotocols) of
                {ok, Subprotocol} ->
                    Resp = cowboy_req:set_resp_header(
                        <<"sec-websocket-protocol">>,
                        Subprotocol,
                        Req
                    ),
                    {cowboy_websocket, Resp, [Req, Opts], WsOpts};
                {error, no_supported_subprotocol} ->
                    {ok, cowboy_req:reply(400, Req), WsOpts}
            end
    end.

pick_subprotocol([], _SupportedSubprotocols) ->
    {error, no_supported_subprotocol};
pick_subprotocol([Subprotocol | Rest], SupportedSubprotocols) ->
    case lists:member(Subprotocol, SupportedSubprotocols) of
        true ->
            {ok, Subprotocol};
        false ->
            pick_subprotocol(Rest, SupportedSubprotocols)
    end.

parse_header_fun_origin(Req, #{listener := {Type, Listener}}) ->
    case cowboy_req:header(<<"origin">>, Req) of
        undefined ->
            case get_ws_opts(Type, Listener, allow_origin_absence) of
                true -> ok;
                false -> {error, origin_header_cannot_be_absent}
            end;
        Value ->
            case lists:member(Value, get_ws_opts(Type, Listener, check_origins)) of
                true -> ok;
                false -> {error, #{bad_origin => Value}}
            end
    end.

check_origin_header(Req, #{listener := {Type, Listener}} = Opts) ->
    case get_ws_opts(Type, Listener, check_origin_enable) of
        true -> parse_header_fun_origin(Req, Opts);
        false -> ok
    end.

websocket_init([Req, Opts]) ->
    #{zone := Zone, listener := {Type, Listener}} = Opts,
    case check_max_connection(Type, Listener) of
        allow ->
            {Peername, PeerCert, PeerSNI} = get_peer_info(Type, Listener, Req, Opts),
            Sockname = cowboy_req:sock(Req),
            WsCookie = get_ws_cookie(Req),
            ConnInfo = #{
                socktype => ws,
                peername => Peername,
                sockname => Sockname,
                peercert => PeerCert,
                peersni => PeerSNI,
                ws_cookie => WsCookie,
                conn_mod => ?MODULE
            },
            MQTTPiggyback = get_ws_opts(Type, Listener, mqtt_piggyback),
            FrameOpts = #{
                strict_mode => emqx_config:get_zone_conf(Zone, [mqtt, strict_mode]),
                max_size => emqx_config:get_zone_conf(Zone, [mqtt, max_packet_size])
            },
            ParseState = emqx_frame:initial_parse_state(FrameOpts),
            Serialize = emqx_frame:initial_serialize_opts(FrameOpts),
            Channel = emqx_channel:init(ConnInfo, Opts),
            GcState = get_force_gc(Zone),
            StatsTimer = get_stats_enable(Zone),
            %% MQTT Idle Timeout
            IdleTimeout = emqx_channel:get_mqtt_conf(Zone, idle_timeout),
            IdleTimer = start_timer(IdleTimeout, idle_timeout),
            _ = tune_heap_size(Channel),
            emqx_logger:set_metadata_peername(esockd:format(Peername)),
            {ok,
                #state{
                    peername = Peername,
                    sockname = Sockname,
                    sockstate = running,
                    mqtt_piggyback = MQTTPiggyback,
                    parse_state = ParseState,
                    serialize = Serialize,
                    channel = Channel,
                    gc_state = GcState,
                    postponed = [],
                    stats_timer = StatsTimer,
                    idle_timer = IdleTimer,
                    zone = Zone,
                    listener = {Type, Listener},
                    extra = []
                },
                hibernate};
        {denny, Reason} ->
            {stop, Reason}
    end.

tune_heap_size(Channel) ->
    case
        emqx_config:get_zone_conf(
            emqx_channel:info(zone, Channel),
            [force_shutdown]
        )
    of
        #{enable := false} -> ok;
        ShutdownPolicy -> emqx_utils:tune_heap_size(ShutdownPolicy)
    end.

get_stats_enable(Zone) ->
    case emqx_config:get_zone_conf(Zone, [stats, enable]) of
        true -> paused;
        false -> disabled
    end.

get_force_gc(Zone) ->
    case emqx_config:get_zone_conf(Zone, [force_gc]) of
        #{enable := false} -> undefined;
        GcPolicy -> emqx_gc:init(GcPolicy)
    end.

get_ws_cookie(Req) ->
    try
        cowboy_req:parse_cookies(Req)
    catch
        error:badarg ->
            ?SLOG(error, #{msg => "bad_cookie"}),
            undefined;
        Error:Reason ->
            ?SLOG(error, #{
                msg => "failed_to_parse_cookie",
                exception => Error,
                reason => Reason
            }),
            undefined
    end.

get_peer_info(Type, Listener, Req, Opts) ->
    Host = maps:get(host, Req, undefined),
    case
        emqx_config:get_listener_conf(Type, Listener, [proxy_protocol]) andalso
            maps:get(proxy_header, Req)
    of
        #{src_address := SrcAddr, src_port := SrcPort, ssl := SSL} = ProxyInfo ->
            SourceName = {SrcAddr, SrcPort},
            %% Notice: CN is only available in Proxy Protocol V2 additional info.
            %% `CN` is unsupported in Proxy Protocol V1
            %% `pp2_ssl_cn` is required by config `peer_cert_as_username` or `peer_cert_as_clientid`.
            %% It will be parsed by esockd.
            %% See also `emqx_channel:set_peercert_infos/3` and `esockd_peercert:common_name/1`
            SourceSSL =
                case maps:get(cn, SSL, undefined) of
                    undefined -> undefined;
                    CN -> [{pp2_ssl_cn, CN}]
                end,
            PeerSNI = maps:get(authority, ProxyInfo, Host),
            {SourceName, SourceSSL, PeerSNI};
        #{src_address := SrcAddr, src_port := SrcPort} = ProxyInfo ->
            PeerSNI = maps:get(authority, ProxyInfo, Host),
            SourceName = {SrcAddr, SrcPort},
            {SourceName, nossl, PeerSNI};
        _ ->
            {get_peer(Req, Opts), cowboy_req:cert(Req), Host}
    end.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);
websocket_handle({binary, Data}, State) ->
    ?LOG(debug, #{
        msg => "raw_bin_received",
        size => iolist_size(Data),
        bin => binary_to_list(binary:encode_hex(Data)),
        type => "hex"
    }),
    State2 = ensure_stats_timer(State),
    {Packets, State3} = parse_incoming(Data, [], State2),
    LenMsg = erlang:length(Packets),
    ByteSize = erlang:iolist_size(Data),
    inc_recv_stats(LenMsg, ByteSize),
    State4 = postpone(Packets, State3),
    return(State4);
%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State) when Frame =:= ping; Frame =:= pong ->
    return(State);
websocket_handle({Frame, _}, State) when Frame =:= ping; Frame =:= pong ->
    return(State);
websocket_handle({Frame, _}, State) ->
    %% TODO: should not close the ws connection
    ?LOG(error, #{msg => "unexpected_frame", frame => Frame}),
    shutdown(unexpected_ws_frame, State).

websocket_info({call, From, Req}, State) ->
    handle_call(From, Req, State);
websocket_info({cast, rate_limit}, State) ->
    Cnt = emqx_pd:reset_counter(incoming_pubs),
    Oct = emqx_pd:reset_counter(incoming_bytes),
    return(postpone({check_gc, Cnt, Oct}, State));
websocket_info({cast, Msg}, State) ->
    handle_info(Msg, State);
websocket_info({incoming, Packet = ?CONNECT_PACKET(ConnPkt)}, State) ->
    Serialize = emqx_frame:serialize_opts(ConnPkt),
    NState = State#state{serialize = Serialize},
    handle_incoming(Packet, cancel_idle_timer(NState));
websocket_info({incoming, Packet}, State) ->
    ?TRACE("WS-MQTT", "mqtt_packet_received", #{packet => Packet}),
    handle_incoming(Packet, State);
websocket_info({outgoing, Packets}, State) ->
    return(enqueue(Packets, State));
websocket_info({check_gc, Cnt, Oct}, State) ->
    return(check_oom(run_gc(Cnt, Oct, State)));
websocket_info(
    Deliver = {deliver, _Topic, _Msg},
    State = #state{listener = {Type, Listener}}
) ->
    ActiveN = get_active_n(Type, Listener),
    Delivers = [Deliver | emqx_utils:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);
websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);
websocket_info({shutdown, Reason}, State) ->
    shutdown(Reason, State);
websocket_info({stop, Reason}, State) ->
    shutdown(Reason, State);
websocket_info(Info, State) ->
    handle_info(Info, State).

websocket_close({_, ReasonCode, _Payload}, State) when is_integer(ReasonCode) ->
    websocket_close(ReasonCode, State);
websocket_close(Reason, State) ->
    ?TRACE("SOCKET", "websocket_closed", #{reason => Reason}),
    handle_info({sock_closed, Reason}, State).

terminate(Reason, _Req, #state{channel = Channel}) ->
    ?TRACE("SOCKET", "websocket_terminated", #{reason => Reason}),
    emqx_channel:terminate(Reason, Channel);
terminate(_Reason, _Req, _UnExpectedState) ->
    ok.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

handle_call(From, info, State) ->
    gen_server:reply(From, info(State)),
    return(State);
handle_call(From, stats, State) ->
    gen_server:reply(From, stats(State)),
    return(State);
handle_call(From, Req, State = #state{channel = Channel}) ->
    case emqx_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            return(State#state{channel = NChannel});
        {shutdown, Reason, Reply, NChannel} ->
            gen_server:reply(From, Reply),
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Reply, Packet, NChannel} ->
            gen_server:reply(From, Reply),
            NState = State#state{channel = NChannel},
            shutdown(Reason, enqueue(Packet, NState))
    end.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

handle_info({connack, ConnAck}, State) ->
    return(enqueue(ConnAck, State));
handle_info({close, Reason}, State) ->
    ?TRACE("SOCKET", "socket_force_closed", #{reason => Reason}),
    return(enqueue({close, Reason}, State));
handle_info({event, connected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:insert_channel_info(ClientId, info(State), stats(State)),
    NState = resume_stats_timer(State),
    return(NState);
handle_info({event, disconnected}, State = #state{channel = Channel}) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_info(ClientId, info(State)),
    return(State);
handle_info({event, _Other}, State = #state{channel = Channel}) ->
    case emqx_channel:info(clientid, Channel) of
        %% ClientId is yet unknown (i.e. connect packet is not received yet)
        undefined ->
            ok;
        ClientId ->
            emqx_cm:set_chan_info(ClientId, info(State)),
            emqx_cm:set_chan_stats(ClientId, stats(State))
    end,
    return(State);
handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(TRef, idle_timeout, State = #state{idle_timer = TRef}) ->
    shutdown(idle_timeout, State);
handle_timeout(TRef, keepalive, State) when is_reference(TRef) ->
    with_channel(handle_timeout, [TRef, keepalive], State);
handle_timeout(
    TRef,
    emit_stats,
    State = #state{
        channel = Channel,
        stats_timer = TRef
    }
) ->
    ClientId = emqx_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    return(State#state{stats_timer = undefined});
handle_timeout(TRef, TMsg, State) ->
    with_channel(handle_timeout, [TRef, TMsg], State).

%%--------------------------------------------------------------------
%% Run GC, Check OOM
%%--------------------------------------------------------------------

run_gc(Cnt, Oct, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Cnt, Oct, GcSt) of
        false -> State;
        {_IsGC, GcSt1} -> State#state{gc_state = GcSt1}
    end.

check_oom(State = #state{channel = Channel}) ->
    ShutdownPolicy = emqx_config:get_zone_conf(
        emqx_channel:info(zone, Channel), [force_shutdown]
    ),
    case ShutdownPolicy of
        #{enable := false} ->
            State;
        #{enable := true} ->
            case emqx_utils:check_oom(ShutdownPolicy) of
                Shutdown = {shutdown, _Reason} ->
                    postpone(Shutdown, State);
                _Other ->
                    State
            end
    end.

%%--------------------------------------------------------------------
%% Parse incoming data
%%--------------------------------------------------------------------

parse_incoming(<<>>, Packets, State) ->
    {lists:reverse(Packets), State};
parse_incoming(Data, Packets, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {Packets, State#state{parse_state = NParseState}};
        {Packet, Rest, NParseState} ->
            NState = State#state{parse_state = NParseState},
            parse_incoming(Rest, [{incoming, Packet} | Packets], NState)
    catch
        throw:{?FRAME_PARSE_ERROR, Reason} ->
            ?LOG(info, #{
                msg => "frame_parse_error",
                reason => Reason,
                at_state => emqx_frame:describe_state(ParseState),
                input_bytes => Data
            }),
            FrameError = {frame_error, Reason},
            NState = enrich_state(Reason, State),
            {[{incoming, FrameError} | Packets], NState};
        error:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "frame_parse_failed",
                at_state => emqx_frame:describe_state(ParseState),
                input_bytes => Data,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            FrameError = {frame_error, Reason},
            {[{incoming, FrameError} | Packets], State}
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

handle_incoming(Packet, State = #state{listener = {Type, Listener}}) when
    is_record(Packet, mqtt_packet)
->
    ok = inc_incoming_stats(Packet),
    NState =
        case
            emqx_pd:get_counter(incoming_pubs) >
                get_active_n(Type, Listener)
        of
            true -> postpone({cast, rate_limit}, State);
            false -> State
        end,
    with_channel(handle_in, [Packet], NState);
handle_incoming(FrameError, State) ->
    with_channel(handle_in, [FrameError], State).

%%--------------------------------------------------------------------
%% With Channel
%%--------------------------------------------------------------------

with_channel(Fun, Args, State = #state{channel = Channel}) ->
    case erlang:apply(emqx_channel, Fun, Args ++ [Channel]) of
        ok ->
            return(State);
        {ok, NChannel} ->
            return(State#state{channel = NChannel});
        {ok, Replies, NChannel} ->
            return(postpone(Replies, State#state{channel = NChannel}));
        {shutdown, Reason, NChannel} ->
            shutdown(Reason, State#state{channel = NChannel});
        {shutdown, Reason, Packet, NChannel} ->
            NState = State#state{channel = NChannel},
            shutdown(Reason, postpone(Packet, NState))
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets
%%--------------------------------------------------------------------

handle_outgoing(
    Packets,
    State = #state{
        mqtt_piggyback = MQTTPiggyback,
        listener = {Type, Listener}
    }
) ->
    IoData = lists:map(serialize_and_inc_stats_fun(State), Packets),
    Oct = iolist_size(IoData),
    ok = inc_sent_stats(length(Packets), Oct),
    NState =
        case
            emqx_pd:get_counter(outgoing_pubs) >
                get_active_n(Type, Listener)
        of
            true ->
                CntPubs = emqx_pd:reset_counter(outgoing_pubs),
                CntBytes = emqx_pd:reset_counter(outgoing_bytes),
                postpone({check_gc, CntPubs, CntBytes}, State);
            false ->
                State
        end,

    {
        case MQTTPiggyback of
            single -> [{binary, IoData}];
            multiple -> lists:map(fun(Bin) -> {binary, Bin} end, IoData)
        end,
        ensure_stats_timer(NState)
    }.

serialize_and_inc_stats_fun(#state{serialize = Serialize}) ->
    fun(Packet) ->
        try emqx_frame:serialize_pkt(Packet, Serialize) of
            <<>> ->
                ?LOG(warning, #{
                    msg => "packet_discarded",
                    reason => "frame_too_large",
                    packet => Packet
                }),
                ok = emqx_metrics:inc('delivery.dropped.too_large'),
                ok = emqx_metrics:inc('delivery.dropped'),
                ok = inc_outgoing_stats({error, message_too_large}),
                <<>>;
            Data ->
                ?TRACE("WS-MQTT", "mqtt_packet_sent", #{packet => Packet}),
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
        end
    end.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats
%%--------------------------------------------------------------------

-compile(
    {inline, [
        inc_recv_stats/2,
        inc_incoming_stats/1,
        inc_outgoing_stats/1,
        inc_sent_stats/2
    ]}
).

inc_recv_stats(Cnt, Oct) ->
    inc_counter(incoming_bytes, Oct),
    inc_counter(recv_cnt, Cnt),
    inc_counter(recv_oct, Oct),
    emqx_metrics:inc('bytes.received', Oct).

inc_incoming_stats(Packet = ?PACKET(Type)) ->
    _ = emqx_pd:inc_counter(recv_pkt, 1),
    case Type of
        ?PUBLISH ->
            inc_counter(recv_msg, 1),
            inc_qos_stats(recv_msg, Packet),
            inc_counter(incoming_pubs, 1);
        _ ->
            ok
    end,
    emqx_metrics:inc_recv(Packet).

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

inc_sent_stats(Cnt, Oct) ->
    inc_counter(outgoing_bytes, Oct),
    inc_counter(send_cnt, Cnt),
    inc_counter(send_oct, Oct),
    emqx_metrics:inc('bytes.sent', Oct).

inc_counter(Name, Value) ->
    _ = emqx_pd:inc_counter(Name, Value),
    ok.

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
%%--------------------------------------------------------------------

-compile({inline, [cancel_idle_timer/1, ensure_stats_timer/1]}).

%%--------------------------------------------------------------------
%% Cancel idle timer

cancel_idle_timer(State = #state{idle_timer = IdleTimer}) ->
    ok = emqx_utils:cancel_timer(IdleTimer),
    State#state{idle_timer = undefined}.

%%--------------------------------------------------------------------
%% Ensure stats timer

ensure_stats_timer(
    State = #state{
        zone = Zone,
        stats_timer = undefined
    }
) ->
    Timeout = emqx_channel:get_mqtt_conf(Zone, idle_timeout),
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(State) ->
    %% Either already active, disabled or paused.
    State.

resume_stats_timer(State = #state{stats_timer = paused}) ->
    State#state{stats_timer = undefined};
resume_stats_timer(State = #state{stats_timer = disabled}) ->
    State.

-compile({inline, [postpone/2, enqueue/2, return/1, shutdown/2]}).

%%--------------------------------------------------------------------
%% Postpone the packet, cmd or event

postpone(Event, State) when is_tuple(Event) ->
    enqueue(Event, State);
postpone(More, State) when is_list(More) ->
    lists:foldl(fun postpone/2, State, More).

enqueue([Packet], State = #state{postponed = Postponed}) ->
    State#state{postponed = [Packet | Postponed]};
enqueue(Packets, State = #state{postponed = Postponed}) when
    is_list(Packets)
->
    State#state{postponed = lists:reverse(Packets) ++ Postponed};
enqueue(Other, State = #state{postponed = Postponed}) ->
    State#state{postponed = [Other | Postponed]}.

shutdown(Reason, State = #state{postponed = Postponed}) ->
    return(State#state{postponed = [{shutdown, Reason} | Postponed]}).

return(State = #state{postponed = []}) ->
    {ok, State};
return(State = #state{postponed = Postponed}) ->
    {Packets, Cmds, Events} = classify(Postponed, [], [], []),
    ok = lists:foreach(fun trigger/1, Events),
    State1 = State#state{postponed = []},
    case {Packets, Cmds} of
        {[], []} ->
            {ok, State1};
        {[], Cmds} ->
            {Cmds, State1};
        {Packets, Cmds} ->
            {Frames, State2} = handle_outgoing(Packets, State1),
            {Frames ++ Cmds, State2}
    end.

classify([], Packets, Cmds, Events) ->
    {Packets, Cmds, Events};
classify([{outgoing, Outgoing} | More], Packets, Cmds, Events) ->
    case is_list(Outgoing) of
        true ->
            %% Outgoing is a list in least-to-most recent order (i.e. not reversed).
            %% Prepending will keep the overall order correct.
            NPackets = Outgoing ++ Packets;
        false ->
            NPackets = [Outgoing | Packets]
    end,
    classify(More, NPackets, Cmds, Events);
classify([{connack, Packet} | More], Packets, Cmds, Events) ->
    classify(More, [Packet | Packets], Cmds, Events);
classify([Packet = #mqtt_packet{} | More], Packets, Cmds, Events) ->
    classify(More, [Packet | Packets], Cmds, Events);
classify([Cmd = {active, _} | More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd | Cmds], Events);
classify([Cmd = {shutdown, _Reason} | More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd | Cmds], Events);
classify([Cmd = close | More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd | Cmds], Events);
%% cowboy_websocket's close reason must be an atom to avoid crashing the sender process.
%% The cause reasons come from parse_frame_error.
classify([{close, #{cause := Cause}} | More], Packets, Cmds, Events) when is_atom(Cause) ->
    classify(More, Packets, [{close, Cause} | Cmds], Events);
classify([Cmd = {close, _Reason} | More], Packets, Cmds, Events) ->
    classify(More, Packets, [Cmd | Cmds], Events);
classify([Event | More], Packets, Cmds, Events) ->
    classify(More, Packets, Cmds, [Event | Events]).

trigger(Event) -> erlang:send(self(), Event).

get_peer(Req, #{listener := {Type, Listener}}) ->
    {PeerAddr, PeerPort} = cowboy_req:peer(Req),
    AddrHeaderName = get_ws_header_opts(Type, Listener, proxy_address_header),
    AddrHeader = cowboy_req:header(AddrHeaderName, Req, <<>>),
    ClientAddr =
        case string:tokens(binary_to_list(AddrHeader), ", ") of
            [] ->
                undefined;
            AddrList ->
                hd(AddrList)
        end,
    Addr =
        case inet:parse_address(ClientAddr) of
            {ok, A} ->
                A;
            _ ->
                PeerAddr
        end,
    PortHeaderName = get_ws_header_opts(Type, Listener, proxy_port_header),
    PortHeader = cowboy_req:header(PortHeaderName, Req, <<>>),
    ClientPort =
        case string:tokens(binary_to_list(PortHeader), ", ") of
            [] ->
                undefined;
            PortList ->
                hd(PortList)
        end,
    try
        {Addr, list_to_integer(ClientPort)}
    catch
        _:_ -> {Addr, PeerPort}
    end.

check_max_connection(Type, Listener) ->
    case emqx_config:get_listener_conf(Type, Listener, [max_connections]) of
        infinity ->
            allow;
        Max ->
            MatchSpec = [{#chan_conn{mod = emqx_ws_connection, _ = '_'}, [], [true]}],
            Curr = ets:select_count(?CHAN_CONN_TAB, MatchSpec),
            case Curr >= Max of
                false ->
                    allow;
                true ->
                    Reason = #{
                        max => Max,
                        current => Curr,
                        msg => "websocket_max_connections_limited"
                    },
                    ?SLOG(warning, Reason),
                    {denny, Reason}
            end
    end.

enrich_state(#{proto_ver := ProtoVer, parse_state := NParseState}, State) ->
    Serialize = emqx_frame:serialize_opts(ProtoVer, ?MAX_PACKET_SIZE),
    State#state{parse_state = NParseState, serialize = Serialize};
enrich_state(_, State) ->
    State.

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, State) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, state)),
    setelement(Pos + 1, State, Value).

%% ensure lowercase letters in headers
get_ws_header_opts(Type, Listener, Key) ->
    iolist_to_binary(string:lowercase(get_ws_opts(Type, Listener, Key))).

get_ws_opts(Type, Listener, Key) ->
    emqx_config:get_listener_conf(Type, Listener, [websocket, Key]).

get_active_n(Type, Listener) ->
    emqx_config:get_listener_conf(Type, Listener, [tcp_options, active_n]).
