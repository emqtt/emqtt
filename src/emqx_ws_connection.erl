%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT WebSocket Connection
-module(emqx_ws_connection).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[WsConnection]").

-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% WebSocket callbacks
-export([ init/2
        , websocket_init/1
        , websocket_handle/2
        , websocket_info/2
        , terminate/3
        ]).

-record(state, {
          %% Peername of the ws connection.
          peername :: emqx_types:peername(),
          %% Sockname of the ws connection
          sockname :: emqx_types:peername(),
          %% Conn state
          conn_state :: idle | connected | disconnected,
          %% Parser State
          parse_state :: emqx_frame:parse_state(),
          %% Serialize function
          serialize :: emqx_frame:serialize_fun(),
          %% Channel State
          chan_state :: emqx_channel:channel(),
          %% Out Pending Packets
          pendings :: list(emqx_types:packet()),
          %% The stop reason
          stop_reason :: term()
        }).

-type(state() :: #state{}).

-define(INFO_KEYS, [socktype, peername, sockname, active_state]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(info(pid()|state()) -> emqx_types:infos()).
info(WsPid) when is_pid(WsPid) ->
    call(WsPid, info);
info(WsConn = #state{chan_state = ChanState}) ->
    ChanInfo = emqx_channel:info(ChanState),
    SockInfo = maps:from_list(info(?INFO_KEYS, WsConn)),
    maps:merge(ChanInfo, #{sockinfo => SockInfo}).

info(Keys, WsConn) when is_list(Keys) ->
    [{Key, info(Key, WsConn)} || Key <- Keys];
info(socktype, _State) ->
    ws;
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(active_state, _State) ->
    running;
info(chan_state, #state{chan_state = ChanState}) ->
    emqx_channel:info(ChanState).

-spec(stats(pid()|state()) -> emqx_types:stats()).
stats(WsPid) when is_pid(WsPid) ->
    call(WsPid, stats);
stats(#state{chan_state = ChanState}) ->
    SockStats = emqx_pd:get_counters(?SOCK_STATS),
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_channel:stats(ChanState),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

%% kick|discard|takeover
-spec(call(pid(), Req :: term()) -> Reply :: term()).
call(WsPid, Req) when is_pid(WsPid) ->
    Mref = erlang:monitor(process, WsPid),
    WsPid ! {call, {self(), Mref}, Req},
    receive
        {Mref, Reply} ->
            erlang:demonitor(Mref, [flush]),
            Reply;
        {'DOWN', Mref, _, _, Reason} ->
            exit(Reason)
    after 5000 ->
        erlang:demonitor(Mref, [flush]),
        exit(timeout)
    end.

%%--------------------------------------------------------------------
%% WebSocket callbacks
%%--------------------------------------------------------------------

init(Req, Opts) ->
    IdleTimeout = proplists:get_value(idle_timeout, Opts, 7200000),
    DeflateOptions = maps:from_list(proplists:get_value(deflate_options, Opts, [])),
    MaxFrameSize = case proplists:get_value(max_frame_size, Opts, 0) of
                       0 -> infinity;
                       I -> I
                   end,
    Compress = proplists:get_value(compress, Opts, false),
    WsOpts = #{compress       => Compress,
               deflate_opts   => DeflateOptions,
               max_frame_size => MaxFrameSize,
               idle_timeout   => IdleTimeout
              },
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            %% TODO: why not reply 500???
            {cowboy_websocket, Req, [Req, Opts], WsOpts};
        [<<"mqtt", Vsn/binary>>] ->
            Resp = cowboy_req:set_resp_header(
                     <<"sec-websocket-protocol">>, <<"mqtt", Vsn/binary>>, Req),
            {cowboy_websocket, Resp, [Req, Opts], WsOpts};
        _ ->
            {ok, cowboy_req:reply(400, Req), WsOpts}
    end.

websocket_init([Req, Opts]) ->
    Peername = cowboy_req:peer(Req),
    Sockname = cowboy_req:sock(Req),
    Peercert = cowboy_req:cert(Req),
    WsCookie = try cowboy_req:parse_cookies(Req)
               catch
                   error:badarg ->
                       ?LOG(error, "Illegal cookie"),
                       undefined;
                   Error:Reason ->
                       ?LOG(error, "Failed to parse cookie, Error: ~p, Reason ~p",
                            [Error, Reason]),
                       undefined
               end,
    ConnInfo = #{socktype  => ws,
                 peername  => Peername,
                 sockname  => Sockname,
                 peercert  => Peercert,
                 ws_cookie => WsCookie,
                 conn_mod  => ?MODULE
                },
    Zone = proplists:get_value(zone, Opts),
    FrameOpts = emqx_zone:frame_options(Zone),
    ParseState = emqx_frame:initial_parse_state(FrameOpts),
    Serialize = emqx_frame:serialize_fun(),
    ChanState = emqx_channel:init(ConnInfo, Opts),
    emqx_logger:set_metadata_peername(esockd_net:format(Peername)),
    {ok, #state{peername    = Peername,
                sockname    = Sockname,
                conn_state  = idle,
                parse_state = ParseState,
                serialize   = Serialize,
                chan_state  = ChanState,
                pendings    = []
               }}.

websocket_handle({binary, Data}, State) when is_list(Data) ->
    websocket_handle({binary, iolist_to_binary(Data)}, State);

websocket_handle({binary, Data}, State = #state{chan_state = ChanState}) ->
    ?LOG(debug, "RECV ~p", [Data]),
    Oct = iolist_size(Data),
    ok = inc_recv_stats(1, Oct),
    NChanState = emqx_channel:received(Oct, ChanState),
    NState = State#state{chan_state = NChanState},
    process_incoming(Data, NState);

%% Pings should be replied with pongs, cowboy does it automatically
%% Pongs can be safely ignored. Clause here simply prevents crash.
websocket_handle(Frame, State) when Frame =:= ping; Frame =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State) when FrameType =:= ping;
                                             FrameType =:= pong ->
    {ok, State};

websocket_handle({FrameType, _}, State) ->
    ?LOG(error, "Unexpected frame - ~p", [FrameType]),
    stop({shutdown, unexpected_ws_frame}, State).

websocket_info({call, From, info}, State) ->
    gen_server:reply(From, info(State)),
    {ok, State};

websocket_info({call, From, stats}, State) ->
    gen_server:reply(From, stats(State)),
    {ok, State};

websocket_info({call, From, state}, State) ->
    gen_server:reply(From, State),
    {ok, State};

websocket_info({call, From, Req}, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_call(Req, ChanState) of
        {ok, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, Reply, NChanState} ->
            _ = gen_server:reply(From, Reply),
            stop(Reason, State#state{chan_state = NChanState})
    end;

websocket_info({cast, Msg}, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Msg, ChanState) of
        ok -> {ok, State};
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end;

websocket_info({incoming, Packet = ?CONNECT_PACKET(ConnPkt)}, State) ->
    NState = State#state{serialize = emqx_frame:serialize_fun(ConnPkt)},
    handle_incoming(Packet, fun connected/1, NState);

websocket_info({incoming, Packet}, State) when is_record(Packet, mqtt_packet) ->
    handle_incoming(Packet, fun reply/1, State);

websocket_info({incoming, FrameError = {frame_error, _Reason}}, State) ->
    handle_incoming(FrameError, State);

websocket_info(Deliver = {deliver, _Topic, _Msg},
               State = #state{chan_state = ChanState}) ->
    Delivers = emqx_misc:drain_deliver([Deliver]),
    case emqx_channel:handle_out({deliver, Delivers}, ChanState) of
        {ok, NChanState} ->
            reply(State#state{chan_state = NChanState});
        {ok, Packets, NChanState} ->
            reply(enqueue(Packets, State#state{chan_state = NChanState}))
    end;

websocket_info({timeout, TRef, keepalive}, State) when is_reference(TRef) ->
    RecvOct = emqx_pd:get_counter(recv_oct),
    handle_timeout(TRef, {keepalive, RecvOct}, State);

websocket_info({timeout, TRef, emit_stats}, State) when is_reference(TRef) ->
    handle_timeout(TRef, {emit_stats, stats(State)}, State);

websocket_info({timeout, TRef, Msg}, State) when is_reference(TRef) ->
    handle_timeout(TRef, Msg, State);

websocket_info({shutdown, Reason}, State) ->
    stop({shutdown, Reason}, State);

websocket_info({stop, Reason}, State) ->
    stop(Reason, State);

websocket_info(Info, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_info(Info, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end.

terminate(SockError, _Req, #state{chan_state  = ChanState,
                                  stop_reason = Reason}) ->
    ?LOG(debug, "Terminated for ~p, sockerror: ~p", [Reason, SockError]),
    emqx_channel:terminate(Reason, ChanState);

terminate(_Reason, _Req, _State) ->
    ok.
%%--------------------------------------------------------------------
%% Connected callback

connected(State = #state{chan_state = ChanState}) ->
    ChanAttrs = emqx_channel:attrs(ChanState),
    SockAttrs = #{active_state => running},
    Attrs = maps:merge(ChanAttrs, #{sockinfo => SockAttrs}),
    ok = emqx_channel:handle_info({register, Attrs, stats(State)}, ChanState),
    reply(State#state{conn_state = connected}).

%%--------------------------------------------------------------------
%% Close

close(Reason, State) ->
    ?LOG(warning, "Closed for ~p", [Reason]),
    reply(State#state{conn_state = disconnected}).

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(TRef, Msg, State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_timeout(TRef, Msg, ChanState) of
        {ok, NChanState} ->
            {ok, State#state{chan_state = NChanState}};
        {ok, Packets, NChanState} ->
            NState = State#state{chan_state = NChanState},
            reply(enqueue(Packets, NState));
        {close, Reason, NChanState} ->
            close(Reason, State#state{chan_state = NChanState});
        {close, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            close(Reason, enqueue(OutPackets, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState})
    end.

%%--------------------------------------------------------------------
%% Process incoming data

process_incoming(<<>>, State) ->
    {ok, State};

process_incoming(Data, State = #state{parse_state = ParseState}) ->
    try emqx_frame:parse(Data, ParseState) of
        {more, NParseState} ->
            {ok, State#state{parse_state = NParseState}};
        {ok, Packet, Rest, NParseState} ->
            self() ! {incoming, Packet},
            process_incoming(Rest, State#state{parse_state = NParseState})
    catch
        error:Reason:Stk ->
            ?LOG(error, "~nParse failed for ~p~nStacktrace: ~p~nFrame data: ~p",
                 [Reason, Stk, Data]),
            self() ! {incoming, {frame_error, Reason}},
            {ok, State}
    end.

%%--------------------------------------------------------------------
%% Handle incoming packets

handle_incoming(Packet = ?PACKET(Type), SuccFun,
                State = #state{chan_state = ChanState}) ->
    _ = inc_incoming_stats(Type),
    _ = emqx_metrics:inc_recv(Packet),
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)]),
    case emqx_channel:handle_in(Packet, ChanState) of
        {ok, NChanState} ->
            SuccFun(State#state{chan_state= NChanState});
        {ok, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            SuccFun(enqueue(OutPackets, NState));
        {close, Reason, NChanState} ->
            close(Reason, State#state{chan_state = NChanState});
        {close, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            close(Reason, enqueue(OutPackets, NState));
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state= NChanState},
            stop(Reason, enqueue(OutPackets, NState))
    end.

handle_incoming(FrameError = {frame_error, _Reason},
                State = #state{chan_state = ChanState}) ->
    case emqx_channel:handle_in(FrameError, ChanState) of
        {stop, Reason, NChanState} ->
            stop(Reason, State#state{chan_state = NChanState});
        {stop, Reason, OutPackets, NChanState} ->
            NState = State#state{chan_state = NChanState},
            stop(Reason, enqueue(OutPackets, NState))
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(Packets, State = #state{chan_state = ChanState}) ->
    IoData = lists:map(serialize_and_inc_stats_fun(State), Packets),
    Oct = iolist_size(IoData),
    ok = inc_sent_stats(length(Packets), Oct),
    NChanState = emqx_channel:sent(Oct, ChanState),
    {{binary, IoData}, State#state{chan_state = NChanState}}.

%% TODO: Duplicated with emqx_channel:serialize_and_inc_stats_fun/1
serialize_and_inc_stats_fun(#state{serialize = Serialize}) ->
    fun(Packet = ?PACKET(Type)) ->
        case Serialize(Packet) of
            <<>> -> ?LOG(warning, "~s is discarded due to the frame is too large!",
                         [emqx_packet:format(Packet)]),
                    <<>>;
            Data -> _ = inc_outgoing_stats(Type),
                    _ = emqx_metrics:inc_sent(Packet),
                    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)]),
                    Data
        end
    end.

%%--------------------------------------------------------------------
%% Inc incoming/outgoing stats

-compile({inline,
          [ inc_recv_stats/2
          , inc_incoming_stats/1
          , inc_outgoing_stats/1
          , inc_sent_stats/2
          ]}).

inc_recv_stats(Cnt, Oct) ->
    emqx_pd:update_counter(recv_cnt, Cnt),
    emqx_pd:update_counter(recv_oct, Oct),
    emqx_metrics:inc('bytes.received', Oct).

inc_incoming_stats(Type) ->
    emqx_pd:update_counter(recv_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(recv_msg, 1).

inc_outgoing_stats(Type) ->
    emqx_pd:update_counter(send_pkt, 1),
    (Type == ?PUBLISH)
        andalso emqx_pd:update_counter(send_msg, 1).

inc_sent_stats(Cnt, Oct) ->
    emqx_pd:update_counter(send_cnt, Cnt),
    emqx_pd:update_counter(send_oct, Oct),
    emqx_metrics:inc('bytes.sent', Oct).

%%--------------------------------------------------------------------
%% Reply or Stop

-compile({inline, [reply/1]}).

reply(State = #state{pendings = []}) ->
    {ok, State};
reply(State = #state{pendings = Pendings}) ->
    {Reply, NState} = handle_outgoing(Pendings, State),
    {reply, Reply, NState#state{pendings = []}}.

stop(Reason, State = #state{pendings = []}) ->
    {stop, State#state{stop_reason = Reason}};
stop(Reason, State = #state{pendings = Pendings}) ->
    {Reply, State1} = handle_outgoing(Pendings, State),
    State2 = State1#state{pendings = [], stop_reason = Reason},
    {reply, [Reply, close], State2}.

enqueue(Packet, State) when is_record(Packet, mqtt_packet) ->
    enqueue([Packet], State);
enqueue(Packets, State = #state{pendings = Pendings}) ->
    State#state{pendings = lists:append(Pendings, Packets)}.

