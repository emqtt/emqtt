%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ws_connection_SUITE).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(
    emqx_ws_connection,
    [
        websocket_handle/2,
        websocket_info/2,
        websocket_close/2
    ]
).

-define(ws_conn, emqx_ws_connection).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(TestCase, Config) when
    TestCase =/= t_ws_sub_protocols_mqtt_equivalents,
    TestCase =/= t_ws_sub_protocols_mqtt,
    TestCase =/= t_ws_check_origin,
    TestCase =/= t_ws_pingreq_before_connected,
    TestCase =/= t_ws_non_check_origin
->
    %% Meck Cm
    ok = meck:new(emqx_cm, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_cm, mark_channel_connected, fun(_) -> ok end),
    ok = meck:expect(emqx_cm, mark_channel_disconnected, fun(_) -> ok end),
    %% Mock cowboy_req
    ok = meck:new(cowboy_req, [passthrough, no_history, no_link]),
    ok = meck:expect(cowboy_req, header, fun(_, _, _) -> <<>> end),
    ok = meck:expect(cowboy_req, peer, fun(_) -> {{127, 0, 0, 1}, 3456} end),
    ok = meck:expect(cowboy_req, sock, fun(_) -> {{127, 0, 0, 1}, 18083} end),
    ok = meck:expect(cowboy_req, cert, fun(_) -> undefined end),
    ok = meck:expect(cowboy_req, parse_cookies, fun(_) -> error(badarg) end),
    Config;
init_per_testcase(t_ws_non_check_origin, Config) ->
    emqx_config:put_listener_conf(ws, default, [websocket, check_origin_enable], false),
    emqx_config:put_listener_conf(ws, default, [websocket, check_origins], []),
    Config;
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, _Config) when
    TestCase =/= t_ws_sub_protocols_mqtt_equivalents,
    TestCase =/= t_ws_sub_protocols_mqtt,
    TestCase =/= t_ws_check_origin,
    TestCase =/= t_ws_non_check_origin,
    TestCase =/= t_ws_pingreq_before_connected
->
    meck:unload([
        emqx_cm,
        cowboy_req
    ]),
    ok;
end_per_testcase(_, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_info(_) ->
    WsPid = spawn(fun() ->
        receive
            {call, From, info} ->
                gen_server:reply(From, ?ws_conn:info(st()))
        end
    end),
    #{sockinfo := SockInfo} = ?ws_conn:call(WsPid, info),
    #{
        socktype := ws,
        peername := {{127, 0, 0, 1}, 3456},
        sockname := {{127, 0, 0, 1}, 18083},
        sockstate := running
    } = SockInfo.

set_ws_opts(Key, Val) ->
    emqx_config:put_listener_conf(ws, default, [websocket, Key], Val).

t_header(_) ->
    ok = meck:expect(
        cowboy_req,
        header,
        fun
            (<<"x-forwarded-for">>, _, _) -> <<"100.100.100.100, 99.99.99.99">>;
            (<<"x-forwarded-port">>, _, _) -> <<"1000">>
        end
    ),
    set_ws_opts(proxy_address_header, <<"x-forwarded-for">>),
    set_ws_opts(proxy_port_header, <<"x-forwarded-port">>),
    {ok, St, _} = ?ws_conn:websocket_init([
        #{},
        #{
            zone => default,
            limiter => undefined,
            listener => {ws, default}
        }
    ]),
    WsPid = spawn(fun() ->
        receive
            {call, From, info} ->
                gen_server:reply(From, ?ws_conn:info(St))
        end
    end),
    #{sockinfo := SockInfo} = ?ws_conn:call(WsPid, info),
    #{
        socktype := ws,
        peername := {{100, 100, 100, 100}, 1000},
        sockstate := running
    } = SockInfo.

t_info_channel(_) ->
    #{conn_state := connected} = ?ws_conn:info(channel, st()).

t_info_gc_state(_) ->
    GcSt = emqx_gc:init(#{count => 10, bytes => 1000}),
    GcInfo = ?ws_conn:info(gc_state, st(#{gc_state => GcSt})),
    ?assertEqual(#{cnt => {10, 10}, oct => {1000, 1000}}, GcInfo).

t_info_postponed(_) ->
    ?assertEqual([], ?ws_conn:info(postponed, st())),
    St = ?ws_conn:postpone({active, false}, st()),
    ?assertEqual([{active, false}], ?ws_conn:info(postponed, St)).

t_stats(_) ->
    WsPid = spawn(fun() ->
        receive
            {call, From, stats} ->
                gen_server:reply(From, ?ws_conn:stats(st()))
        end
    end),
    Stats = ?ws_conn:call(WsPid, stats),
    [
        ?assert(lists:member(V, Stats))
     || V <-
            [
                {recv_oct, 0},
                {recv_cnt, 0},
                {send_oct, 0},
                {send_cnt, 0},
                {recv_pkt, 0},
                {recv_msg, 0},
                {send_pkt, 0},
                {send_msg, 0}
            ]
    ].

t_call(_) ->
    Info = ?ws_conn:info(st()),
    WsPid = spawn(fun() ->
        receive
            {call, From, info} -> gen_server:reply(From, Info)
        end
    end),
    ?assertEqual(Info, ?ws_conn:call(WsPid, info)).

t_ws_pingreq_before_connected(_) ->
    {ok, _} = application:ensure_all_started(gun),
    {ok, WPID} = gun:open("127.0.0.1", 8083),
    ws_pingreq(#{}),
    gun:close(WPID).

ws_pingreq(State) ->
    receive
        {gun_up, WPID, _Proto} ->
            StreamRef = gun:ws_upgrade(WPID, "/mqtt", [], #{
                protocols => [{<<"mqtt">>, gun_ws_h}]
            }),
            ws_pingreq(State#{wref => StreamRef});
        {gun_down, _WPID, _, Reason, _} ->
            State#{result => {gun_down, Reason}};
        {gun_upgrade, WPID, Ref, _Proto, _Data} ->
            ct:pal("-- gun_upgrade, send ping-req"),
            PingReq = {binary, <<192, 0>>},
            ok = gun:ws_send(WPID, Ref, PingReq),
            gun:flush(WPID),
            ws_pingreq(State);
        {gun_ws, _WPID, _Ref, {binary, <<208, 0>>}} ->
            ct:fail(unexpected_pingresp);
        {gun_ws, _WPID, _Ref, Frame} ->
            ct:pal("gun received frame: ~p", [Frame]),
            ws_pingreq(State);
        Message ->
            ct:pal("Received Unknown Message on Gun: ~p~n", [Message]),
            ws_pingreq(State)
    after 1000 ->
        ct:fail(ws_timeout)
    end.

t_ws_sub_protocols_mqtt(_) ->
    {ok, _} = application:ensure_all_started(gun),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{protocols => [<<"mqtt">>]})
    ).

t_ws_sub_protocols_mqtt_equivalents(_) ->
    {ok, _} = application:ensure_all_started(gun),
    %% also support mqtt-v3, mqtt-v3.1.1, mqtt-v5
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{protocols => [<<"mqtt-v3">>]})
    ),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{protocols => [<<"mqtt-v3.1.1">>]})
    ),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{protocols => [<<"mqtt-v5">>]})
    ),
    ?assertMatch(
        {gun_response, {_, 400, _}},
        start_ws_client(#{protocols => [<<"not-mqtt">>]})
    ).

t_ws_check_origin(_) ->
    emqx_config:put_listener_conf(ws, default, [websocket, check_origin_enable], true),
    emqx_config:put_listener_conf(
        ws,
        default,
        [websocket, check_origins],
        [<<"http://localhost:18083">>]
    ),
    {ok, _} = application:ensure_all_started(gun),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{
            protocols => [<<"mqtt">>],
            headers => [{<<"origin">>, <<"http://localhost:18083">>}]
        })
    ),
    ?assertMatch(
        {gun_response, {_, 403, _}},
        start_ws_client(#{
            protocols => [<<"mqtt">>],
            headers => [{<<"origin">>, <<"http://localhost:18080">>}]
        })
    ).

t_ws_non_check_origin(_) ->
    {ok, _} = application:ensure_all_started(gun),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{
            protocols => [<<"mqtt">>],
            headers => [{<<"origin">>, <<"http://localhost:18083">>}]
        })
    ),
    ?assertMatch(
        {gun_upgrade, _},
        start_ws_client(#{
            protocols => [<<"mqtt">>],
            headers => [{<<"origin">>, <<"http://localhost:18080">>}]
        })
    ).

t_init(_) ->
    Opts = #{listener => {ws, default}, zone => default, limiter => undefined},
    ok = meck:expect(cowboy_req, parse_header, fun(_, req) -> undefined end),
    ok = meck:expect(cowboy_req, reply, fun(_, Req) -> Req end),
    {ok, req, _} = ?ws_conn:init(req, Opts),
    ok = meck:expect(cowboy_req, parse_header, fun(_, req) -> [<<"mqtt">>] end),
    ok = meck:expect(cowboy_req, set_resp_header, fun(_, <<"mqtt">>, req) -> resp end),
    {cowboy_websocket, resp, [req, Opts], _} = ?ws_conn:init(req, Opts).

t_websocket_handle_binary(_) ->
    {ok, _} = websocket_handle({binary, <<>>}, st()),
    {ok, _} = websocket_handle({binary, [<<>>]}, st()),
    {ok, _} = websocket_handle({binary, <<192, 0>>}, st()),
    receive
        {incoming, ?PACKET(?PINGREQ)} -> ok
    after 0 -> error(expect_incoming_pingreq)
    end.

t_websocket_handle_ping(_) ->
    {ok, St} = websocket_handle(ping, St = st()),
    {ok, St} = websocket_handle({ping, <<>>}, St).

t_websocket_handle_pong(_) ->
    {ok, St} = websocket_handle(pong, St = st()),
    {ok, St} = websocket_handle({pong, <<>>}, St).

t_websocket_handle_bad_frame(_) ->
    {[{shutdown, unexpected_ws_frame}], _St} = websocket_handle({badframe, <<>>}, st()).

t_websocket_info_call(_) ->
    From = {make_ref(), self()},
    Call = {call, From, badreq},
    {ok, _St} = websocket_info(Call, st()).

t_websocket_info_rate_limit(_) ->
    {ok, _} = websocket_info({cast, rate_limit}, st()),
    ok = timer:sleep(1),
    receive
        {check_gc, Cnt, Oct} ->
            ?assertEqual(0, Cnt),
            ?assertEqual(0, Oct)
    after 0 -> error(expect_check_gc)
    end.

t_websocket_info_cast(_) ->
    {ok, _St} = websocket_info({cast, msg}, st()).

t_websocket_info_incoming(_) ->
    ok = emqx_broker:subscribe(<<"#">>, <<?MODULE_STRING>>),
    ConnPkt = #mqtt_packet_connect{
        proto_name = <<"MQTT">>,
        proto_ver = ?MQTT_PROTO_V5,
        is_bridge = false,
        clean_start = true,
        keepalive = 60,
        properties = undefined,
        clientid = <<"clientid">>,
        username = <<"username">>,
        password = <<"passwd">>
    },
    {[{binary, IoData1}, {close, protocol_error}], St1} =
        websocket_info({incoming, ?CONNECT_PACKET(ConnPkt)}, st()),
    ?assertEqual(<<224, 2, ?RC_PROTOCOL_ERROR, 0>>, iolist_to_binary(IoData1)),
    %% PINGREQ
    {[{binary, IoData2}], St2} =
        websocket_info({incoming, ?PACKET(?PINGREQ)}, St1),
    ?assertEqual(<<208, 0>>, iolist_to_binary(IoData2)),
    %% PUBLISH with property
    Publish = ?PUBLISH_PACKET(
        ?QOS_1, <<"t">>, 1, #{'User-Property' => [{<<"k">>, <<"v">>}]}, <<"payload">>
    ),
    {[{binary, IoData3}], St3} = websocket_info({incoming, Publish}, St2),
    ?assertEqual(<<64, 4, 0, 1, 0, 0>>, iolist_to_binary(IoData3)),
    %% frame_error
    Cause = invalid_property_code,
    FrameError = {frame_error, #{cause => Cause, property_code => 16#2B}},
    %% cowboy_websocket's close reason must be an atom to avoid crashing the sender process.
    %% ensure the cause is atom
    {[{binary, IoData4}, {close, CauseReq}], _St4} =
        websocket_info({incoming, FrameError}, St3),
    ?assertEqual(<<224, 2, ?RC_MALFORMED_PACKET, 0>>, iolist_to_binary(IoData4)),
    ?assertEqual(Cause, CauseReq).

t_websocket_info_check_gc(_) ->
    Stats = #{cnt => 10, oct => 1000},
    {ok, _St} = websocket_info({check_gc, Stats}, st()).

t_websocket_info_deliver(_) ->
    Msg0 = emqx_message:make(clientid, ?QOS_0, <<"t">>, <<"">>),
    Msg1 = emqx_message:make(clientid, ?QOS_1, <<"t">>, <<"">>),
    self() ! {deliver, <<"#">>, Msg1},
    {[{binary, _Pub1}, {binary, _Pub2}], _St} =
        websocket_info({deliver, <<"#">>, Msg0}, st()).

t_websocket_info_timeout_keepalive(_) ->
    {ok, _St} = websocket_info({timeout, make_ref(), keepalive}, st()).

t_websocket_info_timeout_emit_stats(_) ->
    Ref = make_ref(),
    St = st(#{stats_timer => Ref}),
    {ok, St1} = websocket_info({timeout, Ref, emit_stats}, St),
    ?assertEqual(undefined, ?ws_conn:info(stats_timer, St1)).

t_websocket_info_timeout_retry(_) ->
    {ok, _St} = websocket_info({timeout, make_ref(), retry_delivery}, st()).

t_websocket_info_close(_) ->
    {[{close, _}], _St} = websocket_info({close, sock_error}, st()).

t_websocket_info_shutdown(_) ->
    {[{shutdown, reason}], _St} = websocket_info({shutdown, reason}, st()).

t_websocket_info_stop(_) ->
    {[{shutdown, normal}], _St} = websocket_info({stop, normal}, st()).

t_websocket_close(_) ->
    {[{shutdown, badframe}], _St} = websocket_close(badframe, st()).

t_handle_info_connack(_) ->
    ConnAck = ?CONNACK_PACKET(?RC_SUCCESS),
    {[{binary, IoData}], _St} =
        ?ws_conn:handle_info({connack, ConnAck}, st()),
    ?assertEqual(<<32, 2, 0, 0>>, iolist_to_binary(IoData)).

t_handle_info_close(_) ->
    {[{close, _}], _St} = ?ws_conn:handle_info({close, protocol_error}, st()).

t_handle_info_event(_) ->
    ok = meck:expect(emqx_cm, register_channel, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_cm, insert_channel_info, fun(_, _, _) -> ok end),
    {ok, _} = ?ws_conn:handle_info({event, connected}, st()),
    {ok, _} = ?ws_conn:handle_info({event, disconnected}, st()),
    {ok, _} = ?ws_conn:handle_info({event, updated}, st()).

t_handle_timeout_idle_timeout(_) ->
    TRef = make_ref(),
    St = st(#{idle_timer => TRef}),
    {[{shutdown, idle_timeout}], _St} = ?ws_conn:handle_timeout(TRef, idle_timeout, St).

t_handle_timeout_keepalive(_) ->
    {ok, _St} = ?ws_conn:handle_timeout(make_ref(), keepalive, st()).

t_handle_timeout_emit_stats(_) ->
    TRef = make_ref(),
    {ok, St} = ?ws_conn:handle_timeout(
        TRef, emit_stats, st(#{stats_timer => TRef})
    ),
    ?assertEqual(undefined, ?ws_conn:info(stats_timer, St)).

t_parse_incoming(_) ->
    {Packets, St} = ?ws_conn:parse_incoming(<<48, 3>>, [], st()),
    {Packets1, _} = ?ws_conn:parse_incoming(<<0, 1, 116>>, Packets, St),
    Packet = ?PUBLISH_PACKET(?QOS_0, <<"t">>, undefined, <<>>),
    ?assertMatch([{incoming, Packet}], Packets1).

t_parse_incoming_order(_) ->
    Packet1 = ?PUBLISH_PACKET(?QOS_0, <<"t1">>, undefined, <<>>),
    Packet2 = ?PUBLISH_PACKET(?QOS_0, <<"t2">>, undefined, <<>>),
    Bin1 = emqx_frame:serialize(Packet1),
    Bin2 = emqx_frame:serialize(Packet2),
    {Packets1, _} = ?ws_conn:parse_incoming(erlang:iolist_to_binary([Bin1, Bin2]), [], st()),
    ?assertMatch([{incoming, Packet1}, {incoming, Packet2}], Packets1).

t_parse_incoming_frame_error(_) ->
    {Packets, _St} = ?ws_conn:parse_incoming(<<3, 2, 1, 0>>, [], st()),

    ?assertMatch(
        [
            {incoming,
                {frame_error, #{
                    header_type := _,
                    cause := malformed_packet
                }}}
        ],
        Packets
    ).

t_handle_incomming_frame_error(_) ->
    FrameError = {frame_error, bad_qos},
    Serialize = #{version => 5, max_size => 16#FFFF, strict_mode => false},
    {[{binary, IoData}, {close, bad_qos}], _St} =
        ?ws_conn:handle_incoming(FrameError, st(#{serialize => Serialize})),
    ?assertEqual(<<224, 2, 129, 0>>, iolist_to_binary(IoData)).

t_handle_outgoing(_) ->
    Packets = [
        ?PUBLISH_PACKET(?QOS_1, <<"t1">>, 1, <<"payload">>),
        ?PUBLISH_PACKET(?QOS_2, <<"t2">>, 2, <<"payload">>)
    ],
    {[{binary, IoData1}, {binary, IoData2}], _St} = ?ws_conn:handle_outgoing(Packets, st()),
    ?assert(is_binary(iolist_to_binary(IoData1))),
    ?assert(is_binary(iolist_to_binary(IoData2))).

t_run_gc(_) ->
    GcSt = emqx_gc:init(#{count => 10, bytes => 100}),
    WsSt = st(#{gc_state => GcSt}),
    ?ws_conn:run_gc(100, 10000, WsSt).

t_enqueue(_) ->
    Packet = ?PUBLISH_PACKET(?QOS_0),
    St = ?ws_conn:enqueue(Packet, st()),
    [Packet] = ?ws_conn:info(postponed, St).

t_shutdown(_) ->
    {[{shutdown, closed}], _St} = ?ws_conn:shutdown(closed, st()).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

st() -> st(#{}).
st(InitFields) when is_map(InitFields) ->
    {ok, St, _} = ?ws_conn:websocket_init([
        #{},
        #{
            zone => default,
            listener => {ws, default},
            limiter => undefined
        }
    ]),
    maps:fold(
        fun(N, V, S) -> ?ws_conn:set_field(N, V, S) end,
        ?ws_conn:set_field(channel, channel(), St),
        InitFields
    ).

channel() -> channel(#{}).
channel(InitFields) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 3456},
        sockname => {{127, 0, 0, 1}, 18083},
        conn_mod => emqx_ws_connection,
        proto_name => <<"MQTT">>,
        proto_ver => ?MQTT_PROTO_V5,
        clean_start => true,
        keepalive => 30,
        clientid => <<"clientid">>,
        username => <<"username">>,
        receive_maximum => 100,
        expiry_interval => 0
    },
    ClientInfo = #{
        zone => default,
        listener => {ws, default},
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        clientid => <<"clientid">>,
        username => <<"username">>,
        is_superuser => false,
        mountpoint => undefined
    },
    Session = emqx_session:create(
        ClientInfo,
        #{receive_maximum => 0, expiry_interval => 0},
        _WillMsg = undefined
    ),
    maps:fold(
        fun(Field, Value, Channel) ->
            emqx_channel:set_field(Field, Value, Channel)
        end,
        emqx_channel:init(ConnInfo, #{
            zone => default,
            listener => {ws, default},
            limiter => undefined
        }),
        maps:merge(
            #{
                clientinfo => ClientInfo,
                session => Session,
                conn_state => connected
            },
            InitFields
        )
    ).

start_ws_client(State) ->
    Host = maps:get(host, State, "127.0.0.1"),
    Port = maps:get(port, State, 8083),
    {ok, WPID} = gun:open(Host, Port),
    #{result := Result} = ws_client(State#{wpid => WPID}),
    gun:close(WPID),
    Result.

ws_client(State) ->
    receive
        {gun_up, WPID, _Proto} ->
            #{protocols := Protos} = State,
            StreamRef = gun:ws_upgrade(
                WPID,
                "/mqtt",
                maps:get(headers, State, []),
                #{protocols => [{P, gun_ws_h} || P <- Protos]}
            ),
            ws_client(State#{wref => StreamRef});
        {gun_down, _WPID, _, Reason, _} ->
            State#{result => {gun_down, Reason}};
        {gun_upgrade, _WPID, _Ref, _Proto, Data} ->
            ct:pal("-- gun_upgrade: ~p", [Data]),
            State#{result => {gun_upgrade, Data}};
        {gun_response, _WPID, _Ref, _Code, _HttpStatusCode, _Headers} ->
            Rsp = {_Code, _HttpStatusCode, _Headers},
            ct:pal("-- gun_response: ~p", [Rsp]),
            State#{result => {gun_response, Rsp}};
        {gun_error, _WPID, _Ref, _Reason} ->
            State#{result => {gun_error, _Reason}};
        {'DOWN', _PID, process, _WPID, _Reason} ->
            State#{result => {down, _Reason}};
        {gun_ws, _WPID, Frame} ->
            case Frame of
                close ->
                    self() ! stop;
                {close, _Code, _Message} ->
                    self() ! stop;
                {text, TextData} ->
                    io:format("Received Text Frame: ~p~n", [TextData]);
                {binary, BinData} ->
                    io:format("Received Binary Frame: ~p~n", [BinData]);
                _ ->
                    io:format("Received Unhandled Frame: ~p~n", [Frame])
            end,
            ws_client(State);
        stop ->
            #{wpid := WPID} = State,
            gun:flush(WPID),
            gun:shutdown(WPID),
            State#{result => {stop, normal}};
        Message ->
            ct:pal("Received Unknown Message on Gun: ~p~n", [Message]),
            ws_client(State)
    after 5000 ->
        ct:fail(ws_timeout)
    end.
