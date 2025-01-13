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

-module(emqx_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

init_per_suite(Config) ->
    %% Meck Transport
    ok = meck:new(emqx_transport, [non_strict, passthrough, no_history, no_link]),
    ok = meck:expect(emqx_transport, shutdown, fun(_, _) -> ok end),
    %% Meck Channel
    ok = meck:new(emqx_channel, [passthrough, no_history, no_link]),
    %% Meck Cm
    ok = meck:new(emqx_cm, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_cm, mark_channel_connected, fun(_) -> ok end),
    ok = meck:expect(emqx_cm, mark_channel_disconnected, fun(_) -> ok end),
    %% Meck Pd
    ok = meck:new(emqx_pd, [passthrough, no_history, no_link]),
    %% Meck Metrics
    ok = meck:new(emqx_metrics, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc, fun(_, _) -> ok end),
    ok = meck:expect(emqx_metrics, inc_recv, fun(_) -> ok end),
    ok = meck:expect(emqx_metrics, inc_sent, fun(_) -> ok end),
    %% Meck Hooks
    ok = meck:new(emqx_hooks, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_hooks, run, fun(_Hook, _Args) -> ok end),
    ok = meck:expect(emqx_hooks, run_fold, fun(_Hook, _Args, Acc) -> Acc end),

    ok = meck:expect(emqx_channel, ensure_disconnected, fun(_, Channel) -> Channel end),

    ok = meck:expect(emqx_alarm, activate, fun(_, _) -> ok end),
    ok = meck:expect(emqx_alarm, deactivate, fun(_) -> ok end),
    ok = meck:expect(emqx_alarm, deactivate, fun(_, _) -> ok end),

    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = meck:unload(emqx_transport),
    catch meck:unload(emqx_channel),
    ok = meck:unload(emqx_cm),
    ok = meck:unload(emqx_pd),
    ok = meck:unload(emqx_metrics),
    ok = meck:unload(emqx_hooks),
    ok = meck:unload(emqx_alarm),

    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

init_per_testcase(TestCase, Config) when
    TestCase =/= t_ws_pingreq_before_connected
->
    ok = meck:expect(emqx_transport, wait, fun(Sock) -> {ok, Sock} end),
    ok = meck:expect(emqx_transport, type, fun(_Sock) -> tcp end),
    ok = meck:expect(
        emqx_transport,
        ensure_ok_or_exit,
        fun
            (peername, [sock]) -> {ok, {{127, 0, 0, 1}, 3456}};
            (sockname, [sock]) -> {ok, {{127, 0, 0, 1}, 1883}};
            (peercert, [sock]) -> undefined;
            (peersni, [sock]) -> undefined
        end
    ),
    ok = meck:expect(emqx_transport, setopts, fun(_Sock, _Opts) -> ok end),
    ok = meck:expect(emqx_transport, getopts, fun(_Sock, Options) ->
        {ok, [{K, 0} || K <- Options]}
    end),
    ok = meck:expect(emqx_transport, getstat, fun(_Sock, Options) ->
        {ok, [{K, 0} || K <- Options]}
    end),
    ok = meck:expect(emqx_transport, send, fun(_Sock, _Data) -> ok end),
    ok = meck:expect(emqx_transport, fast_close, fun(_Sock) -> ok end),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config);
        _ -> Config
    end;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------
t_ws_pingreq_before_connected(_) ->
    ?assertMatch(
        {ok, [_, {close, protocol_error}], _},
        handle_msg({incoming, ?PACKET(?PINGREQ)}, st(#{}, #{conn_state => disconnected}))
    ).

t_info(_) ->
    CPid = spawn(fun() ->
        receive
            {'$gen_call', From, info} ->
                gen_server:reply(From, emqx_connection:info(st()))
        after 100 -> error("error")
        end
    end),
    #{sockinfo := SockInfo} = emqx_connection:info(CPid),
    ?assertMatch(
        #{
            peername := {{127, 0, 0, 1}, 3456},
            sockname := {{127, 0, 0, 1}, 1883},
            sockstate := idle,
            socktype := tcp
        },
        SockInfo
    ).

t_stats(_) ->
    CPid = spawn(fun() ->
        receive
            {'$gen_call', From, stats} ->
                gen_server:reply(From, emqx_connection:stats(st()))
        after 100 -> error("error")
        end
    end),
    Stats = emqx_connection:stats(CPid),
    ?assertMatch(
        [
            {recv_oct, 0},
            {recv_cnt, 0},
            {send_oct, 0},
            {send_cnt, 0},
            {send_pend, 0}
            | _
        ],
        Stats
    ).

t_process_msg(_) ->
    with_conn(
        fun(CPid) ->
            ok = meck:expect(
                emqx_channel,
                handle_in,
                fun(_Packet, Channel) ->
                    {ok, Channel}
                end
            ),
            CPid ! {incoming, ?PACKET(?PINGREQ)},
            CPid ! {incoming, undefined},
            CPid ! {tcp_passive, sock},
            CPid ! {tcp_closed, sock},
            timer:sleep(100),
            ok = trap_exit(CPid, {shutdown, tcp_closed})
        end,
        #{trap_exit => true}
    ).

t_ensure_stats_timer(_) ->
    NStats = emqx_connection:ensure_stats_timer(st(#{stats_timer => undefined})),
    StatsTimer = emqx_connection:info(stats_timer, NStats),
    ?assert(is_reference(StatsTimer)),
    ?assertEqual(NStats, emqx_connection:ensure_stats_timer(NStats)).

t_cancel_stats_timer(_) ->
    NStats = emqx_connection:cancel_stats_timer(st(#{stats_timer => make_ref()})),
    StatsTimer = emqx_connection:info(stats_timer, NStats),
    ?assertEqual(undefined, StatsTimer),
    ?assertEqual(NStats, emqx_connection:cancel_stats_timer(NStats)).

t_append_msg(_) ->
    ?assertEqual([msg], emqx_connection:append_msg([], [msg])),
    ?assertEqual([msg], emqx_connection:append_msg([], msg)),
    ?assertEqual([msg1, msg], emqx_connection:append_msg([msg1], [msg])),
    ?assertEqual([msg1, msg], emqx_connection:append_msg([msg1], msg)).

t_handle_msg(_) ->
    From = {make_ref(), self()},
    ?assertMatch({ok, _St}, handle_msg({'$gen_call', From, for_testing}, st())),
    ?assertMatch(
        {stop, {shutdown, discarded}, _St}, handle_msg({'$gen_call', From, discard}, st())
    ),
    ?assertMatch(
        {stop, {shutdown, discarded}, _St}, handle_msg({'$gen_call', From, discard}, st())
    ),
    ?assertMatch({ok, [], _St}, handle_msg({tcp, From, <<"for_testing">>}, st())),
    ?assertMatch({ok, _St}, handle_msg(for_testing, st())).

t_handle_msg_incoming(_) ->
    ?assertMatch(
        {ok, _Out, _St},
        handle_msg({incoming, ?CONNECT_PACKET(#mqtt_packet_connect{})}, st())
    ),
    ok = meck:expect(emqx_channel, handle_in, fun(_Packet, Channel) -> {ok, Channel} end),
    ?assertMatch(
        {ok, _St},
        handle_msg({incoming, ?PUBLISH_PACKET(?QOS_1, <<"t">>, 1, <<"payload">>)}, st())
    ),
    Sub1 = <<?SUBSCRIBE:4, 2:4, 11, 0, 2, 0, 6, 84, 111, 112, 105, 99, 65, 2>>,
    ?assertMatch({ok, _St}, handle_msg({incoming, Sub1}, st())),
    Sub2 = <<?UNSUBSCRIBE:4, 2:4, 10, 0, 2, 0, 6, 84, 111, 112, 105, 99, 65>>,
    ?assertMatch({ok, _St}, handle_msg({incoming, Sub2}, st())),
    ?assertMatch({ok, _St}, handle_msg({incoming, undefined}, st())).

t_handle_msg_outgoing(_) ->
    ?assertMatch(
        {ok, _}, handle_msg({outgoing, ?PUBLISH_PACKET(?QOS_2, <<"Topic">>, 1, <<>>)}, st())
    ),
    ?assertMatch({ok, _}, handle_msg({outgoing, ?PUBREL_PACKET(1)}, st())),
    ?assertMatch({ok, _}, handle_msg({outgoing, ?PUBCOMP_PACKET(1)}, st())).

t_handle_msg_tcp_error(_) ->
    ?assertMatch(
        {stop, {shutdown, econnreset}, _St},
        handle_msg({tcp_error, sock, econnreset}, st())
    ).

t_handle_msg_tcp_closed(_) ->
    ?assertMatch({stop, {shutdown, tcp_closed}, _St}, handle_msg({tcp_closed, sock}, st())).

t_handle_msg_passive(_) ->
    ?assertMatch({ok, _Event, _St}, handle_msg({tcp_passive, sock}, st())).

t_handle_msg_deliver(_) ->
    ok = meck:expect(emqx_channel, handle_deliver, fun(_, Channel) -> {ok, Channel} end),
    ?assertMatch({ok, _St}, handle_msg({deliver, topic, msg}, st())).

t_handle_msg_inet_reply(_) ->
    ?assertMatch(
        {stop, {shutdown, for_testing}, _St},
        handle_msg({inet_reply, for_testing, {error, for_testing}}, st())
    ).

t_handle_msg_connack(_) ->
    ?assertMatch({ok, _}, handle_msg({connack, ?CONNACK_PACKET(?CONNACK_ACCEPT)}, st())).

t_handle_msg_close(_) ->
    ?assertMatch({stop, {shutdown, normal}, _St}, handle_msg({close, normal}, st())).

t_handle_msg_event(_) ->
    ok = meck:expect(emqx_cm, register_channel, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_cm, insert_channel_info, fun(_, _, _) -> ok end),
    ok = meck:expect(emqx_cm, set_chan_info, fun(_, _) -> ok end),
    ?assertMatch({ok, _St}, handle_msg({event, connected}, st())),
    ?assertMatch({ok, _St}, handle_msg({event, disconnected}, st())),
    ?assertMatch({ok, _St}, handle_msg({event, undefined}, st())).

t_handle_msg_timeout(_) ->
    ?assertMatch({ok, _St}, handle_msg({timeout, make_ref(), for_testing}, st())).

t_handle_msg_shutdown(_) ->
    ?assertMatch({stop, {shutdown, for_testing}, _St}, handle_msg({shutdown, for_testing}, st())).

t_handle_call(_) ->
    St = st(),
    ?assertMatch({ok, _St}, handle_msg({event, undefined}, St)),
    ?assertMatch({reply, _Info, _NSt}, handle_call(self(), info, St)),
    ?assertMatch({reply, _Stats, _NSt}, handle_call(self(), stats, St)),
    ?assertEqual({reply, ignored, St}, handle_call(self(), for_testing, St)),
    ?assertMatch(
        {stop, {shutdown, kicked}, ok, _NSt},
        handle_call(self(), kick, St)
    ).

t_handle_timeout(_) ->
    TRef = make_ref(),
    State = st(#{idle_timer => TRef, stats_timer => TRef}),
    ?assertMatch(
        {stop, {shutdown, idle_timeout}, _NState},
        emqx_connection:handle_timeout(TRef, idle_timeout, State)
    ),
    ?assertMatch(
        {ok, _NState},
        emqx_connection:handle_timeout(TRef, emit_stats, State)
    ),
    ?assertMatch(
        {ok, _NState},
        emqx_connection:handle_timeout(TRef, keepalive, State)
    ),

    ?assertMatch({ok, _NState}, emqx_connection:handle_timeout(TRef, undefined, State)).

t_parse_incoming(_) ->
    ?assertMatch({[], _NState}, emqx_connection:parse_incoming(<<>>, st())),
    ?assertMatch({[], _NState}, emqx_connection:parse_incoming(<<"for_testing">>, st())).

t_next_incoming_msgs(_) ->
    ?assertEqual(
        {incoming, packet},
        emqx_connection:next_incoming_msgs([packet])
    ),
    ?assertEqual(
        [{incoming, packet2}, {incoming, packet1}],
        emqx_connection:next_incoming_msgs([packet1, packet2])
    ).

t_handle_incoming(_) ->
    ?assertMatch(
        {ok, _Out, _NState},
        emqx_connection:handle_incoming(?CONNECT_PACKET(#mqtt_packet_connect{}), st())
    ),
    ?assertMatch({ok, _Out, _NState}, emqx_connection:handle_incoming(frame_error, st())).

t_handle_outing_non_utf8_topic(_) ->
    Topic = <<"测试"/utf16>>,
    Publish = ?PUBLISH_PACKET(0, Topic, 1),
    StrictOff = #{version => 5, max_size => 16#FFFF, strict_mode => false},
    StOff = st(#{serialize => StrictOff}),
    OffResult = emqx_connection:handle_outgoing(Publish, StOff),
    ?assertMatch({ok, _}, OffResult),
    StrictOn = #{version => 5, max_size => 16#FFFF, strict_mode => true},
    StOn = st(#{serialize => StrictOn}),
    ?assertError(frame_serialize_error, emqx_connection:handle_outgoing(Publish, StOn)).

t_with_channel(_) ->
    State = st(),
    ok = meck:expect(emqx_channel, handle_in, fun(_, _) -> ok end),
    ?assertEqual({ok, State}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(emqx_channel, handle_in, fun(_, _) ->
        Channel = channel(),
        {ok, Channel}
    end),
    ?assertMatch({ok, _NState}, emqx_connection:with_channel(handle_in, [for_testing], State)),

    ok = meck:expect(
        emqx_channel,
        handle_in,
        fun(_, _) ->
            Channel = channel(),
            {ok, ?DISCONNECT_PACKET(), Channel}
        end
    ),
    ?assertMatch(
        {ok, _Out, _NChannel},
        emqx_connection:with_channel(handle_in, [for_testing], State)
    ),

    ok = meck:expect(
        emqx_channel,
        handle_in,
        fun(_, _) ->
            Channel = channel(),
            {shutdown, [for_testing], Channel}
        end
    ),
    ?assertMatch(
        {stop, {shutdown, [for_testing]}, _NState},
        emqx_connection:with_channel(handle_in, [for_testing], State)
    ),

    ok = meck:expect(
        emqx_channel,
        handle_in,
        fun(_, _) ->
            Channel = channel(),
            {shutdown, [for_testing], ?DISCONNECT_PACKET(), Channel}
        end
    ),
    ?assertMatch(
        {stop, {shutdown, [for_testing]}, _NState},
        emqx_connection:with_channel(handle_in, [for_testing], State)
    ),
    meck:unload(emqx_channel).

t_handle_outgoing(_) ->
    ?assertMatch({ok, _}, emqx_connection:handle_outgoing(?PACKET(?PINGRESP), st())),
    ?assertMatch({ok, _}, emqx_connection:handle_outgoing([?PACKET(?PINGRESP)], st())).

t_handle_info(_) ->
    ?assertMatch(
        {ok, {event, running}, _NState},
        emqx_connection:handle_info(activate_socket, st())
    ),
    ?assertMatch(
        {stop, {shutdown, for_testing}, _NStats},
        emqx_connection:handle_info({sock_error, for_testing}, st())
    ),
    ?assertMatch({ok, _NState}, emqx_connection:handle_info(for_testing, st())).

t_activate_socket(_) ->
    State = st(#{}),
    {ok, NStats} = emqx_connection:activate_socket(State),
    ?assertEqual(running, emqx_connection:info(sockstate, NStats)),

    State2 = st(#{sockstate => closed}),
    ?assertEqual({ok, State2}, emqx_connection:activate_socket(State2)).

t_close_socket(_) ->
    State = emqx_connection:close_socket(st(#{sockstate => closed})),
    ?assertEqual(closed, emqx_connection:info(sockstate, State)),
    State1 = emqx_connection:close_socket(st()),
    ?assertEqual(closed, emqx_connection:info(sockstate, State1)).

t_system_code_change(_) ->
    State = st(),
    ?assertEqual({ok, State}, emqx_connection:system_code_change(State, [], [], [])).

t_next_msgs(_) ->
    ?assertEqual({outgoing, ?CONNECT_PACKET()}, emqx_connection:next_msgs(?CONNECT_PACKET())),
    ?assertEqual({}, emqx_connection:next_msgs({})),
    ?assertEqual([], emqx_connection:next_msgs([])).

t_start_link_ok(_) ->
    with_conn(fun(CPid) -> state = element(1, sys:get_state(CPid)) end).

t_start_link_exit_on_wait(_) ->
    ok = exit_on_wait_error(enotconn, normal),
    ok = exit_on_wait_error(einval, normal),
    ok = exit_on_wait_error(closed, normal),
    ok = exit_on_wait_error(timeout, {shutdown, ssl_upgrade_timeout}),
    ok = exit_on_wait_error(enetdown, {shutdown, enetdown}).

t_start_link_exit_on_activate(_) ->
    ok = exit_on_activate_error(enotconn, normal),
    ok = exit_on_activate_error(einval, normal),
    ok = exit_on_activate_error(closed, normal),
    ok = exit_on_activate_error(econnreset, {shutdown, econnreset}).

t_get_conn_info(_) ->
    with_conn(fun(CPid) ->
        #{sockinfo := SockInfo} = emqx_connection:info(CPid),
        ?assertEqual(
            #{
                peername => {{127, 0, 0, 1}, 3456},
                sockname => {{127, 0, 0, 1}, 1883},
                sockstate => running,
                socktype => tcp
            },
            SockInfo
        )
    end).

t_oom_shutdown(init, Config) ->
    ok = snabbkaffe:stop(),
    ok = snabbkaffe:start_trace(),
    ok = meck:new(emqx_utils, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_utils,
        check_oom,
        fun(_) -> {shutdown, "fake_oom"} end
    ),
    Config;
t_oom_shutdown('end', _Config) ->
    snabbkaffe:stop(),
    meck:unload(emqx_utils),
    ok.

t_oom_shutdown(_) ->
    Opts = #{trap_exit => true},
    with_conn(
        fun(Pid) ->
            Pid ! {tcp_passive, foo},
            {ok, _} = ?block_until(#{?snk_kind := check_oom_shutdown}, 1000),
            {ok, _} = ?block_until(#{?snk_kind := terminate}, 100),
            Trace = snabbkaffe:collect_trace(),
            ?assertEqual(1, length(?of_kind(terminate, Trace))),
            receive
                {'EXIT', Pid, Reason} ->
                    ?assertEqual({shutdown, "fake_oom"}, Reason)
            after 1000 ->
                error(timeout)
            end,
            ?assertNot(erlang:is_process_alive(Pid))
        end,
        Opts
    ),
    ok.

t_cancel_congestion_alarm(_) ->
    Opts = #{trap_exit => false},
    ok = meck:expect(
        emqx_transport,
        getstat,
        fun
            (_Sock, [send_pend]) ->
                %% simulate congestion
                {ok, [{send_pend, 999}]};
            (_Sock, Options) ->
                {ok, [{K, 0} || K <- Options]}
        end
    ),
    with_conn(
        fun(Pid) ->
            #{
                channel := Channel,
                transport := Transport,
                socket := Socket
            } = emqx_connection:get_state(Pid),
            %% precondition
            Zone = emqx_channel:info(zone, Channel),
            true = emqx_config:get_zone_conf(Zone, [conn_congestion, enable_alarm]),
            %% should not raise errors
            ok = emqx_congestion:maybe_alarm_conn_congestion(Socket, Transport, Channel),
            %% should not raise errors either
            ok = emqx_congestion:cancel_alarms(Socket, Transport, Channel),
            ok
        end,
        Opts
    ),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

exit_on_wait_error(SockErr, Reason) ->
    ok = meck:expect(
        emqx_transport,
        wait,
        fun(_Sock) ->
            {error, SockErr}
        end
    ),
    with_conn(
        fun(CPid) ->
            timer:sleep(100),
            trap_exit(CPid, Reason)
        end,
        #{trap_exit => true}
    ).

exit_on_activate_error(SockErr, Reason) ->
    ok = meck:expect(
        emqx_transport,
        setopts,
        fun(_Sock, _Opts) ->
            {error, SockErr}
        end
    ),
    with_conn(
        fun(CPid) ->
            timer:sleep(100),
            trap_exit(CPid, Reason)
        end,
        #{trap_exit => true}
    ).

with_conn(TestFun) ->
    with_conn(TestFun, #{trap_exit => false}).

with_conn(TestFun, Opts) when is_map(Opts) ->
    TrapExit = maps:get(trap_exit, Opts, false),
    process_flag(trap_exit, TrapExit),
    {ok, CPid} = emqx_connection:start_link(
        emqx_transport,
        sock,
        maps:merge(
            Opts,
            #{
                zone => default,
                limiter => undefined,
                listener => {tcp, default}
            }
        )
    ),
    TestFun(CPid),
    TrapExit orelse emqx_connection:stop(CPid),
    ok.

trap_exit(Pid, Reason) ->
    receive
        {'EXIT', Pid, Reason} -> ok;
        {'EXIT', Pid, Other} -> error({unexpect_exit, Other})
    after 100 -> error({expect_exit, Reason})
    end.

make_frame(Packet) ->
    iolist_to_binary(emqx_frame:serialize(Packet)).

payload(Len) -> iolist_to_binary(lists:duplicate(Len, 1)).

st() -> st(#{}, #{}).
st(InitFields) when is_map(InitFields) ->
    st(InitFields, #{}).
st(InitFields, ChannelFields) when is_map(InitFields) ->
    St = emqx_connection:init_state(emqx_transport, sock, #{
        zone => default,
        limiter => undefined,
        listener => {tcp, default}
    }),
    maps:fold(
        fun(N, V, S) -> emqx_connection:set_field(N, V, S) end,
        emqx_connection:set_field(channel, channel(ChannelFields), St),
        InitFields
    ).

channel() -> channel(#{}).
channel(InitFields) ->
    ConnInfo = #{
        peername => {{127, 0, 0, 1}, 3456},
        sockname => {{127, 0, 0, 1}, 18083},
        conn_mod => emqx_connection,
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
        listener => {tcp, default},
        protocol => mqtt,
        peerhost => {127, 0, 0, 1},
        clientid => <<"clientid">>,
        username => <<"username">>,
        is_superuser => false,
        mountpoint => undefined
    },
    Session = emqx_session:create(
        ClientInfo,
        #{receive_maximum => 0, expiry_interval => 1000},
        _WillMsg = undefined
    ),
    maps:fold(
        fun(Field, Value, Channel) ->
            emqx_channel:set_field(Field, Value, Channel)
        end,
        emqx_channel:init(ConnInfo, #{
            zone => default,
            limiter => undefined,
            listener => {tcp, default}
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

handle_msg(Msg, St) -> emqx_connection:handle_msg(Msg, St).

handle_call(Pid, Call, St) -> emqx_connection:handle_call(Pid, Call, St).
