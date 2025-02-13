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

-module(emqx_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SERVER_KEY_PASSWORD, "sErve7r8Key$!").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    generate_tls_certs(Config),
    WorkDir = emqx_cth_suite:work_dir(Config),
    Apps = emqx_cth_suite:start([quicer, emqx], #{work_dir => WorkDir}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) when
    Case =:= t_start_stop_listeners;
    Case =:= t_restart_listeners;
    Case =:= t_wait_for_stop_listeners;
    Case =:= t_restart_listeners_with_hibernate_after_disabled
->
    ok = emqx_listeners:stop(),
    Config;
init_per_testcase(_, Config) ->
    ok = emqx_listeners:start(),
    Config.

end_per_testcase(_, _Config) ->
    ok.

t_start_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    ?assertException(error, _, emqx_listeners:start_listener(ws, {"127.0.0.1", 8083}, #{})),
    ok = emqx_listeners:stop().

t_wait_for_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    meck:new([cowboy], [passthrough, no_history, no_link]),
    %% mock stop_listener return ok but listen port is still open
    meck:expect(cowboy, stop_listener, fun(_) -> ok end),
    List = [
        {<<"ws:default">>, {"127.0.0.1", 8083}},
        {<<"wss:default">>, {"127.0.0.1", 8084}}
    ],
    lists:foreach(
        fun({Id, ListenerOn}) ->
            Start = erlang:system_time(seconds),
            ok = emqx_listeners:stop_listener(Id),
            ?assertEqual(timeout, emqx_listeners:wait_listener_stopped(ListenerOn)),
            End = erlang:system_time(seconds),
            ?assert(End - Start >= 9, "wait_listener_stopped should wait at least 9 seconds")
        end,
        List
    ),
    meck:unload(cowboy),
    lists:foreach(
        fun({Id, ListenerOn}) ->
            ok = emqx_listeners:stop_listener(Id),
            ?assertEqual(ok, emqx_listeners:wait_listener_stopped(ListenerOn))
        end,
        List
    ),
    ok = emqx_listeners:stop(),
    ok.

t_restart_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop().

t_restart_listeners_with_hibernate_after_disabled(_Config) ->
    OldLConf = emqx_config:get([listeners]),
    maps:foreach(
        fun(LType, Listeners) ->
            maps:foreach(
                fun(Name, Opts) ->
                    case maps:is_key(ssl_options, Opts) of
                        true ->
                            emqx_config:put(
                                [
                                    listeners,
                                    LType,
                                    Name,
                                    ssl_options,
                                    hibernate_after
                                ],
                                undefined
                            );
                        _ ->
                            skip
                    end
                end,
                Listeners
            )
        end,
        OldLConf
    ),
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop(),
    emqx_config:put([listeners], OldLConf).

t_max_conns_tcp(_Config) ->
    %% Note: Using a string representation for the bind address like
    %% "127.0.0.1" does not work
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"max_connections">> => 4321
    },
    with_listener(tcp, maxconns, Conf, fun() ->
        ?assertEqual(
            4321,
            emqx_listeners:max_conns('tcp:maxconns', {{127, 0, 0, 1}, Port})
        )
    end).

t_client_attr_as_mountpoint(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    ListenerConf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"mountpoint">> => <<"groups/${client_attrs.ns}/">>
    },
    {ok, Compiled} = emqx_variform:compile("nth(1,tokens(clientid,'-'))"),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], [
        #{
            expression => Compiled,
            set_as_attr => <<"ns">>
        }
    ]),
    with_listener(tcp, attr_as_moutpoint, ListenerConf, fun() ->
        {ok, Client} = emqtt:start_link(#{
            hosts => [{"127.0.0.1", Port}],
            clientid => <<"abc-123">>
        }),
        unlink(Client),
        {ok, _} = emqtt:connect(Client),
        TopicPrefix = atom_to_binary(?FUNCTION_NAME),
        SubTopic = <<TopicPrefix/binary, "/#">>,
        MatchTopic = <<"groups/abc/", TopicPrefix/binary, "/1">>,
        {ok, _, [1]} = emqtt:subscribe(Client, SubTopic, 1),
        ?assertMatch([_], emqx_router:match_routes(MatchTopic)),
        emqtt:stop(Client)
    end),
    emqx_config:put_zone_conf(default, [mqtt, client_attrs_init], []),
    ok.

t_current_conns_tcp(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"max_connections">> => 42
    },
    with_listener(tcp, curconns, Conf, fun() ->
        ?assertEqual(
            0,
            emqx_listeners:current_conns('tcp:curconns', {{127, 0, 0, 1}, Port})
        )
    end).

t_tcp_frame_parsing_conn(_Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"parse_unit">> => <<"frame">>
    },
    with_listener(tcp, ?FUNCTION_NAME, Conf, fun() ->
        Client = emqtt_connect_tcp({127, 0, 0, 1}, Port),
        pong = emqtt:ping(Client),
        ClientId = proplists:get_value(clientid, emqtt:info(Client)),
        [CPid] = emqx_cm:lookup_channels(ClientId),
        CState = emqx_connection:get_state(CPid),
        ?assertMatch(#{listener := {tcp, ?FUNCTION_NAME}}, CState),
        emqx_listeners:is_packet_parser_available(mqtt) andalso
            ?assertMatch(#{parser := {frame, _Options}}, CState)
    end).

t_ssl_frame_parsing_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server.key")
        },
        <<"parse_unit">> => <<"frame">>
    },
    with_listener(ssl, ?FUNCTION_NAME, Conf, fun() ->
        Client = emqtt_connect_ssl({127, 0, 0, 1}, Port, [{verify, verify_none}]),
        pong = emqtt:ping(Client),
        ClientId = proplists:get_value(clientid, emqtt:info(Client)),
        [CPid] = emqx_cm:lookup_channels(ClientId),
        CState = emqx_connection:get_state(CPid),
        ?assertMatch(#{listener := {ssl, ?FUNCTION_NAME}}, CState),
        emqx_listeners:is_packet_parser_available(mqtt) andalso
            ?assertMatch(#{parser := {frame, _Options}}, CState)
    end).

t_wss_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server.key")
        }
    },
    with_listener(wss, wssconn, Conf, fun() ->
        {ok, Socket} = ssl:connect({127, 0, 0, 1}, Port, [{verify, verify_none}], 1000),
        ok = ssl:close(Socket)
    end).

t_quic_conn(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(quic),
    Conf = #{
        <<"bind">> => format_bind({"127.0.0.1", Port}),
        <<"ssl_options">> => #{
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key")
        }
    },
    with_listener(quic, ?FUNCTION_NAME, Conf, fun() ->
        {ok, Conn} = quicer:connect(
            {127, 0, 0, 1},
            Port,
            [
                {verify, verify_none},
                {alpn, ["mqtt"]}
            ],
            1000
        ),
        ok = quicer:close_connection(Conn)
    end).

t_ssl_password_cert(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Port = emqx_common_test_helpers:select_free_port(ssl),
    SSLOptsPWD = #{
        <<"password">> => ?SERVER_KEY_PASSWORD,
        <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
        <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
        <<"keyfile">> => filename:join(PrivDir, "server-password.key")
    },
    LConf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({{127, 0, 0, 1}, Port}),
        <<"ssl_options">> => SSLOptsPWD
    },
    with_listener(ssl, ?FUNCTION_NAME, LConf, fun() ->
        {ok, SSLSocket} = ssl:connect("127.0.0.1", Port, [{verify, verify_none}]),
        ssl:close(SSLSocket)
    end).

t_ssl_update_opts(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ssl, Name, Conf, fun() ->
        %% Client connects successfully.
        ct:pal("attempting successful connection"),
        C1 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: another set of cert/key files.
        ct:pal("updating config"),
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ct:pal("attempting connection with unknown CA"),
        ?assertError(
            {tls_alert, {unknown_ca, _}},
            emqtt_connect_ssl(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        C2 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, ssl, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        try
            emqtt_connect_ssl(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
            ]),
            ct:fail("l ~b: unexpected success", [?LINE])
        catch
            error:{ssl_error, _Socket, {tls_alert, {certificate_required, _}}} ->
                ok;
            error:closed ->
                ok;
            error:connack_timeout ->
                ok;
            K:E:S ->
                error({unexpected_exception, {K, E, S}})
        end,

        C3 = emqtt_connect_ssl(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_wss_update_opts(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ssl),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(wss, Name, Conf, fun() ->
        %% Start a client.
        ct:pal("attempting successful connection"),
        C1 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")}
            | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration.
        %% 1. Another set of (password protected) cert/key files.
        %% 2. Require peer certificate.
        ct:pal("changing config"),
        {ok, _} = emqx:update_config(
            [listeners, wss, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ct:pal("attempting connection with unknown CA"),
        ?assertError(
            {down, {shutdown, {tls_alert, {unknown_ca, _}}}},
            emqtt_connect_wss(Host, Port, [
                {cacerts, public_key:cacerts_get()}
                | ClientSSLOpts
            ])
        ),

        ct:pal("attempting connection with another CA"),
        C2 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")}
            | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, wss, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        ?assertError(
            {ws_upgrade_failed, {closed, {error, {tls_alert, {certificate_required, _}}}}},
            emqtt_connect_wss(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")}
                | ClientSSLOpts
            ])
        ),

        C3 = emqtt_connect_wss(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

t_quic_update_opts(Config) ->
    ListenerType = quic,
    ConnectFun = connect_fun(ListenerType),
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ListenerType),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ListenerType, Name, Conf, fun() ->
        %% Client connects successfully.
        C1 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: another set of cert/key files.
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),

        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ?assertError(
            {transport_down, #{error := _, status := Status}} when
                ((Status =:= bad_certificate orelse
                    Status =:= cert_untrusted_root orelse
                    Status =:= unknown_certificate orelse
                    Status =:= handshake_failure)),
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        C2 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Change the listener SSL configuration: require peer certificate.
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"verify">> => verify_peer,
                    <<"fail_if_no_peer_cert">> => true
                }
            }}
        ),

        %% Unable to connect with old SSL options, certificate is now required.
        ?assertExceptionOneOf(
            {exit, _},
            {error, _},
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
            ])
        ),

        C3 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Change the listener port
        NewPort = emqx_common_test_helpers:select_free_port(ListenerType),
        {ok, _} = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"bind">> => format_bind({Host, NewPort})
            }}
        ),

        %% Connect to old port fail
        ?assertExceptionOneOf(
            {exit, _},
            {error, _},
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca-next.pem")},
                {certfile, filename:join(PrivDir, "client.pem")},
                {keyfile, filename:join(PrivDir, "client.key")}
                | ClientSSLOpts
            ])
        ),

        %% Connect to new port successfully.
        C4 = ConnectFun(Host, NewPort, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")},
            {certfile, filename:join(PrivDir, "client.pem")},
            {keyfile, filename:join(PrivDir, "client.key")}
            | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),
        ?assertEqual(pong, emqtt:ping(C4)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3),
        ok = emqtt:stop(C4)
    end).

t_quic_update_opts_fail(Config) ->
    ListenerType = quic,
    ConnectFun = connect_fun(ListenerType),
    PrivDir = ?config(priv_dir, Config),
    Host = "127.0.0.1",
    Port = emqx_common_test_helpers:select_free_port(ListenerType),
    Conf = #{
        <<"enable">> => true,
        <<"bind">> => format_bind({Host, Port}),
        <<"ssl_options">> => #{
            <<"cacertfile">> => filename:join(PrivDir, "ca.pem"),
            <<"password">> => ?SERVER_KEY_PASSWORD,
            <<"certfile">> => filename:join(PrivDir, "server-password.pem"),
            <<"keyfile">> => filename:join(PrivDir, "server-password.key"),
            <<"verify">> => verify_none
        }
    },
    ClientSSLOpts = [
        {verify, verify_peer},
        {customize_hostname_check, [{match_fun, fun(_, _) -> true end}]}
    ],
    Name = ?FUNCTION_NAME,
    with_listener(ListenerType, Name, Conf, fun() ->
        %% GIVEN: an working Listener that client could connect to.
        C1 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% WHEN: reload the listener with invalid SSL options (certfile and keyfile missmatch).
        UpdateResult1 = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server-password.key")
                }
            }}
        ),

        %% THEN: Reload failed but old listener is rollbacked.
        ?assertMatch(
            {error, {post_config_update, emqx_listeners, {rollbacked, {error, tls_error}}}},
            UpdateResult1
        ),

        %% THEN: Client with old TLS options could still connect
        C2 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
        ]),

        %% WHEN: Change the listener SSL configuration again
        UpdateResult2 = emqx:update_config(
            [listeners, ListenerType, Name],
            {update, #{
                <<"ssl_options">> => #{
                    <<"cacertfile">> => filename:join(PrivDir, "ca-next.pem"),
                    <<"certfile">> => filename:join(PrivDir, "server.pem"),
                    <<"keyfile">> => filename:join(PrivDir, "server.key")
                }
            }}
        ),
        %% THEN: update should success
        ?assertMatch({ok, _}, UpdateResult2),

        %% THEN: Client with old TLS options could not connect
        %% Unable to connect with old SSL options, server's cert is signed by another CA.
        ?assertError(
            {transport_down, #{error := _, status := Status}} when
                ((Status =:= bad_certificate orelse
                    Status =:= cert_untrusted_root orelse
                    Status =:= unknown_certificate orelse
                    Status =:= handshake_failure)),
            ConnectFun(Host, Port, [
                {cacertfile, filename:join(PrivDir, "ca.pem")} | ClientSSLOpts
            ])
        ),

        %% THEN: Client with new TLS options could connect
        C3 = ConnectFun(Host, Port, [
            {cacertfile, filename:join(PrivDir, "ca-next.pem")} | ClientSSLOpts
        ]),

        %% Both pre- and post-update clients should be alive.
        ?assertEqual(pong, emqtt:ping(C1)),
        ?assertEqual(pong, emqtt:ping(C2)),
        ?assertEqual(pong, emqtt:ping(C3)),

        ok = emqtt:stop(C1),
        ok = emqtt:stop(C2),
        ok = emqtt:stop(C3)
    end).

with_listener(Type, Name, Config, Then) ->
    {ok, _} = emqx:update_config([listeners, Type, Name], {create, Config}),
    try
        Then()
    after
        ok = emqx_listeners:stop(),
        emqx:remove_config([listeners, Type, Name])
    end.

emqtt_connect_tcp(Host, Port) ->
    emqtt_connect(fun emqtt:connect/1, #{
        host => Host,
        port => Port,
        connect_timeout => 500
    }).

emqtt_connect_ssl(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ssl => true,
        ssl_opts => SSLOpts
    }).

emqtt_connect_quic(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:quic_connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ssl => true,
        ssl_opts => SSLOpts
    }).

emqtt_connect_wss(Host, Port, SSLOpts) ->
    emqtt_connect(fun emqtt:ws_connect/1, #{
        hosts => [{Host, Port}],
        connect_timeout => 500,
        ws_transport_options => [
            {protocols, [http]},
            {transport, tls},
            {tls_opts, SSLOpts}
        ]
    }).

emqtt_connect(Connect, Opts) ->
    case emqtt:start_link(Opts) of
        {ok, Client} ->
            true = erlang:unlink(Client),
            case Connect(Client) of
                {ok, _} -> Client;
                {error, Reason} -> error(Reason, [Opts])
            end;
        {error, Reason} ->
            error(Reason, [Opts])
    end.

t_format_bind(_) ->
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(1883))
    ),
    ?assertEqual(
        "0.0.0.0:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "[::]:1883",
        lists:flatten(emqx_listeners:format_bind({{0, 0, 0, 0, 0, 0, 0, 0}, 1883}))
    ),
    ?assertEqual(
        "127.0.0.1:1883",
        lists:flatten(emqx_listeners:format_bind({{127, 0, 0, 1}, 1883}))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind("1883"))
    ),
    ?assertEqual(
        ":1883",
        lists:flatten(emqx_listeners:format_bind(":1883"))
    ).

generate_tls_certs(Config) ->
    PrivDir = ?config(priv_dir, Config),
    emqx_common_test_helpers:gen_ca(PrivDir, "ca"),
    emqx_common_test_helpers:gen_ca(PrivDir, "ca-next"),
    emqx_common_test_helpers:gen_host_cert("server", "ca-next", PrivDir, #{}),
    emqx_common_test_helpers:gen_host_cert("client", "ca-next", PrivDir, #{}),
    emqx_common_test_helpers:gen_host_cert("server-password", "ca", PrivDir, #{
        password => ?SERVER_KEY_PASSWORD
    }).

format_bind(Bind) ->
    iolist_to_binary(emqx_listeners:format_bind(Bind)).

connect_fun(ssl) ->
    fun emqtt_connect_ssl/3;
connect_fun(quic) ->
    fun emqtt_connect_quic/3;
connect_fun(wss) ->
    fun emqtt_connect_wss/3.
