%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_dashboard_https_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_dashboard.hrl").

-define(NAME, 'https:dashboard').
-define(HOST, "https://127.0.0.1:18084").
-define(BASE_PATH, "/api/v5").
-define(OVERVIEWS, [
    "alarms",
    "banned",
    "stats",
    "metrics",
    "listeners",
    "clients",
    "subscriptions"
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) -> Config.
end_per_suite(_Config) -> emqx_mgmt_api_test_util:end_suite([emqx_management]).

init_per_testcase(_TestCase, Config) -> Config.
end_per_testcase(_TestCase, _Config) -> emqx_mgmt_api_test_util:end_suite([emqx_management]).

t_default_ssl_cert(_Config) ->
    Conf = #{dashboard => #{listeners => #{https => #{bind => 18084, enable => true}}}},
    validate_https(Conf, 512, default_ssl_cert(), verify_none),
    ok.

t_normal_ssl_cert(_Config) ->
    MaxConnection = 1000,
    Conf = #{
        dashboard => #{
            listeners => #{
                https => #{
                    bind => 18084,
                    enable => true,
                    cacertfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cacert.pem">>),
                    certfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cert.pem">>),
                    keyfile => naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/key.pem">>),
                    max_connections => MaxConnection
                }
            }
        }
    },
    validate_https(Conf, MaxConnection, default_ssl_cert(), verify_none),
    ok.

t_verify_cacertfile(_Config) ->
    MaxConnection = 1024,
    DefaultSSLCert = default_ssl_cert(),
    SSLCert = DefaultSSLCert#{cacertfile => <<"">>},
    %% default #{verify => verify_none}
    Conf = #{
        dashboard => #{
            listeners => #{
                https => #{
                    bind => 18084,
                    enable => true,
                    cacertfile => <<"">>,
                    max_connections => MaxConnection
                }
            }
        }
    },
    validate_https(Conf, MaxConnection, SSLCert, verify_none),
    %% verify_peer but cacertfile is empty
    VerifyPeerConf1 = emqx_utils_maps:deep_put(
        [dashboard, listeners, https, verify],
        Conf,
        verify_peer
    ),
    emqx_common_test_helpers:load_config(emqx_dashboard_schema, VerifyPeerConf1),
    ?assertMatch({error, [?NAME]}, emqx_dashboard:start_listeners()),
    %% verify_peer and cacertfile is ok.
    VerifyPeerConf2 = emqx_utils_maps:deep_put(
        [dashboard, listeners, https, cacertfile],
        VerifyPeerConf1,
        naive_env_interpolation(<<"${EMQX_ETC_DIR}/certs/cacert.pem">>)
    ),
    validate_https(VerifyPeerConf2, MaxConnection, DefaultSSLCert, verify_peer),
    ok.

t_bad_certfile(_Config) ->
    Conf = #{
        dashboard => #{
            listeners => #{
                https => #{
                    bind => 18084,
                    enable => true,
                    certfile => <<"${EMQX_ETC_DIR}/certs/not_found_cert.pem">>
                }
            }
        }
    },
    emqx_common_test_helpers:load_config(emqx_dashboard_schema, Conf),
    ?assertMatch({error, [?NAME]}, emqx_dashboard:start_listeners()),
    ok.

validate_https(Conf, MaxConnection, SSLCert, Verify) ->
    emqx_common_test_helpers:load_config(emqx_dashboard_schema, Conf),
    emqx_mgmt_api_test_util:init_suite([emqx_management], fun(X) -> X end),
    assert_ranch_options(MaxConnection, SSLCert, Verify),
    assert_https_request(),
    emqx_mgmt_api_test_util:end_suite([emqx_management]).

assert_ranch_options(MaxConnections0, SSLCert, Verify) ->
    Middlewares = [emqx_dashboard_middleware, cowboy_router, cowboy_handler],
    [
        ?NAME,
        ranch_ssl,
        #{
            max_connections := MaxConnections,
            num_acceptors := _,
            socket_opts := SocketOpts
        },
        cowboy_tls,
        #{
            env := #{
                dispatch := {persistent_term, ?NAME},
                options := #{
                    name := ?NAME,
                    protocol := https,
                    protocol_options := #{proxy_header := false},
                    security := [#{basicAuth := []}, #{bearerAuth := []}],
                    swagger_global_spec := _
                }
            },
            middlewares := Middlewares,
            proxy_header := false
        }
    ] = ranch_server:get_listener_start_args(?NAME),
    ?assertEqual(MaxConnections0, MaxConnections),
    ?assert(lists:member(inet, SocketOpts), SocketOpts),
    #{
        backlog := 1024,
        ciphers := Ciphers,
        port := 18084,
        send_timeout := 10000,
        verify := Verify,
        versions := Versions
    } = SocketMaps = maps:from_list(SocketOpts -- [inet]),
    %% without tlsv1.1 tlsv1
    ?assertMatch(['tlsv1.3', 'tlsv1.2'], Versions),
    ?assert(Ciphers =/= []),
    maps:foreach(
        fun(K, ConfVal) ->
            case maps:find(K, SocketMaps) of
                {ok, File} -> ?assertEqual(naive_env_interpolation(ConfVal), File);
                error -> ?assertEqual(<<"">>, ConfVal)
            end
        end,
        SSLCert
    ),
    ?assertMatch(
        #{
            env := #{dispatch := {persistent_term, ?NAME}},
            middlewares := Middlewares,
            proxy_header := false
        },
        ranch:get_protocol_options(?NAME)
    ),
    ok.

assert_https_request() ->
    Headers = emqx_dashboard_SUITE:auth_header_(),
    lists:foreach(
        fun(Path) ->
            ApiPath = api_path([Path]),
            ?assertMatch(
                {ok, _},
                emqx_dashboard_SUITE:request_dashboard(get, ApiPath, Headers)
            )
        end,
        ?OVERVIEWS
    ).

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

naive_env_interpolation(Str0) ->
    Str1 = emqx_schema:naive_env_interpolation(Str0),
    %% assert all envs are replaced
    ?assertNot(lists:member($$, Str1)),
    Str1.

default_ssl_cert() ->
    #{
        cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
        certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
        keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>
    }.
