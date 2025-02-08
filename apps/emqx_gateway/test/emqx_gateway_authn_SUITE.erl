%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_authn_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_gateway_auth_ct, [with_resource/3]).

-define(checkMatch(Guard),
    (fun(Expr) ->
        case (Expr) of
            Guard ->
                ok;
            X__V ->
                erlang:error(
                    {assertMatch, [
                        {module, ?MODULE},
                        {line, ?LINE},
                        {expression, (??Expr)},
                        {pattern, (??Guard)},
                        {value, X__V}
                    ]}
                )
        end
    end)
).
-define(FUNCTOR(Expr), fun() -> Expr end).
-define(FUNCTOR(Arg, Expr), fun(Arg) -> Expr end).

-define(AUTHNS, [authn_http]).

all() ->
    emqx_gateway_auth_ct:group_names(?AUTHNS).

groups() ->
    emqx_gateway_auth_ct:init_groups(?MODULE, ?AUTHNS).

init_per_group(AuthName, Conf) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_auth_http,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            {emqx_gateway, emqx_gateway_auth_ct:list_gateway_conf()}
            | emqx_gateway_test_utils:all_gateway_apps()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Conf)}
    ),
    _ = emqx_common_test_http:create_default_app(),
    ok = emqx_gateway_auth_ct:start_auth(AuthName),
    [{group_apps, Apps} | Conf].

end_per_group(AuthName, Conf) ->
    ok = emqx_gateway_auth_ct:stop_auth(AuthName),
    _ = emqx_common_test_http:delete_default_app(),
    emqx_config:delete_override_conf_files(),
    ok = emqx_cth_suite:stop(?config(group_apps, Conf)),
    Conf.

init_per_suite(Config) ->
    {ok, Apps1} = application:ensure_all_started(grpc),
    {ok, Apps2} = application:ensure_all_started(cowboy),
    {ok, _} = emqx_gateway_auth_ct:start(),
    [{suite_apps, Apps1 ++ Apps2} | Config].

end_per_suite(Config) ->
    ok = emqx_gateway_auth_ct:stop(),
    ok = emqx_cth_suite:stop_apps(?config(suite_apps, Config)),
    Config.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_case_coap(_) ->
    emqx_coap_SUITE:restart_coap_with_connection_mode(false),
    Login = fun(URI, Checker) ->
        Action = fun(Channel) ->
            Req = emqx_coap_SUITE:make_req(post),
            Checker(emqx_coap_SUITE:do_request(Channel, URI, Req))
        end,
        emqx_coap_SUITE:do(Action)
    end,
    Prefix = emqx_coap_SUITE:mqtt_prefix(),
    RightUrl =
        Prefix ++
            "/connection?clientid=client1&username=admin&password=public",
    Login(RightUrl, ?checkMatch({ok, created, _Data})),

    LeftUrl =
        Prefix ++
            "/connection?clientid=client1&username=bad&password=bad",
    Login(LeftUrl, ?checkMatch({error, bad_request, _Data})),

    disable_authn(coap, udp, default),
    NowRightUrl =
        Prefix ++
            "/connection?clientid=client1&username=bad&password=bad",
    Login(NowRightUrl, ?checkMatch({ok, created, _Data})),
    ok.

-record(coap_content, {content_format, payload = <<>>}).

t_case_lwm2m(_) ->
    MsgId = 12,
    Mod = emqx_lwm2m_SUITE,
    Epn = "urn:oma:lwm2m:oma:3",
    Port = emqx_lwm2m_SUITE:default_port(),
    Login = fun(URI, Checker) ->
        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary, {active, false}])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:test_send_coap_request(
                    Socket,
                    post,
                    Mod:sprintf(URI, [Port, Epn]),
                    #coap_content{
                        content_format = <<"text/plain">>,
                        payload = <<"</1>, </2>, </3>, </4>, </5>">>
                    },
                    [],
                    MsgId
                ),

                Checker(Mod:test_recv_coap_response(Socket))
            end
        )
    end,

    MakeCheker = fun(Type, Method) ->
        fun(Msg) ->
            ?assertEqual(Type, emqx_coap_SUITE:get_field(type, Msg)),
            ?assertEqual(Method, emqx_coap_SUITE:get_field(method, Msg))
        end
    end,

    RightUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1&imei=admin&password=public",
    Login(RightUrl, MakeCheker(ack, {ok, created})),

    LeftUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1&imei=bad&password=bad",
    Login(LeftUrl, MakeCheker(ack, {error, bad_request})),

    NoInfoUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1",
    Login(NoInfoUrl, MakeCheker(ack, {error, bad_request})),

    disable_authn(lwm2m, udp, default),
    NowRightUrl = "coap://127.0.0.1:~b/rd?ep=~ts&lt=345&lwm2m=1&imei=bad&password=bad",
    Login(NowRightUrl, MakeCheker(ack, {ok, created})),

    ok.

-define(SN_CONNACK, 16#05).

t_case_mqttsn(_) ->
    Mod = emqx_sn_protocol_SUITE,
    Login = fun(Username, Password, Expect) ->
        RawCfg = emqx_conf:get_raw([gateway, mqttsn], #{}),
        NewCfg = RawCfg#{
            <<"clientinfo_override">> => #{
                <<"username">> => Username,
                <<"password">> => Password
            }
        },
        emqx_gateway_conf:update_gateway(mqttsn, NewCfg),

        with_resource(
            ?FUNCTOR(gen_udp:open(0, [binary])),
            ?FUNCTOR(Socket, gen_udp:close(Socket)),
            fun(Socket) ->
                Mod:send_connect_msg(Socket, <<"client_id_test1">>),
                ?assertEqual(Expect, Mod:receive_response(Socket))
            end
        )
    end,
    Login(<<"badadmin">>, <<"badpassowrd">>, <<3, ?SN_CONNACK, 16#80>>),
    Login(<<"admin">>, <<"public">>, <<3, ?SN_CONNACK, 0>>),

    disable_authn(mqttsn, udp, default),
    Login(<<"badadmin">>, <<"badpassowrd">>, <<3, ?SN_CONNACK, 0>>),
    ok.

t_case_stomp(_) ->
    Mod = emqx_stomp_SUITE,
    Login = fun(Username, Password, Checker) ->
        Fun = fun(Sock) ->
            gen_tcp:send(
                Sock,
                Mod:serialize(
                    <<"CONNECT">>,
                    [
                        {<<"accept-version">>, Mod:stomp_ver()},
                        {<<"host">>, <<"127.0.0.1:61613">>},
                        {<<"login">>, Username},
                        {<<"passcode">>, Password},
                        {<<"heart-beat">>, <<"1000,2000">>}
                    ]
                )
            ),
            {ok, Data} = gen_tcp:recv(Sock, 0),
            {ok, Frame, _, _} = Mod:parse(Data),
            Checker(Frame)
        end,
        Mod:with_connection(Fun)
    end,
    Login(
        <<"admin">>,
        <<"public">>,
        ?FUNCTOR(
            Frame,
            ?assertEqual(<<"CONNECTED">>, Mod:get_field(command, Frame))
        )
    ),
    Login(<<"bad">>, <<"bad">>, fun(Frame) ->
        ?assertEqual(<<"ERROR">>, Mod:get_field(command, Frame)),
        ?assertEqual(<<"Login Failed: not_authorized">>, Mod:get_field(body, Frame))
    end),

    disable_authn(stomp, tcp, default),
    Login(
        <<"bad">>,
        <<"bad">>,
        ?FUNCTOR(
            Frame,
            ?assertEqual(<<"CONNECTED">>, Mod:get_field(command, Frame))
        )
    ),
    ok.

t_case_exproto(_) ->
    Mod = emqx_exproto_SUITE,
    SvrMod = emqx_exproto_echo_svr,
    Svrs = SvrMod:start(http),
    Login = fun(Username, Password, Expect) ->
        with_resource(
            ?FUNCTOR(Mod:open(tcp)),
            ?FUNCTOR(Sock, Mod:close(Sock)),
            fun(Sock) ->
                Client = #{
                    proto_name => <<"exproto">>,
                    proto_ver => <<"v0.1">>,
                    clientid => <<"test_client_1">>,
                    username => Username
                },

                ConnBin = SvrMod:frame_connect(Client, Password),

                Mod:send(Sock, ConnBin),
                {ok, Recv} = Mod:recv(Sock, 5000),
                C = ?FUNCTOR(Bin, emqx_utils_json:decode(Bin)),
                ?assertEqual(C(Expect), C(Recv))
            end
        )
    end,
    Login(<<"admin">>, <<"public">>, SvrMod:frame_connack(0)),
    Login(<<"bad">>, <<"bad">>, SvrMod:frame_connack(1)),

    disable_authn(exproto, tcp, default),
    Login(<<"bad">>, <<"bad">>, SvrMod:frame_connack(0)),

    SvrMod:stop(Svrs),
    ok.

disable_authn(GwName, Type, Name) ->
    RawCfg = emqx_conf:get_raw([gateway, GwName], #{}),
    ListenerCfg = emqx_utils_maps:deep_get(
        [<<"listeners">>, atom_to_binary(Type), atom_to_binary(Name)], RawCfg
    ),
    {ok, _} = emqx_gateway_conf:update_listener(GwName, {Type, Name}, ListenerCfg#{
        <<"enable_authn">> => false
    }).
