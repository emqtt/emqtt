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
-module(emqx_mgmt_api_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PORT(Base), (Base + ?LINE)).
-define(PORT, ?PORT(20000)).

all() ->
    [
        {group, with_defaults_in_file},
        {group, without_defaults_in_file},
        {group, max_connections}
    ].

groups() ->
    AllTests = emqx_common_test_helpers:all(?MODULE),
    MaxConnTests = [
        t_max_connection_default
    ],
    ZoneTests = [
        t_update_listener_zone
    ],
    [
        {with_defaults_in_file, AllTests -- MaxConnTests},
        {without_defaults_in_file, AllTests -- (MaxConnTests ++ ZoneTests)},
        {max_connections, MaxConnTests}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(without_defaults_in_file, Config) ->
    init_group_apps(#{}, Config);
init_per_group(with_defaults_in_file, Config) ->
    %% we have to materialize the config file with default values for this test group
    %% because we want to test the deletion of non-existing listener
    %% if there is no config file, the such deletion would result in a deletion
    %% of the default listener.
    PrivDir = ?config(priv_dir, Config),
    FileName = filename:join([PrivDir, "etc", atom_to_list(?MODULE) ++ "-default-listeners"]),
    ok = filelib:ensure_dir(FileName),
    ok = file:write_file(FileName, default_listeners_hocon_text()),
    init_group_apps("include \"" ++ FileName ++ "\"", Config);
init_per_group(max_connections, Config) ->
    init_group_apps(
        io_lib:format("listeners.tcp.max_connection_test {bind = \"0.0.0.0:~p\"}", [?PORT]),
        Config
    ).

init_group_apps(Config, CTConfig) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx_conf, Config},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(CTConfig)}
    ),
    [{suite_apps, Apps} | CTConfig].

end_per_group(_Group, Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({init, Config})
    catch
        error:function_clause ->
            Config
    end.

end_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({'end', Config})
    catch
        error:function_clause ->
            ok
    end.

t_max_connection_default(Config) when is_list(Config) ->
    #{<<"listeners">> := Listeners} = emqx_mgmt_api_listeners:do_list_listeners(),
    Target = lists:filter(
        fun(#{<<"id">> := Id}) -> Id =:= 'tcp:max_connection_test' end,
        Listeners
    ),
    DefaultMaxConn = emqx_listeners:default_max_conn(),
    ?assertMatch([#{<<"max_connections">> := DefaultMaxConn}], Target),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", "tcp:max_connection_test"]),
    ?assertMatch(#{<<"max_connections">> := DefaultMaxConn}, request(get, NewPath, [], [])),
    emqx_conf:tombstone([listeners, tcp, max_connection_test], #{override_to => cluster}),
    ok.

t_list_listeners(Config) when is_list(Config) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners"]),
    Res = request(get, Path, [], []),
    #{<<"listeners">> := Expect} = emqx_mgmt_api_listeners:do_list_listeners(),
    ?assertEqual(length(Expect), length(Res)),

    %% POST /listeners
    ListenerId = <<"tcp:default">>,
    NewListenerId = <<"tcp:new11">>,

    OriginPath = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),

    OriginListener = request(get, OriginPath, [], []),

    %% create with full options
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),

    OriginListener2 = maps:remove(<<"id">>, OriginListener),
    Port = integer_to_binary(?PORT),
    NewConf = OriginListener2#{
        <<"name">> => <<"new11">>,
        <<"bind">> => <<"0.0.0.0:", Port/binary>>,
        <<"max_connections">> := <<"infinity">>
    },
    Create = request(post, Path, [], NewConf),
    ?assertEqual(lists:sort(maps:keys(OriginListener)), lists:sort(maps:keys(Create))),
    Get1 = request(get, NewPath, [], []),
    ?assertMatch(Create, Get1),
    ?assertMatch(#{<<"max_connections">> := <<"infinity">>}, Create),
    ?assert(is_running(NewListenerId)),

    Update2 = request(put, NewPath, [], Create#{<<"max_connections">> => 100}),
    ?assertMatch(#{<<"max_connections">> := 100}, Update2),
    Get2 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"max_connections">> := 100}, Get2),

    Update3 = request(put, NewPath, [], Create#{<<"max_connections">> => <<"123">>}),
    ?assertMatch(#{<<"max_connections">> := 123}, Update3),
    Get3 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"max_connections">> := 123}, Get3),

    %% delete
    ?assertEqual([], delete(NewPath)),
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),
    ok.

t_tcp_crud_listeners_by_id(Config) when is_list(Config) ->
    ListenerId = <<"tcp:default">>,
    NewListenerId = <<"tcp:new">>,
    MinListenerId = <<"tcp:min">>,
    BadId = <<"tcp:bad">>,
    Type = <<"tcp">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type, 31000).

t_ssl_crud_listeners_by_id(Config) when is_list(Config) ->
    ListenerId = <<"ssl:default">>,
    NewListenerId = <<"ssl:new">>,
    MinListenerId = <<"ssl:min">>,
    BadId = <<"ssl:bad">>,
    Type = <<"ssl">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type, 32000).

t_ws_crud_listeners_by_id(Config) when is_list(Config) ->
    ListenerId = <<"ws:default">>,
    NewListenerId = <<"ws:new">>,
    MinListenerId = <<"ws:min">>,
    BadId = <<"ws:bad">>,
    Type = <<"ws">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type, 33000).

t_wss_crud_listeners_by_id(Config) when is_list(Config) ->
    ListenerId = <<"wss:default">>,
    NewListenerId = <<"wss:new">>,
    MinListenerId = <<"wss:min">>,
    BadId = <<"wss:bad">>,
    Type = <<"wss">>,
    crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type, 34000).

t_api_listeners_list_not_ready(Config) when is_list(Config) ->
    ct:timetrap({seconds, 120}),
    Apps = [
        {emqx, #{after_start => fun() -> emqx_app:set_config_loader(emqx) end}},
        {emqx_conf, #{}}
    ],
    Nodes =
        [Node1, Node2] = emqx_cth_cluster:start(
            [
                {t_api_listeners_list_not_ready1, #{role => core, apps => Apps}},
                {t_api_listeners_list_not_ready2, #{role => core, apps => Apps}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    try
        L1 = get_tcp_listeners(Node1),

        %% test init_config not ready.
        _ = rpc:call(Node1, emqx_app, set_config_loader, [emqx]),
        assert_config_load_not_done(Node1),

        L2 = get_tcp_listeners(Node1),
        L3 = get_tcp_listeners(Node2),

        Comment = #{
            node1 => rpc:call(Node1, emqx, running_nodes, []),
            node2 => rpc:call(Node2, emqx, running_nodes, [])
        },

        ?assert(length(L1) > length(L2), Comment),
        ?assertEqual(length(L2), length(L3), Comment)
    after
        emqx_cth_cluster:stop(Nodes)
    end.

t_clear_certs(Config) when is_list(Config) ->
    ListenerId = <<"ssl:default">>,
    NewListenerId = <<"ssl:clear">>,

    OriginPath = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),
    ConfTempT = request(get, OriginPath, [], []),
    Port = integer_to_binary(?PORT),
    ConfTemp = ConfTempT#{
        <<"id">> => NewListenerId,
        <<"bind">> => <<"0.0.0.0:", Port/binary>>
    },

    %% create, make sure the cert files are created
    NewConf = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"certfile">>], ConfTemp, cert_file("certfile")
    ),
    NewConf2 = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"keyfile">>], NewConf, cert_file("keyfile")
    ),
    _ = request(post, NewPath, [], NewConf2),
    ListResult1 = list_pem_dir("ssl", "clear"),
    ?assertMatch({ok, [_, _]}, ListResult1),

    %% update
    UpdateConf = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"keyfile">>], NewConf2, cert_file("keyfile2")
    ),
    _ = request(put, NewPath, [], UpdateConf),
    _ = emqx_tls_certfile_gc:force(),
    ListResult2 = list_pem_dir("ssl", "clear"),

    %% make sure the old cert file is deleted
    ?assertMatch({ok, [_, _]}, ListResult2),

    {ok, ResultList1} = ListResult1,
    {ok, ResultList2} = ListResult2,

    FindKeyFile = fun(List) ->
        case lists:search(fun(E) -> lists:prefix("key", E) end, List) of
            {value, Value} ->
                Value;
            _ ->
                ?assert(false, "Can't find keyfile")
        end
    end,

    %% check the keyfile has changed
    ?assertNotEqual(FindKeyFile(ResultList1), FindKeyFile(ResultList2)),

    %% remove, check all cert files are deleted
    _ = delete(NewPath),
    _ = emqx_tls_certfile_gc:force(),
    ?assertMatch({error, enoent}, list_pem_dir("ssl", "clear")),

    %% test create listeners without id in path
    NewPath1 = emqx_mgmt_api_test_util:api_path(["listeners"]),
    NewConf3 = maps:remove(<<"id">>, NewConf2#{<<"name">> => <<"clear">>}),
    ?assertNotMatch({error, {"HTTP/1.1", 400, _}}, request(post, NewPath1, [], NewConf3)),
    ListResult3 = list_pem_dir("ssl", "clear"),
    ?assertMatch({ok, [_, _]}, ListResult3),
    _ = delete(NewPath),
    _ = emqx_tls_certfile_gc:force(),
    ?assertMatch({error, enoent}, list_pem_dir("ssl", "clear")),

    ok.

get_tcp_listeners(Node) ->
    Query = #{query_string => #{<<"type">> => tcp}},
    {200, L} = rpc:call(Node, emqx_mgmt_api_listeners, list_listeners, [get, Query]),
    [#{node_status := NodeStatus}] = L,
    ct:pal("Node:~p:~p", [Node, L]),
    NodeStatus.

assert_config_load_not_done(Node) ->
    Prio = rpc:call(Node, emqx_app, get_config_loader, []),
    ?assertEqual(emqx, Prio, #{node => Node}).

crud_listeners_by_id(ListenerId, NewListenerId, MinListenerId, BadId, Type, PortBase) ->
    OriginPath = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),
    OriginListener = request(get, OriginPath, [], []),

    %% create with full options
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),
    Port1 = integer_to_binary(?PORT(PortBase)),
    Port2 = integer_to_binary(?PORT(PortBase)),
    NewConf = OriginListener#{
        <<"id">> => NewListenerId,
        <<"bind">> => <<"0.0.0.0:", Port1/binary>>
    },
    Create = request(post, NewPath, [], NewConf),
    ?assertEqual(lists:sort(maps:keys(OriginListener)), lists:sort(maps:keys(Create))),
    Get1 = request(get, NewPath, [], []),
    ?assertMatch(Create, Get1),
    ?assertEqual({true, NewListenerId}, {is_running(NewListenerId), NewListenerId}),

    %% create with required options
    MinPath = emqx_mgmt_api_test_util:api_path(["listeners", MinListenerId]),
    ?assertEqual({error, not_found}, is_running(MinListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, MinPath, [], [])),
    MinConf =
        case OriginListener of
            #{
                <<"ssl_options">> :=
                    #{
                        <<"cacertfile">> := CaCertFile,
                        <<"certfile">> := CertFile,
                        <<"keyfile">> := KeyFile
                    }
            } ->
                #{
                    <<"id">> => MinListenerId,
                    <<"bind">> => <<"0.0.0.0:", Port2/binary>>,
                    <<"type">> => Type,
                    <<"ssl_options">> => #{
                        <<"cacertfile">> => CaCertFile,
                        <<"certfile">> => CertFile,
                        <<"keyfile">> => KeyFile
                    }
                };
            _ ->
                #{
                    <<"id">> => MinListenerId,
                    <<"bind">> => <<"0.0.0.0:", Port2/binary>>,
                    <<"type">> => Type
                }
        end,
    MinCreate = request(post, MinPath, [], MinConf),
    ?assertEqual(lists:sort(maps:keys(OriginListener)), lists:sort(maps:keys(MinCreate))),
    MinGet = request(get, MinPath, [], []),
    ?assertMatch(MinCreate, MinGet),
    ?assert(is_running(MinListenerId)),

    %% bad create(same port)
    BadPath = emqx_mgmt_api_test_util:api_path(["listeners", BadId]),
    BadConf = OriginListener#{
        <<"id">> => BadId,
        <<"bind">> => <<"0.0.0.0:", Port1/binary>>
    },
    ?assertMatch({error, {"HTTP/1.1", 400, _}}, request(post, BadPath, [], BadConf)),

    %% update
    #{<<"acceptors">> := Acceptors} = Create,
    Acceptors1 = Acceptors + 10,
    Update =
        request(put, NewPath, [], Create#{<<"acceptors">> => Acceptors1}),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Update),
    Get2 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors1}, Get2),
    ?assert(is_running(NewListenerId)),

    %% update an stopped listener
    action_listener(NewListenerId, "stop", false),
    ?assertNot(is_running(NewListenerId)),
    %% update
    Get3 = request(get, NewPath, [], []),
    #{<<"acceptors">> := Acceptors3} = Get3,
    Acceptors4 = Acceptors3 + 1,
    Update1 =
        request(put, NewPath, [], Get3#{<<"acceptors">> => Acceptors4}),
    ?assertMatch(#{<<"acceptors">> := Acceptors4}, Update1),
    Get4 = request(get, NewPath, [], []),
    ?assertMatch(#{<<"acceptors">> := Acceptors4}, Get4),
    ?assertNot(is_running(NewListenerId)),

    %% delete
    ?assertEqual([], delete(NewPath)),
    ?assertEqual([], delete(MinPath)),
    ?assertEqual({error, not_found}, is_running(NewListenerId)),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(get, NewPath, [], [])),
    ?assertMatch({error, {"HTTP/1.1", 404, _}}, request(delete, NewPath, [], [])),
    ok.

t_update_listener_zone({init, Config}) ->
    %% fake a zone
    Config;
t_update_listener_zone({'end', _Config}) ->
    ok;
t_update_listener_zone(_Config) ->
    ListenerId = <<"tcp:default">>,
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    Conf = request(get, Path, [], []),
    %% update
    AddConf1 = Conf#{<<"zone">> => <<"unknownzone">>},
    AddConf2 = Conf#{<<"zone">> => <<"zone1">>},
    ?assertMatch({error, {_, 400, _}}, request(put, Path, [], AddConf1)),
    ?assertMatch(#{<<"zone">> := <<"zone1">>}, request(put, Path, [], AddConf2)).

t_update_listener_max_conn_rate({init, Config}) ->
    Config;
t_update_listener_max_conn_rate({'end', _Config}) ->
    ok;
t_update_listener_max_conn_rate(_Config) ->
    ListenerId = <<"tcp:default">>,
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    Conf = request(get, Path, [], []),
    %% check there is no limiter by default
    ?assertNotMatch(#{<<"max_conn_rate">> := _}, Conf),
    %% Update to infinity
    UpdateConfToInfinity = Conf#{<<"max_conn_rate">> => <<"infinity">>},
    ?assertMatch(
        #{<<"max_conn_rate">> := <<"infinity">>},
        request(put, Path, [], UpdateConfToInfinity)
    ),
    %% Update to 42/s
    UpdateConfTo42PerSec = Conf#{<<"max_conn_rate">> => <<"42/s">>},
    ?assertMatch(
        #{<<"max_conn_rate">> := <<"42/s">>},
        request(put, Path, [], UpdateConfTo42PerSec)
    ),
    %% Update back to infinity
    UpdateConfToInfinity = Conf#{<<"max_conn_rate">> => <<"infinity">>},
    ?assertMatch(
        #{<<"max_conn_rate">> := <<"infinity">>},
        request(put, Path, [], UpdateConfToInfinity)
    ).

t_delete_nonexistent_listener(Config) when is_list(Config) ->
    NonExist = emqx_mgmt_api_test_util:api_path(["listeners", "tcp:nonexistent"]),
    ?assertMatch(
        {error, {_, 404, _}},
        request(delete, NonExist, [], [])
    ),
    ok.

t_action_listeners(Config) when is_list(Config) ->
    ID = "tcp:default",
    action_listener(ID, "stop", false),
    action_listener(ID, "start", true),
    action_listener(ID, "restart", true).

t_update_validation_error_message({init, Config}) ->
    NewListenerId = <<"ssl:new", (integer_to_binary(?LINE))/binary>>,
    NewPath = emqx_mgmt_api_test_util:api_path(["listeners", NewListenerId]),
    ListenerId = "ssl:default",
    OriginalPath = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    OriginalListener = request(get, OriginalPath, [], []),
    [
        {new_listener_id, NewListenerId},
        {new_path, NewPath},
        {original_listener, OriginalListener}
        | Config
    ];
t_update_validation_error_message(Config) when is_list(Config) ->
    NewListenerId = ?config(new_listener_id, Config),
    NewPath = ?config(new_path, Config),
    OriginalListener = ?config(original_listener, Config),
    Port = integer_to_binary(?PORT),
    NewListener = OriginalListener#{
        <<"id">> := NewListenerId,
        <<"bind">> => <<"0.0.0.0:", Port/binary>>
    },
    CreateResp = request(post, NewPath, [], NewListener),
    ?assertEqual(lists:sort(maps:keys(OriginalListener)), lists:sort(maps:keys(CreateResp))),

    %% check that a validation error is user-friendly
    WrongConf1a = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"enable_crl_check">>],
        CreateResp,
        true
    ),
    WrongConf1 = emqx_utils_maps:deep_put(
        [<<"ssl_options">>, <<"verify">>],
        WrongConf1a,
        <<"verify_none">>
    ),
    Result1 = request(put, NewPath, [], WrongConf1, #{return_all => true}),
    ?assertMatch({error, {{_, 400, _}, _Headers, _Body}}, Result1),
    {error, {{_, _Code, _}, _Headers, Body1}} = Result1,
    #{<<"message">> := RawMsg1} = emqx_utils_json:decode(Body1),
    Msg1 = emqx_utils_json:decode(RawMsg1),
    %% No confusing union type errors.
    ?assertNotMatch(#{<<"mismatches">> := _}, Msg1),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"verify must be verify_peer when CRL check is enabled">>,
            <<"value">> := #{}
        },
        Msg1
    ),
    ok;
t_update_validation_error_message({'end', Config}) ->
    NewPath = ?config(new_path, Config),
    ?assertEqual([], delete(NewPath)),
    ok.

action_listener(ID, Action, Running) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ID, Action]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path),
    timer:sleep(500),
    GetPath = emqx_mgmt_api_test_util:api_path(["listeners", ID]),
    Listener = request(get, GetPath, [], []),
    listener_stats(Listener, Running).

request(Method, Url, QueryParams, Body) ->
    request(Method, Url, QueryParams, Body, _Opts = #{}).

request(Method, Url, QueryParams, Body, Opts) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(Method, Url, QueryParams, AuthHeader, Body, Opts) of
        {ok, Res} -> emqx_utils_json:decode(Res);
        Error -> Error
    end.

delete(Url) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Res} = emqx_mgmt_api_test_util:request_api(delete, Url, AuthHeader),
    Res.

listener_stats(Listener, ExpectedStats) ->
    ?assertEqual(ExpectedStats, maps:get(<<"running">>, Listener)).

is_running(Id) ->
    emqx_listeners:is_running(binary_to_atom(Id)).

list_pem_dir(Type, Name) ->
    ListenerDir = emqx_listeners:certs_dir(Type, Name),
    Dir = filename:join([emqx:mutable_certs_dir(), ListenerDir]),
    file:list_dir(Dir).

data_file(Name) ->
    Dir = code:lib_dir(emqx),
    {ok, Bin} = file:read_file(filename:join([Dir, "test", "data", Name])),
    Bin.

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).

default_listeners_hocon_text() ->
    Sc = #{roots => emqx_schema:listeners()},
    Listeners = hocon_tconf:make_serializable(Sc, #{}, #{}),
    Zones = #{<<"zone1">> => #{<<"mqtt">> => #{<<"max_inflight">> => 2}}},
    Config = #{
        <<"listeners">> => Listeners,
        <<"zones">> => Zones
    },
    hocon_pp:do(Config, #{}).
