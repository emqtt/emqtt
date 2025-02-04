%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_license_test_lib:mock_parser(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, "license { key = \"default\" }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_license_test_lib:unmock_parser(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    setup_test(Case, Config) ++ Config.

end_per_testcase(Case, Config) ->
    teardown_test(Case, Config).

setup_test(_TestCase, _Config) ->
    [].

teardown_test(_TestCase, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_update_value(_Config) ->
    ?assertMatch(
        {error, #{parse_results := [_ | _]}},
        emqx_license:update_key("invalid.license")
    ),

    LicenseValue = emqx_license_test_lib:default_test_license(),

    ?assertMatch(
        {ok, #{}},
        emqx_license:update_key(LicenseValue)
    ).

t_check_exceeded(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),
    #{} = update(License),

    Pids = lists:map(
        fun(_) ->
            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C),
            C
        end,
        lists:seq(1, 12)
    ),
    sync_cache(),
    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ),
    ok = lists:foreach(fun(Pid) -> emqtt:stop(Pid) end, Pids).

t_check_exceeded_non_clean(_Config) ->
    ?assertEqual(0, emqx_cm:get_sessions_count()),
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),
    #{} = update(License),
    Properties = #{'Session-Expiry-Interval' => 10},
    IDs = [iolist_to_binary(["test-client-", integer_to_list(I)]) || I <- lists:seq(1, 12)],
    Pids = lists:map(
        fun(Id) ->
            {ok, C} = emqtt:start_link([{clientid, Id}, {proto_ver, v5}, {properties, Properties}]),
            {ok, _} = emqtt:connect(C),
            C
        end,
        IDs
    ),
    sync_cache(),
    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ),
    ok = lists:foreach(fun(Pid) -> emqtt:stop(Pid) end, Pids),
    %% wait until all clients disconnected
    ?retry(100, 50, ?assertEqual(0, emqx_cm:get_connected_client_count())),
    ?assertEqual(12, emqx_cm:get_sessions_count()),
    %% continue to expect quota exceeded
    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ),
    lists:foreach(fun emqx_cm:kick_session/1, IDs),
    ?retry(100, 50, ?assertEqual(0, emqx_cm:get_sessions_count())),
    ok.

t_check_ok(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),
    #{} = update(License),

    Pids = lists:map(
        fun(I) ->
            {ok, C} = emqtt:start_link([{proto_ver, v5}]),
            ?assertMatch({I, {ok, _}}, {I, emqtt:connect(C)}),
            C
        end,
        lists:seq(1, 11)
    ),
    ?assertEqual(
        {ok, #{}},
        emqx_license:check(#{}, #{})
    ),
    ok = lists:foreach(fun(Pid) -> emqtt:stop(Pid) end, Pids).

t_check_expired(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            %% Official customer
            "1",
            %% Small customer
            "0",
            "Foo",
            "contact@foo.com",
            "bar",
            %% Expired long ago
            "20211101",
            "10",
            "10"
        ]
    ),
    #{} = update(License),

    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_check_max_uptime_reached(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20991231",
            "1",
            "123"
        ]
    ),

    meck:new(emqx_license_parser_v20220101, [passthrough, no_history]),
    meck:expect(emqx_license_parser_v20220101, max_uptime_seconds, fun(_) -> 0 end),

    #{} = update(License),

    meck:unload(emqx_license_parser_v20220101),

    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_check_not_loaded(_Config) ->
    ok = emqx_license_checker:purge(),
    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_import_config(_Config) ->
    %% Import to default license
    ?assertMatch(
        {ok, #{root_key := license, changed := _}},
        emqx_license:import_config(#{<<"license">> => #{<<"key">> => <<"default">>}})
    ),
    ?assertEqual(default, emqx:get_config([license, key])),
    ?assertMatch({ok, #{max_connections := 10}}, emqx_license_checker:limits()),

    %% Import to a new license
    EncodedLicense = emqx_license_test_lib:make_license(#{max_connections => "100"}),
    ?assertMatch(
        {ok, #{root_key := license, changed := _}},
        emqx_license:import_config(
            #{
                <<"license">> =>
                    #{
                        <<"key">> => EncodedLicense,
                        <<"connection_low_watermark">> => <<"20%">>,
                        <<"connection_high_watermark">> => <<"50%">>
                    }
            }
        )
    ),
    ?assertMatch({ok, #{max_connections := 100}}, emqx_license_checker:limits()),
    ?assertMatch(
        #{connection_low_watermark := 0.2, connection_high_watermark := 0.5},
        emqx:get_config([license])
    ).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

mk_license(Fields) ->
    EncodedLicense = emqx_license_test_lib:make_license(Fields),
    {ok, License} = emqx_license_parser:parse(
        EncodedLicense,
        emqx_license_test_lib:public_key_pem()
    ),
    {EncodedLicense, License}.

update(License) ->
    Result = emqx_license_checker:update(License),
    sync_cache(),
    Result.

sync_cache() ->
    %% force refresh the cache
    _ = whereis(emqx_license_resources) ! update_resources,
    %% force sync with the process
    _ = sys:get_state(whereis(emqx_license_resources)),
    ok.
