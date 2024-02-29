%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_http_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_license_test_lib:mock_parser(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{
                    license => #{
                        key => emqx_license_test_lib:make_license(#{max_connections => "100"}),
                        connection_low_watermark => <<"75%">>,
                        connection_high_watermark => <<"80%">>
                    }
                }
            }},
            {emqx_dashboard,
                "dashboard {"
                "\n  listeners.http { enable = true, bind = 18083 }"
                "\n  default_username = \"license_admin\""
                "\n}"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_license_test_lib:unmock_parser(),
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    {ok, _} = reset_license(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

request(Method, Uri, Body) ->
    emqx_dashboard_api_test_helpers:request(<<"license_admin">>, Method, Uri, Body).

uri(Segments) ->
    emqx_dashboard_api_test_helpers:uri(Segments).

get_license() ->
    maps:from_list(emqx_license_checker:dump()).

default_license() ->
    emqx_license_test_lib:make_license(#{max_connections => "100"}).

reset_license() ->
    emqx_license:update_key(default_license()).

assert_untouched_license() ->
    ?assertMatch(
        #{max_connections := 100},
        get_license()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_license_info(_Config) ->
    Res = request(get, uri(["license"]), []),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"customer">> => <<"Foo">>,
            <<"customer_type">> => 10,
            <<"deployment">> => <<"bar-deployment">>,
            <<"email">> => <<"contact@foo.com">>,
            <<"expiry">> => false,
            <<"expiry_at">> => <<"2295-10-27">>,
            <<"max_connections">> => 100,
            <<"start_at">> => <<"2022-01-11">>,
            <<"type">> => <<"trial">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    ok.

t_set_default_license(_Config) ->
    NewKey = <<"default">>,
    Res = request(
        post,
        uri(["license"]),
        #{key => NewKey}
    ),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    %% assert that it's not the string "default" returned
    ?assertMatch(#{<<"customer">> := _}, emqx_utils_json:decode(Payload, [return_maps])),
    ok.

t_license_upload_key_success(_Config) ->
    NewKey = emqx_license_test_lib:make_license(#{max_connections => "999"}),
    Res = request(
        post,
        uri(["license"]),
        #{key => NewKey}
    ),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"customer">> => <<"Foo">>,
            <<"customer_type">> => 10,
            <<"deployment">> => <<"bar-deployment">>,
            <<"email">> => <<"contact@foo.com">>,
            <<"expiry">> => false,
            <<"expiry_at">> => <<"2295-10-27">>,
            <<"max_connections">> => 999,
            <<"start_at">> => <<"2022-01-11">>,
            <<"type">> => <<"trial">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    ?assertMatch(
        #{max_connections := 999},
        get_license()
    ),
    ok.

t_license_upload_key_bad_key(_Config) ->
    BadKey = <<"bad key">>,
    Res = request(
        post,
        uri(["license"]),
        #{key => BadKey}
    ),
    ?assertMatch({ok, 400, _}, Res),
    {ok, 400, Payload} = Res,
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Bad license key">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    assert_untouched_license(),
    ok.

t_license_upload_key_not_json(_Config) ->
    Res = request(
        post,
        uri(["license"]),
        <<"">>
    ),
    ?assertMatch({ok, 400, _}, Res),
    {ok, 400, Payload} = Res,
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Invalid request params">>
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ),
    assert_untouched_license(),
    ok.

t_license_setting(_Config) ->
    %% get
    GetRes = request(get, uri(["license", "setting"]), []),
    validate_setting(GetRes, <<"75%">>, <<"80%">>),
    %% update
    Low = <<"50%">>,
    High = <<"55%">>,
    UpdateRes = request(put, uri(["license", "setting"]), #{
        <<"connection_low_watermark">> => Low,
        <<"connection_high_watermark">> => High
    }),
    validate_setting(UpdateRes, Low, High),
    ?assertEqual(0.5, emqx_config:get([license, connection_low_watermark])),
    ?assertEqual(0.55, emqx_config:get([license, connection_high_watermark])),

    %% update
    Low1 = <<"50.12%">>,
    High1 = <<"100%">>,
    UpdateRes1 = request(put, uri(["license", "setting"]), #{
        <<"connection_low_watermark">> => Low1,
        <<"connection_high_watermark">> => High1
    }),
    validate_setting(UpdateRes1, Low1, High1),
    ?assertEqual(0.5012, emqx_config:get([license, connection_low_watermark])),
    ?assertEqual(1.0, emqx_config:get([license, connection_high_watermark])),

    %% update bad setting low >= high
    ?assertMatch(
        {ok, 400, _},
        request(put, uri(["license", "setting"]), #{
            <<"connection_low_watermark">> => <<"50%">>,
            <<"connection_high_watermark">> => <<"50%">>
        })
    ),
    ?assertMatch(
        {ok, 400, _},
        request(put, uri(["license", "setting"]), #{
            <<"connection_low_watermark">> => <<"51%">>,
            <<"connection_high_watermark">> => <<"50%">>
        })
    ),
    ok.

validate_setting(Res, ExpectLow, ExpectHigh) ->
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Payload} = Res,
    ?assertEqual(
        #{
            <<"connection_low_watermark">> => ExpectLow,
            <<"connection_high_watermark">> => ExpectHigh
        },
        emqx_utils_json:decode(Payload, [return_maps])
    ).
