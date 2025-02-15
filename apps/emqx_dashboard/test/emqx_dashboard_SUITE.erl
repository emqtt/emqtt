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

-module(emqx_dashboard_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(
    emqx_common_test_http,
    [
        request_api/3,
        request_api/5,
        get_http_data/1
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_dashboard.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083").

-define(BASE_PATH, "/api/v5").

-define(APP_DASHBOARD, emqx_dashboard).
-define(APP_MANAGEMENT, emqx_management).

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

init_per_suite(Config) ->
    %% Load all applications to ensure swagger.json is fully generated.
    Apps = emqx_machine_boot:reboot_apps(),
    ct:pal("load apps:~p~n", [Apps]),
    lists:foreach(fun(App) -> application:load(App) end, Apps),
    SuiteApps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    _ = emqx_conf_schema:roots(),
    ok = emqx_dashboard_desc_cache:init(),
    emqx_dashboard:save_dispatch_eterm(emqx_conf:schema_module()),
    emqx_common_test_http:create_default_app(),
    [{suite_apps, SuiteApps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

stop_dashboard() ->
    application:stop(emqx_dashboard),
    clear_swagger_spec().

clear_swagger_spec() ->
    %% Perf hack: when restarting `emqx_dashboard' and regenerating dispatch, one source
    %% of slowness is currently in `cowboy_swagger': loading the existing spec,
    %% normalizing it to a JSON term (it's already normalized), then adding the
    %% definitions (already JSON terms, but get normalized) _twice_, merging it with
    %% existing spec, then storing it back to application env, not before normalizing the
    %% result yet again.
    %% By clearing this env parameter, we save some time in the tests.
    application:unset_env(cowboy_swagger, global_spec).

start_dashboard() ->
    application:start(emqx_dashboard).

t_overview(_) ->
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        <<"admin">>, <<"public_www1">>, ?ROLE_SUPERUSER, <<"simple_description">>
    ),
    Headers = auth_header_(<<"admin">>, <<"public_www1">>),
    [
        {ok, _} = request_dashboard(get, api_path([Overview]), Headers)
     || Overview <- ?OVERVIEWS
    ].

t_dashboard_restart(Config) ->
    emqx_config:put([dashboard], #{
        i18n_lang => en,
        swagger_support => true,
        listeners =>
            #{
                http =>
                    #{
                        inet6 => false,
                        bind => 18083,
                        ipv6_v6only => false,
                        send_timeout => 10000,
                        num_acceptors => 8,
                        max_connections => 512,
                        backlog => 1024,
                        proxy_header => false
                    }
            }
    }),
    stop_dashboard(),
    start_dashboard(),
    Name = 'http:dashboard',
    t_overview(Config),
    [{'_', [], Rules}] = Dispatch = persistent_term:get(Name),
    %% complete dispatch has more than 150 rules.
    ?assertNotMatch([{[], [], cowboy_static, _} | _], Rules),
    ?assert(erlang:length(Rules) > 150),
    CheckRules = fun(Tag) ->
        [{'_', [], NewRules}] = persistent_term:get(Name, Tag),
        ?assertEqual(length(Rules), length(NewRules), Tag),
        ?assertEqual(lists:sort(Rules), lists:sort(NewRules), Tag)
    end,
    ?check_trace(
        ?wait_async_action(
            begin
                stop_dashboard(),
                ?assertEqual(Dispatch, persistent_term:get(Name)),
                start_dashboard(),
                %% After we restart the dashboard, the dispatch rules should be the same.
                CheckRules(step_1)
            end,
            #{?snk_kind := regenerate_minirest_dispatch},
            30_000
        ),
        fun(Trace) ->
            ?assertMatch([#{i18n_lang := en}], ?of_kind(regenerate_minirest_dispatch, Trace)),
            %% The dispatch is updated after being regenerated.
            CheckRules(step_2)
        end
    ),
    t_overview(Config),
    ?check_trace(
        ?wait_async_action(
            begin
                %% erase to mock the initial dashboard startup.
                persistent_term:erase(Name),
                stop_dashboard(),
                start_dashboard(),
                ct:sleep(800),
                %% regenerate the dispatch rules again
                CheckRules(step_3)
            end,
            #{?snk_kind := regenerate_minirest_dispatch},
            30_000
        ),
        fun(Trace) ->
            ?assertMatch([#{i18n_lang := en}], ?of_kind(regenerate_minirest_dispatch, Trace)),
            CheckRules(step_4)
        end
    ),
    t_overview(Config),
    ok.

t_admins_add_delete(_) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"simple description">>,
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"username">>, <<"password_0">>, ?ROLE_SUPERUSER, Desc
    ),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"username1">>, <<"password1">>, ?ROLE_SUPERUSER, Desc
    ),
    Admins = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(Admins)),
    {ok, _} = emqx_dashboard_admin:remove_user(<<"username1">>),
    Users = emqx_dashboard_admin:all_users(),
    ?assertEqual(1, length(Users)),
    {ok, _} = emqx_dashboard_admin:change_password(
        <<"username">>,
        <<"password_0">>,
        <<"new_pwd_1234">>
    ),
    timer:sleep(10),
    {ok, _} = emqx_dashboard_admin:remove_user(<<"username">>).

t_admin_delete_self_failed(_) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"simple description">>,
    _ = emqx_dashboard_admin:add_user(<<"username1">>, <<"password_1">>, ?ROLE_SUPERUSER, Desc),
    Admins = emqx_dashboard_admin:all_users(),
    ?assertEqual(1, length(Admins)),
    Header = auth_header_(<<"username1">>, <<"password_1">>),
    {error, {_, 400, _}} = request_dashboard(delete, api_path(["users", "username1"]), Header),
    Token = ["Basic ", base64:encode("username1:password_1")],
    Header2 = {"Authorization", Token},
    {error, {_, 401, _}} = request_dashboard(delete, api_path(["users", "username1"]), Header2),
    mnesia:clear_table(?ADMIN).

t_rest_api(_Config) ->
    mnesia:clear_table(?ADMIN),
    Desc = <<"administrator">>,
    Password = <<"public_www1">>,
    emqx_dashboard_admin:add_user(<<"admin">>, Password, ?ROLE_SUPERUSER, Desc),
    {ok, 200, Res0} = http_get(["users"]),
    ?assertEqual(
        [
            filter_req(#{
                <<"backend">> => <<"local">>,
                <<"username">> => <<"admin">>,
                <<"description">> => <<"administrator">>,
                <<"role">> => ?ROLE_SUPERUSER
            })
        ],
        get_http_data(Res0)
    ),
    {ok, 200, _} = http_put(
        ["users", "admin"],
        filter_req(#{
            <<"role">> => ?ROLE_SUPERUSER,
            <<"description">> => <<"a_new_description">>
        })
    ),
    {ok, 200, _} = http_post(
        ["users"],
        filter_req(#{
            <<"username">> => <<"usera">>,
            <<"password">> => <<"passwd_01234">>,
            <<"role">> => ?ROLE_SUPERUSER,
            <<"description">> => Desc
        })
    ),
    {ok, 204, _} = http_delete(["users", "usera"]),
    {ok, 404, _} = http_delete(["users", "usera"]),
    {ok, 204, _} = http_post(
        ["users", "admin", "change_pwd"],
        #{
            <<"old_pwd">> => Password,
            <<"new_pwd">> => <<"newpwd_lkdfki1">>
        }
    ),
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(<<"admin">>, Password, ?ROLE_SUPERUSER, <<"administrator">>),
    ok.

t_swagger_json(_Config) ->
    Url = ?HOST ++ "/api-docs/swagger.json",
    %% with auth
    Auth = auth_header_(<<"admin">>, <<"public_www1">>),
    {ok, 200, Body1} = request_api(get, Url, Auth),
    ?assert(emqx_utils_json:is_json(Body1)),
    %% without auth
    {ok, {{"HTTP/1.1", 200, "OK"}, _Headers, Body2}} =
        httpc:request(get, {Url, []}, [], [{body_format, binary}]),
    ?assertEqual(Body1, Body2),
    ?assertMatch(
        #{
            <<"info">> := #{
                <<"title">> := _,
                <<"version">> := _
            }
        },
        emqx_utils_json:decode(Body1)
    ),
    ok.

t_disable_swagger_json(_Config) ->
    Url = ?HOST ++ "/api-docs/index.html",

    ?assertMatch(
        {ok, {{"HTTP/1.1", 200, "OK"}, __, _}},
        httpc:request(get, {Url, []}, [], [{body_format, binary}])
    ),
    DashboardCfg = emqx:get_raw_config([dashboard]),

    ?check_trace(
        ?wait_async_action(
            begin
                clear_swagger_spec(),
                DashboardCfg2 = DashboardCfg#{<<"swagger_support">> => false},
                emqx:update_config([dashboard], DashboardCfg2)
            end,
            #{?snk_kind := regenerate_minirest_dispatch},
            30_000
        ),
        fun(Trace) ->
            ?assertMatch([#{i18n_lang := en}], ?of_kind(regenerate_minirest_dispatch, Trace)),
            ?assertMatch(
                {ok, {{"HTTP/1.1", 404, "Not Found"}, _, _}},
                httpc:request(get, {Url, []}, [], [{body_format, binary}])
            )
        end
    ),
    ?check_trace(
        ?wait_async_action(
            begin
                clear_swagger_spec(),
                DashboardCfg3 = DashboardCfg#{<<"swagger_support">> => true},
                emqx:update_config([dashboard], DashboardCfg3)
            end,
            #{?snk_kind := regenerate_minirest_dispatch},
            30_000
        ),
        fun(Trace) ->
            ?assertMatch([#{i18n_lang := en}], ?of_kind(regenerate_minirest_dispatch, Trace)),
            ?assertMatch(
                {ok, {{"HTTP/1.1", 200, "OK"}, __, _}},
                httpc:request(get, {Url, []}, [], [{body_format, binary}])
            )
        end
    ),
    ok.

t_cli(_Config) ->
    [mria:dirty_delete(?ADMIN, Admin) || Admin <- mnesia:dirty_all_keys(?ADMIN)],
    emqx_dashboard_cli:admins(["add", "username", "password_ww2"]),
    [#?ADMIN{username = <<"username">>, pwdhash = <<Salt:4/binary, Hash/binary>>}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(Hash, crypto:hash(sha256, <<Salt/binary, <<"password_ww2">>/binary>>)),
    emqx_dashboard_cli:admins(["passwd", "username", "new_password"]),
    [#?ADMIN{username = <<"username">>, pwdhash = <<Salt1:4/binary, Hash1/binary>>}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(Hash1, crypto:hash(sha256, <<Salt1/binary, <<"new_password">>/binary>>)),
    emqx_dashboard_cli:admins(["del", "username"]),
    [] = emqx_dashboard_admin:lookup_user(<<"username">>),
    emqx_dashboard_cli:admins(["add", "admin1", "pass_lkdfkd1"]),
    emqx_dashboard_cli:admins(["add", "admin2", "w_pass_lkdfkd2"]),
    AdminList = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(AdminList)).

t_lookup_by_username_jwt(_Config) ->
    User = bin(["user-", integer_to_list(random_num())]),
    Pwd = bin("t_password" ++ integer_to_list(random_num())),
    emqx_dashboard_token:sign(#?ADMIN{username = User}, Pwd),
    ?assertMatch(
        [#?ADMIN_JWT{username = User}],
        emqx_dashboard_token:lookup_by_username(User)
    ),
    ok = emqx_dashboard_token:destroy_by_username(User),
    %% issue a gen_server call to sync the async destroy gen_server cast
    ok = gen_server:call(emqx_dashboard_token, dummy, infinity),
    ?assertMatch([], emqx_dashboard_token:lookup_by_username(User)),
    ok.

t_clean_expired_jwt(_Config) ->
    User = bin(["user-", integer_to_list(random_num())]),
    Pwd = bin("t_password" ++ integer_to_list(random_num())),
    emqx_dashboard_token:sign(#?ADMIN{username = User}, Pwd),
    [#?ADMIN_JWT{username = User, exptime = ExpTime}] =
        emqx_dashboard_token:lookup_by_username(User),
    ok = emqx_dashboard_token:clean_expired_jwt(_Now1 = ExpTime),
    ?assertMatch(
        [#?ADMIN_JWT{username = User}],
        emqx_dashboard_token:lookup_by_username(User)
    ),
    ok = emqx_dashboard_token:clean_expired_jwt(_Now2 = ExpTime + 1),
    ?assertMatch([], emqx_dashboard_token:lookup_by_username(User)),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

random_num() ->
    erlang:system_time(nanosecond).

http_get(Parts) ->
    request_api(get, api_path(Parts), auth_header_(<<"admin">>, <<"public_www1">>)).

http_delete(Parts) ->
    request_api(delete, api_path(Parts), auth_header_(<<"admin">>, <<"public_www1">>)).

http_post(Parts, Body) ->
    request_api(post, api_path(Parts), [], auth_header_(<<"admin">>, <<"public_www1">>), Body).

http_put(Parts, Body) ->
    request_api(put, api_path(Parts), [], auth_header_(<<"admin">>, <<"public_www1">>), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).

do_request_dashboard(Method, {Url, _} = Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, maybe_ssl(Url), []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code >= 200 andalso Code =< 299
        ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason};
        {error, Reason} ->
            {error, Reason}
    end.

maybe_ssl("http://" ++ _) -> [];
maybe_ssl("https://" ++ _) -> [{ssl, [{verify, verify_none}]}].

auth_header_() ->
    auth_header_(<<"admin">>, <<"public">>).

auth_header_(Username, Password) ->
    {ok, _Role, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

api_path(Parts) ->
    ?HOST ++ filename:join([?BASE_PATH | Parts]).

json(Data) ->
    {ok, Jsx} = emqx_utils_json:safe_decode(Data, [return_maps]),
    Jsx.

-if(?EMQX_RELEASE_EDITION == ee).
filter_req(Req) ->
    Req.

-else.

filter_req(Req) ->
    maps:without([role, <<"role">>, backend, <<"backend">>], Req).

-endif.
