%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_machine_app).

-export([ start/2
        , stop/1
        , prep_stop/1
        ]).

%% Shutdown and reboot
-export([ shutdown/1
        , ensure_apps_started/0
        ]).

-export([sorted_reboot_apps/0]).

-behaviour(application).

-include_lib("emqx/include/logger.hrl").

start(_Type, _Args) ->
    ok = set_backtrace_depth(),
    ok = print_otp_version_warning(),

    %% need to load some app envs
    %% TODO delete it once emqx boot does not depend on modules envs
    _ = load_modules(),
    ok = load_config_files(),

    {ok, RootSupPid} = emqx_machine_sup:start_link(),

    ok = ensure_apps_started(),

    _ = emqx_plugins:load(),

    ok = print_vsn(),

    ok = start_autocluster(),
    {ok, RootSupPid}.

prep_stop(_State) ->
    application:stop(emqx).

stop(_State) ->
    ok.

set_backtrace_depth() ->
    {ok, Depth} = application:get_env(emqx_machine, backtrace_depth),
    _ = erlang:system_flag(backtrace_depth, Depth),
    ok.

-if(?OTP_RELEASE > 22).
print_otp_version_warning() -> ok.
-else.
print_otp_version_warning() ->
    ?ULOG("WARNING: Running on Erlang/OTP version ~p. Recommended: 23~n",
          [?OTP_RELEASE]).
-endif. % OTP_RELEASE > 22

-ifdef(TEST).
print_vsn() -> ok.
-else. % TEST
print_vsn() ->
    ?ULOG("~s ~s is running now!~n", [emqx_app:get_description(), emqx_app:get_release()]).
-endif. % TEST

-ifndef(EMQX_ENTERPRISE).
load_modules() ->
    application:load(emqx_modules).
-else.
load_modules() ->
    ok.
-endif.

load_config_files() ->
    %% the app env 'config_files' for 'emqx` app should be set
    %% in app.time.config by boot script before starting Erlang VM
    ConfFiles = application:get_env(emqx, config_files, []),
    %% emqx_machine_schema is a superset of emqx_schema
    ok = emqx_config:init_load(emqx_machine_schema, ConfFiles),
    %% to avoid config being loaded again when emqx app starts.
    ok = emqx_app:set_init_config_load_done().

start_autocluster() ->
    ekka:callback(prepare, fun ?MODULE:shutdown/1),
    ekka:callback(reboot,  fun ?MODULE:ensure_apps_started/0),
    _ = ekka:autocluster(emqx), %% returns 'ok' or a pid or 'any()' as in spec
    ok.

shutdown(Reason) ->
    ?SLOG(critical, #{msg => "stopping_apps", reason => Reason}),
    _ = emqx_alarm_handler:unload(),
    lists:foreach(fun stop_one_app/1, lists:reverse(sorted_reboot_apps())).

stop_one_app(App) ->
    ?SLOG(debug, #{msg => "stopping_app", app => App}),
    application:stop(App).

ensure_apps_started() ->
    lists:foreach(fun start_one_app/1, sorted_reboot_apps()).

start_one_app(App) ->
    ?SLOG(debug, #{msg => "starting_app", app => App}),
    case application:ensure_all_started(App) of
        {ok, Apps} ->
            ?SLOG(debug, #{msg => "started_apps", apps => [App | Apps]});
        {error, Reason} ->
            ?SLOG(critical, #{msg => "failed_to_start_app", app => App, reason => Reason}),
            error({faile_to_start_app, App, Reason})
    end.

%% list of app names which should be rebooted when:
%% 1. due to static static config change
%% 2. after join a cluster
reboot_apps() ->
    [gproc, esockd, ranch, cowboy, ekka, quicer, emqx | ?EMQX_DEP_APPS].

%% quicer can not be added to emqx's .app because it might be opted out at build time
implicit_deps() ->
    [{emqx, [quicer]}].

sorted_reboot_apps() ->
    Apps = [{App, app_deps(App)} || App <- reboot_apps()],
    sorted_reboot_apps(Apps ++ implicit_deps()).

app_deps(App) ->
    case application:get_key(App, applications) of
        undefined -> [];
        {ok, List} -> lists:filter(fun(A) -> lists:member(A, reboot_apps()) end, List)
    end.

sorted_reboot_apps(Apps) ->
    G = digraph:new(),
    lists:foreach(fun({App, Deps}) -> add_app(G, App, Deps) end, Apps),
    case digraph_utils:topsort(G) of
        Sorted when is_list(Sorted) ->
            Sorted;
        false ->
            Loops = find_loops(G),
            error({circular_application_dependency, Loops})
    end.

add_app(G, App, undefined) ->
    ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
    %% not loaded
    add_app(G, App, []);
add_app(_G, _App, []) ->
    ok;
add_app(G, App, [Dep | Deps]) ->
    digraph:add_vertex(G, App),
    digraph:add_vertex(G, Dep),
    digraph:add_edge(G, Dep, App), %% dep -> app as dependency
    add_app(G, App, Deps).

find_loops(G) ->
    lists:filtermap(
      fun (App) ->
              case digraph:get_short_cycle(G, App) of
                  false -> false;
                  Apps -> {true, Apps}
              end
      end, digraph:vertices(G)).
