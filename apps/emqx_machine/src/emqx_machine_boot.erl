%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_machine_boot).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,

    stop_apps/0,
    ensure_apps_started/0,
    sorted_reboot_apps/0,
    stop_port_apps/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports (for `emqx_machine_terminator' only)
-export([do_stop_apps/0]).

-dialyzer({no_match, [basic_reboot_apps/0]}).

-ifdef(TEST).
-export([read_apps/0, sorted_reboot_apps/1, reboot_apps/0, start_autocluster/0]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% These apps are always (re)started by emqx_machine:
-define(BASIC_REBOOT_APPS, [gproc, esockd, ranch, cowboy, emqx_durable_storage, emqx]).

%% If any of these applications crash, the entire EMQX node shuts down:
-define(BASIC_PERMANENT_APPS, [mria, ekka, esockd, emqx]).

%% These apps are optional, they may or may not be present in the
%% release, depending on the build flags:
-define(OPTIONAL_APPS, [bcrypt, observer]).

%% calls/casts/infos
-record(start_apps, {}).
-record(stop_apps, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop_apps() ->
    gen_server:call(?MODULE, #stop_apps{}, infinity).

%% Those port apps are terminated after the main apps
%% Don't need to stop when reboot.
stop_port_apps() ->
    Loaded = application:loaded_applications(),
    lists:foreach(
        fun(App) ->
            case lists:keymember(App, 1, Loaded) of
                true -> stop_one_app(App);
                false -> ok
            end
        end,
        [os_mon, jq]
    ).

ensure_apps_started() ->
    gen_server:call(?MODULE, #start_apps{}, infinity).

sorted_reboot_apps() ->
    RebootApps = reboot_apps(),
    Apps0 = [{App, app_deps(App, RebootApps)} || App <- RebootApps],
    Apps = emqx_machine_boot_runtime_deps:inject(Apps0, runtime_deps()),
    sorted_reboot_apps(Apps).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    %% Ensure that the stop callback is set, so that join requests concurrent to the
    %% startup are serialized here.
    %% It would still be problematic if a join request arrives before this process is
    %% started, though.
    ekka:callback(stop, fun ?MODULE:stop_apps/0),
    do_start_apps(),
    ok = print_vsn(),
    ok = start_autocluster(),
    State = #{},
    {ok, State}.

handle_call(#start_apps{}, _From, State) ->
    do_start_apps(),
    {reply, ok, State};
handle_call(#stop_apps{}, _From, State) ->
    do_stop_apps(),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

%% Callers who wish to stop applications should not call this directly, and instead use
%% `stop_apps/0`.  The only exception is `emqx_machine_terminator': it should call this
%% block directly in its try-catch block, without the possibility of crashing this
%% process.
do_stop_apps() ->
    ?SLOG(notice, #{msg => "stopping_emqx_apps"}),
    _ = emqx_alarm_handler:unload(),
    ok = emqx_conf_app:unset_config_loaded(),
    ok = emqx_plugins:ensure_stopped(),
    lists:foreach(fun stop_one_app/1, lists:reverse(?MODULE:sorted_reboot_apps())).

restart_type(App) ->
    PermanentApps =
        ?BASIC_PERMANENT_APPS ++ application:get_env(emqx_machine, permanent_applications, []),
    case lists:member(App, PermanentApps) of
        true ->
            permanent;
        false ->
            temporary
    end.

start_autocluster() ->
    ekka:callback(stop, fun ?MODULE:stop_apps/0),
    ekka:callback(start, fun ?MODULE:ensure_apps_started/0),
    %% returns 'ok' or a pid or 'any()' as in spec
    _ = ekka:autocluster(emqx),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-ifdef(TEST).
print_vsn() -> ok.
% TEST
-else.
print_vsn() ->
    ?ULOG("~ts ~ts is running now!~n", [emqx_app:get_description(), emqx_app:get_release()]).
% TEST
-endif.

%% list of app names which should be rebooted when:
%% 1. due to static config change
%% 2. after join a cluster

%% the list of (re)started apps depends on release type/edition
reboot_apps() ->
    ConfigApps0 = application:get_env(emqx_machine, applications, []),
    BaseRebootApps = basic_reboot_apps(),
    ConfigApps = lists:filter(fun(App) -> not lists:member(App, BaseRebootApps) end, ConfigApps0),
    BaseRebootApps ++ ConfigApps.

basic_reboot_apps() ->
    #{
        common_business_apps := CommonBusinessApps,
        ee_business_apps := EEBusinessApps,
        ce_business_apps := CEBusinessApps
    } = read_apps(),
    EditionSpecificApps =
        case emqx_release:edition() of
            ee -> EEBusinessApps;
            ce -> CEBusinessApps;
            _ -> []
        end,
    BusinessApps = CommonBusinessApps ++ EditionSpecificApps,
    ?BASIC_REBOOT_APPS ++ (BusinessApps -- excluded_apps()).

excluded_apps() ->
    %% Optional apps _should_ be (re)started automatically, but only
    %% when they are found in the release:
    [App || App <- ?OPTIONAL_APPS, not is_app(App)].

is_app(Name) ->
    case application:load(Name) of
        ok -> true;
        {error, {already_loaded, _}} -> true;
        _ -> false
    end.

do_start_apps() ->
    ?SLOG(notice, #{msg => "(re)starting_emqx_apps"}),
    lists:foreach(fun start_one_app/1, ?MODULE:sorted_reboot_apps()),
    ?tp(emqx_machine_boot_apps_started, #{}).

start_one_app(App) ->
    ?SLOG(debug, #{msg => "starting_app", app => App}),
    case application:ensure_all_started(App, restart_type(App)) of
        {ok, Apps} ->
            ?SLOG(debug, #{msg => "started_apps", apps => Apps});
        {error, Reason} ->
            ?SLOG(critical, #{msg => "failed_to_start_app", app => App, reason => Reason}),
            error({failed_to_start_app, App, Reason})
    end.

stop_one_app(App) ->
    ?SLOG(debug, #{msg => "stopping_app", app => App}),
    try
        _ = application:stop(App)
    catch
        C:E ->
            ?SLOG(error, #{
                msg => "failed_to_stop_app",
                app => App,
                exception => C,
                reason => E
            })
    end.

app_deps(App, RebootApps) ->
    case application:get_key(App, applications) of
        undefined -> undefined;
        {ok, List} -> lists:filter(fun(A) -> lists:member(A, RebootApps) end, List)
    end.

%% @doc Read business apps belonging to the current profile/edition.
read_apps() ->
    PrivDir = code:priv_dir(emqx_machine),
    RebootListPath = filename:join([PrivDir, "reboot_lists.eterm"]),
    {ok, [Apps]} = file:consult(RebootListPath),
    Apps.

runtime_deps() ->
    [
        %% `emqx_bridge' is special in that it needs all the bridges apps to
        %% be started before it, so that, when it loads the bridges from
        %% configuration, the bridge app and its dependencies need to be up.
        {emqx_bridge, fun(App) -> lists:prefix("emqx_bridge_", atom_to_list(App)) end},
        %% `emqx_connector' also needs to start all connector dependencies for the same reason.
        %% Since standalone apps like `emqx_mongodb' are already dependencies of `emqx_bridge_*'
        %% apps, we may apply the same tactic for `emqx_connector' and inject individual bridges
        %% as its dependencies.
        {emqx_connector, fun(App) -> lists:prefix("emqx_bridge_", atom_to_list(App)) end},
        %% emqx_fdb_ds is an EE app
        {emqx_durable_storage, emqx_fdb_ds},
        %% emqx_ds_builtin is an EE app
        {emqx_ds_backends, emqx_ds_builtin_raft},
        %% emqx_ds_fdb_backend is an EE app
        {emqx_ds_backends, emqx_ds_fdb_backend},
        {emqx_dashboard, emqx_license}
    ].

sorted_reboot_apps(Apps) ->
    G = digraph:new(),
    try
        NoDepApps = add_apps_to_digraph(G, Apps),
        case digraph_utils:topsort(G) of
            Sorted when is_list(Sorted) ->
                %% ensure emqx_conf boot up first
                AllApps = Sorted ++ (NoDepApps -- Sorted),
                [emqx_conf | lists:delete(emqx_conf, AllApps)];
            false ->
                Loops = find_loops(G),
                error({circular_application_dependency, Loops})
        end
    after
        digraph:delete(G)
    end.

%% Build a dependency graph from the provided application list.
%% Return top-sort result of the apps.
%% Isolated apps without which are not dependency of any other apps are
%% put to the end of the list in the original order.
add_apps_to_digraph(G, Apps) ->
    lists:foldl(
        fun
            ({App, undefined}, Acc) ->
                ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
                Acc;
            ({App, []}, Acc) ->
                %% use '++' to keep the original order
                Acc ++ [App];
            ({App, Deps}, Acc) ->
                add_app_deps_to_digraph(G, App, Deps),
                Acc
        end,
        [],
        Apps
    ).

add_app_deps_to_digraph(G, App, undefined) ->
    ?SLOG(debug, #{msg => "app_is_not_loaded", app => App}),
    %% not loaded
    add_app_deps_to_digraph(G, App, []);
add_app_deps_to_digraph(_G, _App, []) ->
    ok;
add_app_deps_to_digraph(G, App, [Dep | Deps]) ->
    digraph:add_vertex(G, App),
    digraph:add_vertex(G, Dep),
    %% dep -> app as dependency
    digraph:add_edge(G, Dep, App),
    add_app_deps_to_digraph(G, App, Deps).

find_loops(G) ->
    lists:filtermap(
        fun(App) ->
            case digraph:get_short_cycle(G, App) of
                false -> false;
                Apps -> {true, Apps}
            end
        end,
        digraph:vertices(G)
    ).
