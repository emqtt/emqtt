%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins).

-include("emqx.hrl").
-include("logger.hrl").


-export([ load/0
        , load/1
        , unload/0
        , unload/1
        , reload/1
        , list/0
        , find_plugin/1
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Load all plugins when the broker started.
-spec(load() -> ok | ignore | {error, term()}).
load() ->
    ok = load_ext_plugins(emqx:get_config([plugins, expand_plugins_dir], undefined)).

%% @doc Load a Plugin
-spec(load(atom()) -> ok | {error, term()}).
load(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?SLOG(alert, #{msg => "failed_to_load_plugin",
                           plugin_name => PluginName,
                           reason => not_found}),
            {error, not_found};
        {_, true} ->
            ?SLOG(notice, #{msg => "plugin_already_loaded",
                            plugin_name => PluginName,
                            reason => already_loaded}),
            {error, already_started};
        {_, false} ->
            load_plugin(PluginName)
    end.

%% @doc Unload all plugins before broker stopped.
-spec(unload() -> ok).
unload() ->
    stop_plugins(list()).

%% @doc UnLoad a Plugin
-spec(unload(atom()) -> ok | {error, term()}).
unload(PluginName) when is_atom(PluginName) ->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?SLOG(error, #{msg => "fialed_to_unload_plugin",
                           plugin_name => PluginName,
                           reason => not_found}),
            {error, not_found};
        {_, false} ->
            ?SLOG(error, #{msg => "failed_to_unload_plugin",
                           plugin_name => PluginName,
                           reason => not_loaded}),
            {error, not_started};
        {_, _} ->
            unload_plugin(PluginName)
    end.

reload(PluginName) when is_atom(PluginName)->
    case {lists:member(PluginName, names(plugin)), lists:member(PluginName, names(started_app))} of
        {false, _} ->
            ?SLOG(error, #{msg => "failed_to_reload_plugin",
                           plugin_name => PluginName,
                           reason => not_found}),
            {error, not_found};
        {_, false} ->
            load(PluginName);
        {_, true} ->
            case unload(PluginName) of
                ok -> load(PluginName);
                {error, Reason} -> {error, Reason}
            end
    end.

%% @doc List all available plugins
-spec(list() -> [emqx_types:plugin()]).
list() ->
    StartedApps = names(started_app),
    lists:map(fun({Name, _, _}) ->
        Plugin = plugin(Name),
        case lists:member(Name, StartedApps) of
            true  -> Plugin#plugin{active = true};
            false -> Plugin
        end
    end, lists:sort(ekka_boot:all_module_attributes(emqx_plugin))).

find_plugin(Name) ->
    find_plugin(Name, list()).

find_plugin(Name, Plugins) ->
    lists:keyfind(Name, 2, Plugins).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% load external plugins which are placed in etc/plugins dir
load_ext_plugins(undefined) -> ok;
load_ext_plugins(Dir) ->
    lists:foreach(
        fun(Plugin) ->
                PluginDir = filename:join(Dir, Plugin),
                case filelib:is_dir(PluginDir) of
                    true  -> load_ext_plugin(PluginDir);
                    false -> ok
                end
        end, filelib:wildcard("*", Dir)).

load_ext_plugin(PluginDir) ->
    ?SLOG(debug, #{msg => "loading_extra_plugin", plugin_dir => PluginDir}),
    Ebin = filename:join([PluginDir, "ebin"]),
    AppFile = filename:join([Ebin, "*.app"]),
    AppName = case filelib:wildcard(AppFile) of
                  [App] ->
                      list_to_atom(filename:basename(App, ".app"));
                  [] ->
                      ?SLOG(alert, #{msg => "plugin_app_file_not_found", app_file => AppFile}),
                      error({plugin_app_file_not_found, AppFile})
              end,
    ok = load_plugin_app(AppName, Ebin).
    % try
    %     ok = generate_configs(AppName, PluginDir)
    % catch
    %     throw : {conf_file_not_found, ConfFile} ->
    %         %% this is maybe a dependency of an external plugin
    %         ?LOG(debug, "config_load_error_ignored for app=~p, path=~ts", [AppName, ConfFile]),
    %         ok
    % end.

load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    lists:foreach(
        fun(BeamFile) ->
                Module = list_to_atom(filename:basename(BeamFile, ".beam")),
                case code:load_file(Module) of
                    {module, _} -> ok;
                    {error, Reason} -> error({failed_to_load_plugin_beam, BeamFile, Reason})
                end
        end, Modules),
    case application:load(AppName) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.

%% Stop plugins
stop_plugins(Plugins) ->
    _ = [stop_app(Plugin#plugin.name) || Plugin <- Plugins],
    ok.

plugin(AppName) ->
    case application:get_all_key(AppName) of
        {ok, Attrs} ->
            Descr = proplists:get_value(description, Attrs, ""),
            #plugin{name = AppName, descr = Descr};
        undefined -> error({plugin_not_found, AppName})
    end.

load_plugin(Name) ->
    try
        case load_app(Name) of
            ok ->
                start_app(Name);
            {error, Error0} ->
                {error, Error0}
        end
    catch Error : Reason : Stacktrace ->
        ?SLOG(alert, #{
            msg => "plugin_load_failed",
            name => Name,
            exception => Error,
            reason => Reason,
            stacktrace => Stacktrace
        }),
        {error, parse_config_file_failed}
    end.

load_app(App) ->
    case application:load(App) of
        ok ->
            ok;
        {error, {already_loaded, App}} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

start_app(App) ->
    case application:ensure_all_started(App) of
        {ok, Started} ->
            case Started =/= [] of
                true -> ?SLOG(info, #{msg => "started_plugin_dependency_apps", apps => Started});
                false -> ok
            end,
            ?SLOG(info, #{msg => "started_plugin_app", app => App}),
            ok;
        {error, {ErrApp, Reason}} ->
            ?SLOG(error, #{msg => failed_to_start_plugin_app,
                           app => App,
                           err_app => ErrApp,
                           reason => Reason
                          }),
            {error, failed_to_start_plugin_app}
    end.

unload_plugin(App) ->
    case stop_app(App) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(info, #{msg => "stop_plugin_successfully", app => App}),
            ok;
        {error, {not_started, App}} ->
            ?SLOG(info, #{msg => "plugin_not_started", app => App}),
            ok;
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_stop_plugin_app",
                           app => App,
                           error => Reason
                          }),
            {error, Reason}
    end.

names(plugin) ->
    names(list());

names(started_app) ->
    [Name || {Name, _Descr, _Ver} <- application:which_applications()];

names(Plugins) ->
    [Name || #plugin{name = Name} <- Plugins].
