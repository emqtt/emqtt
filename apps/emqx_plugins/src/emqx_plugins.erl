%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-feature(maybe_expr, enable).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    describe/1,
    plugin_schema_json/1,
    plugin_i18n_json/1,
    raw_plugin_config_content/1,
    parse_name_vsn/1,
    make_name_vsn_string/2
]).

%% Package operations
-export([
    ensure_installed/0,
    ensure_installed/1,
    ensure_installed/2,
    ensure_uninstalled/1,
    ensure_enabled/1,
    ensure_enabled/2,
    ensure_enabled/3,
    ensure_disabled/1,
    purge/1,
    delete_package/1
]).

%% Plugin runtime management
-export([
    ensure_started/0,
    ensure_started/1,
    ensure_stopped/0,
    ensure_stopped/1,
    restart/1,
    list/0,
    list/1
]).

%% Plugin config APIs
-export([
    get_config/1,
    get_config/2,
    get_config/3,
    get_config/4,
    put_config/3
]).

%% Package utils
-export([
    decode_plugin_config_map/2,
    install_dir/0,
    avsc_file_path/1,
    md5sum_file/1,
    with_plugin_avsc/1,
    ensure_ssl_files/2,
    ensure_ssl_files/3
]).

%% `emqx_config_handler' API
-export([
    post_config_update/5
]).

%% RPC call
-export([get_tar/1]).

%% Internal export
-export([
    ensure_config_map/1,
    do_ensure_started/1
]).
%% for test cases
-export([put_config_internal/2]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%% Defines
-define(PLUGIN_PERSIS_CONFIG_KEY(NameVsn), {?MODULE, NameVsn}).

-define(RAW_BIN, binary).
-define(JSON_MAP, json_map).

-define(MAX_KEEP_BACKUP_CONFIGS, 10).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Describe a plugin.
-spec describe(name_vsn()) -> {ok, plugin_info()} | {error, any()}.
describe(NameVsn) ->
    read_plugin_info(NameVsn, #{fill_readme => true}).

-spec plugin_schema_json(name_vsn()) -> {ok, schema_json_map()} | {error, any()}.
plugin_schema_json(NameVsn) ->
    read_plugin_avsc(NameVsn).

-spec plugin_i18n_json(name_vsn()) -> {ok, i18n_json_map()} | {error, any()}.
plugin_i18n_json(NameVsn) ->
    read_plugin_i18n(NameVsn).

-spec raw_plugin_config_content(name_vsn()) -> {ok, raw_plugin_config_content()} | {error, any()}.
raw_plugin_config_content(NameVsn) ->
    read_plugin_hocon(NameVsn).

parse_name_vsn(NameVsn) when is_binary(NameVsn) ->
    parse_name_vsn(binary_to_list(NameVsn));
parse_name_vsn(NameVsn) when is_list(NameVsn) ->
    case lists:splitwith(fun(X) -> X =/= $- end, NameVsn) of
        {AppName, [$- | Vsn]} -> {ok, list_to_atom(AppName), Vsn};
        _ -> {error, "bad_name_vsn"}
    end.

make_name_vsn_string(Name, Vsn) ->
    binary_to_list(iolist_to_binary([Name, "-", Vsn])).

app_dir(AppName, Apps) ->
    case
        lists:filter(
            fun(AppNameVsn) -> nomatch =/= string:prefix(AppNameVsn, AppName) end,
            Apps
        )
    of
        [AppNameVsn] ->
            {ok, AppNameVsn};
        _ ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Package operations

%% @doc Start all configured plugins are started.
-spec ensure_installed() -> ok.
ensure_installed() ->
    Fun = fun(#{name_vsn := NameVsn}) ->
        case ensure_installed(NameVsn) of
            ok -> [];
            {error, Reason} -> [{NameVsn, Reason}]
        end
    end,
    ok = for_plugins(Fun).

%% @doc Install a .tar.gz package placed in install_dir.
-spec ensure_installed(name_vsn()) -> ok | {error, map()}.
ensure_installed(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, _} ->
            ok,
            _ = maybe_ensure_plugin_config(NameVsn, ?normal);
        {error, _} ->
            ok = purge(NameVsn),
            case ensure_exists_and_installed(NameVsn) of
                ok ->
                    maybe_post_op_after_installed(NameVsn, ?normal),
                    ok;
                {error, _Reason} = Err ->
                    Err
            end
    end.

ensure_installed(NameVsn, ?fresh_install = Mode) ->
    case ensure_exists_and_installed(NameVsn) of
        ok ->
            maybe_post_op_after_installed(NameVsn, Mode),
            ok;
        {error, _Reason} = Err ->
            Err
    end.

%% @doc Ensure files and directories for the given plugin are being deleted.
%% If a plugin is running, or enabled, an error is returned.
-spec ensure_uninstalled(name_vsn()) -> ok | {error, any()}.
ensure_uninstalled(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{running_status := RunningSt}} when RunningSt =/= stopped ->
            {error, #{
                msg => "bad_plugin_running_status",
                hint => "stop_the_plugin_first"
            }};
        {ok, #{config_status := enabled}} ->
            {error, #{
                msg => "bad_plugin_config_status",
                hint => "disable_the_plugin_first"
            }};
        _ ->
            purge(NameVsn),
            ensure_delete(NameVsn)
    end.

%% @doc Ensure a plugin is enabled to the end of the plugins list.
-spec ensure_enabled(name_vsn()) -> ok | {error, any()}.
ensure_enabled(NameVsn) ->
    ensure_enabled(NameVsn, no_move).

%% @doc Ensure a plugin is enabled at the given position of the plugin list.
-spec ensure_enabled(name_vsn(), position()) -> ok | {error, any()}.
ensure_enabled(NameVsn, Position) ->
    ensure_state(NameVsn, Position, _Enabled = true, _ConfLocation = local).

-spec ensure_enabled(name_vsn(), position(), local | global) -> ok | {error, any()}.
ensure_enabled(NameVsn, Position, ConfLocation) when
    ConfLocation =:= local; ConfLocation =:= global
->
    ensure_state(NameVsn, Position, _Enabled = true, ConfLocation).

%% @doc Ensure a plugin is disabled.
-spec ensure_disabled(name_vsn()) -> ok | {error, any()}.
ensure_disabled(NameVsn) ->
    ensure_state(NameVsn, no_move, false, _ConfLocation = local).

%% @doc Delete extracted dir
%% In case one lib is shared by multiple plugins.
%% it might be the case that purging one plugin's install dir
%% will cause deletion of loaded beams.
%% It should not be a problem, because shared lib should
%% reside in all the plugin install dirs.
-spec purge(name_vsn()) -> ok.
purge(NameVsn) ->
    _ = maybe_purge_plugin_config(NameVsn),
    purge_plugin(NameVsn).

%% @doc Delete the package file.
-spec delete_package(name_vsn()) -> ok.
delete_package(NameVsn) ->
    File = pkg_file_path(NameVsn),
    _ = emqx_plugins_serde:delete_schema(NameVsn),
    case file:delete(File) of
        ok ->
            ?SLOG(info, #{msg => "purged_plugin_dir", path => File}),
            ok;
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_delete_package_file",
                path => File,
                reason => Reason
            }),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Plugin runtime management

%% @doc Start all configured plugins are started.
-spec ensure_started() -> ok.
ensure_started() ->
    Fun = fun
        (#{name_vsn := NameVsn, enable := true}) ->
            case do_ensure_started(NameVsn) of
                ok -> [];
                {error, Reason} -> [{NameVsn, Reason}]
            end;
        (#{name_vsn := NameVsn, enable := false}) ->
            ?SLOG(debug, #{msg => "plugin_disabled", name_vsn => NameVsn}),
            []
    end,
    ok = for_plugins(Fun).

%% @doc Start a plugin from Management API or CLI.
%% the input is a <name>-<vsn> string.
-spec ensure_started(name_vsn()) -> ok | {error, term()}.
ensure_started(NameVsn) ->
    case do_ensure_started(NameVsn) of
        ok ->
            ok;
        {error, ReasonMap} ->
            ?SLOG(error, ReasonMap#{msg => "failed_to_start_plugin"}),
            {error, ReasonMap}
    end.

%% @doc Stop all plugins before broker stops.
-spec ensure_stopped() -> ok.
ensure_stopped() ->
    Fun = fun
        (#{name_vsn := NameVsn, enable := true}) ->
            case ensure_stopped(NameVsn) of
                ok ->
                    [];
                {error, Reason} ->
                    [{NameVsn, Reason}]
            end;
        (#{name_vsn := NameVsn, enable := false}) ->
            ?SLOG(debug, #{msg => "plugin_disabled", action => stop_plugin, name_vsn => NameVsn}),
            []
    end,
    ok = for_plugins(Fun).

%% @doc Stop a plugin from Management API or CLI.
-spec ensure_stopped(name_vsn()) -> ok | {error, term()}.
ensure_stopped(NameVsn) ->
    tryit(
        "stop_plugin",
        fun() ->
            Plugin = do_read_plugin(NameVsn),
            ensure_apps_stopped(Plugin)
        end
    ).

get_config(Name, Vsn, Opt, Default) ->
    get_config(make_name_vsn_string(Name, Vsn), Opt, Default).

-spec get_config(name_vsn()) ->
    {ok, plugin_config_map() | any()}
    | {error, term()}.
get_config(NameVsn) ->
    get_config(NameVsn, ?CONFIG_FORMAT_MAP, #{}).

-spec get_config(name_vsn(), ?CONFIG_FORMAT_MAP | ?CONFIG_FORMAT_BIN) ->
    {ok, raw_plugin_config_content() | plugin_config_map() | any()}
    | {error, term()}.
get_config(NameVsn, ?CONFIG_FORMAT_MAP) ->
    get_config(NameVsn, ?CONFIG_FORMAT_MAP, #{});
get_config(NameVsn, ?CONFIG_FORMAT_BIN) ->
    get_config_bin(NameVsn).

%% Present default config value only in map format.
-spec get_config(name_vsn(), ?CONFIG_FORMAT_MAP, any()) ->
    {ok, plugin_config_map() | any()}
    | {error, term()}.
get_config(NameVsn, ?CONFIG_FORMAT_MAP, Default) ->
    {ok, persistent_term:get(?PLUGIN_PERSIS_CONFIG_KEY(bin(NameVsn)), Default)}.

get_config_bin(NameVsn) ->
    %% no default value when get raw binary config
    case read_plugin_hocon(NameVsn) of
        {ok, _Map} = Res -> Res;
        {error, _Reason} = Err -> Err
    end.

%% @doc Update plugin's config.
%% RPC call from Management API or CLI.
%% The plugin config Json Map was valid by avro schema
%% Or: if no and plugin config ALWAYS be valid before calling this function.
put_config(NameVsn, ConfigJsonMap, AvroValue) when (not is_binary(NameVsn)) ->
    put_config(bin(NameVsn), ConfigJsonMap, AvroValue);
put_config(NameVsn, ConfigJsonMap, _AvroValue) ->
    HoconBin = hocon_pp:do(ConfigJsonMap, #{}),
    ok = backup_and_write_hocon_bin(NameVsn, HoconBin),
    %% TODO: callback in plugin's on_config_upgraded (config vsn upgrade v1 -> v2)
    ok = maybe_call_on_config_changed(NameVsn, ConfigJsonMap),
    ok = persistent_term:put(?PLUGIN_PERSIS_CONFIG_KEY(NameVsn), ConfigJsonMap),
    ok.

%% @doc Stop and then start the plugin.
restart(NameVsn) ->
    case ensure_stopped(NameVsn) of
        ok -> ensure_started(NameVsn);
        {error, Reason} -> {error, Reason}
    end.

%% @doc Call plugin's callback on_config_changed/2
maybe_call_on_config_changed(NameVsn, NewConf) ->
    FuncName = on_config_changed,
    maybe
        {ok, PluginAppModule} ?= app_module_name(NameVsn),
        true ?= erlang:function_exported(PluginAppModule, FuncName, 2),
        {ok, OldConf} = get_config(NameVsn),
        try erlang:apply(PluginAppModule, FuncName, [OldConf, NewConf]) of
            _ -> ok
        catch
            Class:CatchReason:Stacktrace ->
                ?SLOG(error, #{
                    msg => "failed_to_call_on_config_changed",
                    exception => Class,
                    reason => CatchReason,
                    stacktrace => Stacktrace
                }),
                ok
        end
    else
        {error, Reason} ->
            ?SLOG(info, #{msg => "failed_to_call_on_config_changed", reason => Reason});
        false ->
            ?SLOG(info, #{msg => "on_config_changed_callback_not_exported"});
        _ ->
            ok
    end.

app_module_name(NameVsn) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, #{<<"name">> := Name} = _PluginInfo} ->
            emqx_utils:safe_to_existing_atom(<<Name/binary, "_app">>);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "failed_to_read_plugin_info"}),
            {error, Reason}
    end.

%% @doc List all installed plugins.
%% Including the ones that are installed, but not enabled in config.
-spec list() -> [plugin_info()].
list() ->
    list(normal).

-spec list(all | normal | hidden) -> [plugin_info()].
list(Type) ->
    Pattern = filename:join([install_dir(), "*", "release.json"]),
    All = lists:filtermap(
        fun(JsonFilePath) ->
            [_, NameVsn | _] = lists:reverse(filename:split(JsonFilePath)),
            case read_plugin_info(NameVsn, #{}) of
                {ok, Info} ->
                    filter_plugin_of_type(Type, Info);
                {error, Reason} ->
                    ?SLOG(warning, Reason#{msg => "failed_to_read_plugin_info"}),
                    false
            end
        end,
        filelib:wildcard(Pattern)
    ),
    do_list(configured(), All).

filter_plugin_of_type(all, Info) ->
    {true, Info};
filter_plugin_of_type(normal, #{<<"hidden">> := true}) ->
    false;
filter_plugin_of_type(normal, Info) ->
    {true, Info};
filter_plugin_of_type(hidden, #{<<"hidden">> := true} = Info) ->
    {true, Info};
filter_plugin_of_type(hidden, _Info) ->
    false.

%%--------------------------------------------------------------------
%% Package utils

-spec decode_plugin_config_map(name_vsn(), map() | binary()) ->
    {ok, map() | ?plugin_without_config_schema}
    | {error, any()}.
decode_plugin_config_map(NameVsn, AvroJsonMap) ->
    case with_plugin_avsc(NameVsn) of
        true ->
            case emqx_plugins_serde:lookup_serde(NameVsn) of
                {error, not_found} ->
                    Reason = "plugin_config_schema_serde_not_found",
                    ?SLOG(error, #{
                        msg => Reason, name_vsn => NameVsn, plugin_with_avro_schema => true
                    }),
                    {error, Reason};
                {ok, _Serde} ->
                    do_decode_plugin_config_map(NameVsn, AvroJsonMap)
            end;
        false ->
            ?SLOG(debug, #{
                msg => "plugin_without_config_schema",
                name_vsn => NameVsn
            }),
            {ok, ?plugin_without_config_schema}
    end.

do_decode_plugin_config_map(NameVsn, AvroJsonMap) when is_map(AvroJsonMap) ->
    do_decode_plugin_config_map(NameVsn, emqx_utils_json:encode(AvroJsonMap));
do_decode_plugin_config_map(NameVsn, AvroJsonBin) ->
    case emqx_plugins_serde:decode(NameVsn, AvroJsonBin) of
        {ok, Config} -> {ok, Config};
        {error, ReasonMap} -> {error, ReasonMap}
    end.

-spec with_plugin_avsc(name_vsn()) -> boolean().
with_plugin_avsc(NameVsn) ->
    case read_plugin_info(NameVsn, #{fill_readme => false}) of
        {ok, #{<<"with_config_schema">> := WithAvsc}} when is_boolean(WithAvsc) ->
            WithAvsc;
        _ ->
            false
    end.

get_config_interal(Key, Default) when is_atom(Key) ->
    get_config_interal([Key], Default);
get_config_interal(Path, Default) ->
    emqx_conf:get([?CONF_ROOT | Path], Default).

put_config_internal(Key, Value) ->
    do_put_config_internal(Key, Value, _ConfLocation = local).

-spec get_tar(name_vsn()) -> {ok, binary()} | {error, any}.
get_tar(NameVsn) ->
    TarGz = pkg_file_path(NameVsn),
    case file:read_file(TarGz) of
        {ok, Content} ->
            {ok, Content};
        {error, _} ->
            case maybe_create_tar(NameVsn, TarGz, install_dir()) of
                ok ->
                    file:read_file(TarGz);
                Err ->
                    Err
            end
    end.

ensure_ssl_files(NameVsn, SSL) ->
    emqx_tls_lib:ensure_ssl_files(plugin_certs_dir(NameVsn), SSL).

ensure_ssl_files(NameVsn, SSL, Opts) ->
    emqx_tls_lib:ensure_ssl_files(plugin_certs_dir(NameVsn), SSL, Opts).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

maybe_create_tar(NameVsn, TarGzName, InstallDir) when is_binary(InstallDir) ->
    maybe_create_tar(NameVsn, TarGzName, binary_to_list(InstallDir));
maybe_create_tar(NameVsn, TarGzName, InstallDir) ->
    case filelib:wildcard(filename:join(plugin_dir(NameVsn), "**")) of
        [_ | _] = PluginFiles ->
            InstallDir1 = string:trim(InstallDir, trailing, "/") ++ "/",
            PluginFiles1 = [{string:prefix(F, InstallDir1), F} || F <- PluginFiles],
            erl_tar:create(TarGzName, PluginFiles1, [compressed]);
        _ ->
            {error, plugin_not_found}
    end.

write_tar_file_content(BaseDir, TarContent) ->
    lists:foreach(
        fun({Name, Bin}) ->
            Filename = filename:join(BaseDir, Name),
            ok = filelib:ensure_dir(Filename),
            ok = file:write_file(Filename, Bin)
        end,
        TarContent
    ).

delete_tar_file_content(BaseDir, TarContent) ->
    lists:foreach(
        fun({Name, _}) ->
            Filename = filename:join(BaseDir, Name),
            case filelib:is_file(Filename) of
                true ->
                    TopDirOrFile = top_dir(BaseDir, Filename),
                    ok = file:del_dir_r(TopDirOrFile);
                false ->
                    %% probably already deleted
                    ok
            end
        end,
        TarContent
    ).

top_dir(BaseDir0, DirOrFile) ->
    BaseDir = normalize_dir(BaseDir0),
    case filename:dirname(DirOrFile) of
        RockBottom when RockBottom =:= "/" orelse RockBottom =:= "." ->
            throw({out_of_bounds, DirOrFile});
        BaseDir ->
            DirOrFile;
        Parent ->
            top_dir(BaseDir, Parent)
    end.

normalize_dir(Dir) ->
    %% Get rid of possible trailing slash
    filename:join([Dir, ""]).

-ifdef(TEST).
normalize_dir_test_() ->
    [
        ?_assertEqual("foo", normalize_dir("foo")),
        ?_assertEqual("foo", normalize_dir("foo/")),
        ?_assertEqual("/foo", normalize_dir("/foo")),
        ?_assertEqual("/foo", normalize_dir("/foo/"))
    ].

top_dir_test_() ->
    [
        ?_assertEqual("base/foo", top_dir("base", filename:join(["base", "foo", "bar"]))),
        ?_assertEqual("/base/foo", top_dir("/base", filename:join(["/", "base", "foo", "bar"]))),
        ?_assertEqual("/base/foo", top_dir("/base/", filename:join(["/", "base", "foo", "bar"]))),
        ?_assertThrow({out_of_bounds, _}, top_dir("/base", filename:join(["/", "base"]))),
        ?_assertThrow({out_of_bounds, _}, top_dir("/base", filename:join(["/", "foo", "bar"])))
    ].
-endif.

do_ensure_installed(NameVsn) ->
    TarGz = pkg_file_path(NameVsn),
    case erl_tar:extract(TarGz, [compressed, memory]) of
        {ok, TarContent} ->
            ok = write_tar_file_content(install_dir(), TarContent),
            case read_plugin_info(NameVsn, #{}) of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, Reason#{msg => "failed_to_read_after_install"}),
                    ok = delete_tar_file_content(install_dir(), TarContent),
                    {error, Reason}
            end;
        {error, {_, enoent}} ->
            {error, #{
                msg => "failed_to_extract_plugin_package",
                path => TarGz,
                reason => plugin_tarball_not_found
            }};
        {error, Reason} ->
            {error, #{
                msg => "bad_plugin_package",
                path => TarGz,
                reason => Reason
            }}
    end.

ensure_delete(NameVsn0) ->
    NameVsn = bin(NameVsn0),
    List = configured(),
    put_configured(lists:filter(fun(#{name_vsn := N1}) -> bin(N1) =/= NameVsn end, List)),
    ok.

ensure_state(NameVsn, Position, State, ConfLocation) when is_binary(NameVsn) ->
    ensure_state(binary_to_list(NameVsn), Position, State, ConfLocation);
ensure_state(NameVsn, Position, State, ConfLocation) ->
    case read_plugin_info(NameVsn, #{}) of
        {ok, _} ->
            Item = #{
                name_vsn => NameVsn,
                enable => State
            },
            tryit(
                "ensure_state",
                fun() -> ensure_configured(Item, Position, ConfLocation) end
            );
        {error, Reason} ->
            ?SLOG(error, #{msg => "ensure_plugin_states_failed", reason => Reason}),
            {error, Reason}
    end.

ensure_configured(#{name_vsn := NameVsn} = Item, Position, ConfLocation) ->
    Configured = configured(),
    SplitFun = fun(#{name_vsn := Nv}) -> bin(Nv) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    NewConfigured =
        case Rear of
            [_ | More] when Position =:= no_move ->
                Front ++ [Item | More];
            [_ | More] ->
                add_new_configured(Front ++ More, Position, Item);
            [] ->
                add_new_configured(Configured, Position, Item)
        end,
    ok = put_configured(NewConfigured, ConfLocation).

add_new_configured(Configured, no_move, Item) ->
    %% default to rear
    add_new_configured(Configured, rear, Item);
add_new_configured(Configured, front, Item) ->
    [Item | Configured];
add_new_configured(Configured, rear, Item) ->
    Configured ++ [Item];
add_new_configured(Configured, {Action, NameVsn}, Item) ->
    SplitFun = fun(#{name_vsn := Nv}) -> bin(Nv) =/= bin(NameVsn) end,
    {Front, Rear} = lists:splitwith(SplitFun, Configured),
    Rear =:= [] andalso
        throw(#{
            msg => "position_anchor_plugin_not_configured",
            hint => "maybe_install_and_configure",
            name_vsn => NameVsn
        }),
    case Action of
        before ->
            Front ++ [Item | Rear];
        behind ->
            [Anchor | Rear0] = Rear,
            Front ++ [Anchor, Item | Rear0]
    end.

maybe_purge_plugin_config(NameVsn) ->
    _ = persistent_term:erase(?PLUGIN_PERSIS_CONFIG_KEY(NameVsn)),
    ok.

purge_plugin(NameVsn) ->
    Dir = plugin_dir(NameVsn),
    purge_plugin_dir(Dir).

purge_plugin_dir(Dir) ->
    case file:del_dir_r(Dir) of
        ok ->
            ?SLOG(info, #{
                msg => "purged_plugin_dir",
                dir => Dir
            });
        {error, enoent} ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_purge_plugin_dir",
                dir => Dir,
                reason => Reason
            }),
            {error, Reason}
    end.

%% Make sure configured ones are ordered in front.
do_list([], All) ->
    All;
do_list([#{name_vsn := NameVsn} | Rest], All) ->
    SplitF = fun(#{<<"name">> := Name, <<"rel_vsn">> := Vsn}) ->
        bin([Name, "-", Vsn]) =/= bin(NameVsn)
    end,
    case lists:splitwith(SplitF, All) of
        {_, []} ->
            do_list(Rest, All);
        {Front, [I | Rear]} ->
            [I | do_list(Rest, Front ++ Rear)]
    end.

do_ensure_started(NameVsn) ->
    tryit(
        "start_plugins",
        fun() ->
            case ensure_exists_and_installed(NameVsn) of
                ok ->
                    _ = maybe_load_config_schema(NameVsn, ?normal),
                    Plugin = do_read_plugin(NameVsn),
                    ok = load_code_start_apps(NameVsn, Plugin);
                {error, #{reason := Reason} = ReasonMap} ->
                    ?SLOG(error, #{
                        msg => "failed_to_start_plugin",
                        name_vsn => NameVsn,
                        reason => Reason
                    }),
                    {error, ReasonMap}
            end
        end
    ).

%%--------------------------------------------------------------------

%% try the function, catch 'throw' exceptions as normal 'error' return
%% other exceptions with stacktrace logged.
tryit(WhichOp, F) ->
    try
        F()
    catch
        throw:ReasonMap when is_map(ReasonMap) ->
            %% thrown exceptions are known errors
            %% translate to a return value without stacktrace
            {error, ReasonMap};
        throw:Reason ->
            {error, #{reason => Reason}};
        error:Reason:Stacktrace ->
            %% unexpected errors, log stacktrace
            ?SLOG(warning, #{
                msg => "plugin_op_failed",
                which_op => WhichOp,
                exception => Reason,
                stacktrace => Stacktrace
            }),
            {error, #{
                which_op => WhichOp,
                exception => Reason,
                stacktrace => Stacktrace
            }}
    end.

%% read plugin info from the JSON file
%% returns {ok, Info} or {error, Reason}
read_plugin_info(NameVsn, Options) ->
    tryit(
        atom_to_list(?FUNCTION_NAME),
        fun() -> {ok, do_read_plugin(NameVsn, Options)} end
    ).

do_read_plugin(NameVsn) ->
    do_read_plugin(NameVsn, #{}).

do_read_plugin(NameVsn, Option) ->
    do_read_plugin(NameVsn, info_file_path(NameVsn), Option).

do_read_plugin(NameVsn, InfoFilePath, Options) ->
    {ok, PlainMap} = (read_file_fun(InfoFilePath, "bad_info_file", #{read_mode => ?JSON_MAP}))(),
    Info0 = check_plugin(PlainMap, NameVsn, InfoFilePath),
    Info1 = plugins_readme(NameVsn, Options, Info0),
    Info2 = plugins_package_info(NameVsn, Info1),
    plugin_status(NameVsn, Info2).

read_plugin_avsc(NameVsn) ->
    read_plugin_avsc(NameVsn, #{read_mode => ?JSON_MAP}).
read_plugin_avsc(NameVsn, Options) ->
    tryit(
        atom_to_list(?FUNCTION_NAME),
        read_file_fun(avsc_file_path(NameVsn), "bad_avsc_file", Options)
    ).

read_plugin_i18n(NameVsn) ->
    read_plugin_i18n(NameVsn, #{read_mode => ?JSON_MAP}).
read_plugin_i18n(NameVsn, Options) ->
    tryit(
        atom_to_list(?FUNCTION_NAME),
        read_file_fun(i18n_file_path(NameVsn), "bad_i18n_file", Options)
    ).

read_plugin_hocon(NameVsn) ->
    read_plugin_hocon(NameVsn, #{read_mode => ?RAW_BIN}).
read_plugin_hocon(NameVsn, Options) ->
    tryit(
        atom_to_list(?FUNCTION_NAME),
        read_file_fun(plugin_config_file(NameVsn), "bad_hocon_file", Options)
    ).

ensure_exists_and_installed(NameVsn) ->
    case filelib:is_dir(plugin_dir(NameVsn)) of
        true ->
            ok;
        false ->
            %% Do we have the package, but it's not extracted yet?
            case get_tar(NameVsn) of
                {ok, TarContent} ->
                    ok = file:write_file(pkg_file_path(NameVsn), TarContent),
                    do_ensure_installed(NameVsn);
                _ ->
                    %% If not, try to get it from the cluster.
                    do_get_from_cluster(NameVsn)
            end
    end.

do_get_from_cluster(NameVsn) ->
    Nodes = [N || N <- mria:running_nodes(), N /= node()],
    case get_plugin_tar_from_any_node(Nodes, NameVsn, []) of
        {ok, TarContent} ->
            ok = file:write_file(pkg_file_path(NameVsn), TarContent),
            ok = do_ensure_installed(NameVsn);
        {error, NodeErrors} when Nodes =/= [] ->
            ErrMeta = #{
                msg => "failed_to_copy_plugin_from_other_nodes",
                name_vsn => NameVsn,
                node_errors => NodeErrors,
                reason => plugin_not_found
            },
            ?SLOG(error, ErrMeta),
            {error, ErrMeta};
        {error, _} ->
            ErrMeta = #{
                msg => "no_nodes_to_copy_plugin_from",
                name_vsn => NameVsn,
                reason => plugin_not_found
            },
            ?SLOG(error, ErrMeta),
            {error, ErrMeta}
    end.

get_plugin_tar_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_plugin_tar_from_any_node([Node | T], NameVsn, Errors) ->
    case emqx_plugins_proto_v1:get_tar(Node, NameVsn, infinity) of
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_tar_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_plugin_tar_from_any_node(T, NameVsn, [{Node, Err} | Errors])
    end.

get_plugin_config_from_any_node([], _NameVsn, Errors) ->
    {error, Errors};
get_plugin_config_from_any_node([Node | T], NameVsn, Errors) ->
    case
        emqx_plugins_proto_v2:get_config(
            Node, NameVsn, ?CONFIG_FORMAT_MAP, ?plugin_conf_not_found, 5_000
        )
    of
        {ok, _} = Res ->
            ?SLOG(debug, #{
                msg => "get_plugin_config_from_cluster_successfully",
                node => Node,
                name_vsn => NameVsn
            }),
            Res;
        Err ->
            get_plugin_config_from_any_node(T, NameVsn, [{Node, Err} | Errors])
    end.

plugins_package_info(NameVsn, Info) ->
    case file:read_file(md5sum_file(NameVsn)) of
        {ok, MD5} -> Info#{md5sum => MD5};
        _ -> Info#{md5sum => <<>>}
    end.

plugins_readme(NameVsn, #{fill_readme := true}, Info) ->
    case file:read_file(readme_file(NameVsn)) of
        {ok, Bin} -> Info#{readme => Bin};
        _ -> Info#{readme => <<>>}
    end;
plugins_readme(_NameVsn, _Options, Info) ->
    Info.

plugin_status(NameVsn, Info) ->
    {ok, AppName, _AppVsn} = parse_name_vsn(NameVsn),
    RunningSt =
        case application:get_key(AppName, vsn) of
            {ok, _} ->
                case lists:keyfind(AppName, 1, running_apps()) of
                    {AppName, _} -> running;
                    _ -> loaded
                end;
            undefined ->
                stopped
        end,
    Configured = lists:filtermap(
        fun(#{name_vsn := Nv, enable := St}) ->
            case bin(Nv) =:= bin(NameVsn) of
                true -> {true, St};
                false -> false
            end
        end,
        configured()
    ),
    ConfSt =
        case Configured of
            [] -> not_configured;
            [true] -> enabled;
            [false] -> disabled
        end,
    Info#{
        running_status => RunningSt,
        config_status => ConfSt
    }.

check_plugin(
    #{
        <<"name">> := Name,
        <<"rel_vsn">> := Vsn,
        <<"rel_apps">> := Apps,
        <<"description">> := _
    } = Info,
    NameVsn,
    FilePath
) ->
    case bin(NameVsn) =:= bin([Name, "-", Vsn]) of
        true ->
            try
                %% assert
                [_ | _] = Apps,
                %% validate if the list is all <app>-<vsn> strings
                lists:foreach(fun(App) -> {ok, _, _} = parse_name_vsn(App) end, Apps)
            catch
                _:_ ->
                    throw(#{
                        msg => "bad_rel_apps",
                        rel_apps => Apps,
                        hint => "A non-empty string list of app_name-app_vsn format"
                    })
            end,
            Info;
        false ->
            throw(#{
                msg => "name_vsn_mismatch",
                name_vsn => NameVsn,
                path => FilePath,
                name => Name,
                rel_vsn => Vsn
            })
    end;
check_plugin(_What, NameVsn, File) ->
    throw(#{
        msg => "bad_info_file_content",
        mandatory_fields => [rel_vsn, name, rel_apps, description],
        name_vsn => NameVsn,
        path => File
    }).

load_code_start_apps(RelNameVsn, #{<<"rel_apps">> := Apps}) ->
    LibDir = filename:join([install_dir(), RelNameVsn]),
    RunningApps = running_apps(),
    %% load plugin apps and beam code
    AppNames =
        lists:map(
            fun(AppNameVsn) ->
                {ok, AppName, AppVsn} = parse_name_vsn(AppNameVsn),
                EbinDir = filename:join([LibDir, AppNameVsn, "ebin"]),
                ok = load_plugin_app(AppName, AppVsn, EbinDir, RunningApps),
                AppName
            end,
            Apps
        ),
    lists:foreach(fun start_app/1, AppNames).

load_plugin_app(AppName, AppVsn, Ebin, RunningApps) ->
    case lists:keyfind(AppName, 1, RunningApps) of
        false ->
            do_load_plugin_app(AppName, Ebin);
        {_, Vsn} ->
            case bin(Vsn) =:= bin(AppVsn) of
                true ->
                    %% already started on the exact version
                    ok;
                false ->
                    %% running but a different version
                    ?SLOG(warning, #{
                        msg => "plugin_app_already_running",
                        name => AppName,
                        running_vsn => Vsn,
                        loading_vsn => AppVsn
                    })
            end
    end.

do_load_plugin_app(AppName, Ebin) when is_binary(Ebin) ->
    do_load_plugin_app(AppName, binary_to_list(Ebin));
do_load_plugin_app(AppName, Ebin) ->
    _ = code:add_patha(Ebin),
    Modules = filelib:wildcard(filename:join([Ebin, "*.beam"])),
    lists:foreach(
        fun(BeamFile) ->
            Module = list_to_atom(filename:basename(BeamFile, ".beam")),
            _ = code:purge(Module),
            case code:load_file(Module) of
                {module, _} ->
                    ok;
                {error, Reason} ->
                    throw(#{
                        msg => "failed_to_load_plugin_beam",
                        path => BeamFile,
                        reason => Reason
                    })
            end
        end,
        Modules
    ),
    case application:load(AppName) of
        ok ->
            ok;
        {error, {already_loaded, _}} ->
            ok;
        {error, Reason} ->
            throw(#{
                msg => "failed_to_load_plugin_app",
                name => AppName,
                reason => Reason
            })
    end.

start_app(App) ->
    case run_with_timeout(application, ensure_all_started, [App], 10_000) of
        {ok, {ok, Started}} ->
            case Started =/= [] of
                true -> ?SLOG(debug, #{msg => "started_plugin_apps", apps => Started});
                false -> ok
            end;
        {ok, {error, Reason}} ->
            throw(#{
                msg => "failed_to_start_app",
                app => App,
                reason => Reason
            });
        {error, Reason} ->
            throw(#{
                msg => "failed_to_start_plugin_app",
                app => App,
                reason => Reason
            })
    end.

%% Stop all apps installed by the plugin package,
%% but not the ones shared with others.
ensure_apps_stopped(#{<<"rel_apps">> := Apps}) ->
    %% load plugin apps and beam code
    AppsToStop = lists:filtermap(fun parse_name_vsn_for_stopping/1, Apps),
    case tryit("stop_apps", fun() -> stop_apps(AppsToStop) end) of
        {ok, []} ->
            %% all apps stopped
            ok;
        {ok, Left} ->
            ?SLOG(warning, #{
                msg => "unabled_to_stop_plugin_apps",
                apps => Left,
                reason => "running_apps_still_depends_on_this_apps"
            }),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% On one hand, Elixir plugins might include Elixir itself, when targetting a non-Elixir
%% EMQX release.  If, on the other hand, the EMQX release already includes Elixir, we
%% shouldn't stop Elixir nor IEx.
-ifdef(EMQX_ELIXIR).
is_protected_app(elixir) -> true;
is_protected_app(iex) -> true;
is_protected_app(_) -> false.

parse_name_vsn_for_stopping(NameVsn) ->
    {ok, AppName, _AppVsn} = parse_name_vsn(NameVsn),
    case is_protected_app(AppName) of
        true ->
            false;
        false ->
            {true, AppName}
    end.
%% ELSE ifdef(EMQX_ELIXIR)
-else.
parse_name_vsn_for_stopping(NameVsn) ->
    {ok, AppName, _AppVsn} = parse_name_vsn(NameVsn),
    {true, AppName}.
%% END ifdef(EMQX_ELIXIR)
-endif.

stop_apps(Apps) ->
    RunningApps = running_apps(),
    case do_stop_apps(Apps, [], RunningApps) of
        %% all stopped
        {ok, []} -> {ok, []};
        %% no progress
        {ok, Remain} when Remain =:= Apps -> {ok, Apps};
        %% try again
        {ok, Remain} -> stop_apps(Remain)
    end.

do_stop_apps([], Remain, _AllApps) ->
    {ok, lists:reverse(Remain)};
do_stop_apps([App | Apps], Remain, RunningApps) ->
    case is_needed_by_any(App, RunningApps) of
        true ->
            do_stop_apps(Apps, [App | Remain], RunningApps);
        false ->
            ok = stop_app(App),
            do_stop_apps(Apps, Remain, RunningApps)
    end.

stop_app(App) ->
    case application:stop(App) of
        ok ->
            ?SLOG(debug, #{msg => "stop_plugin_successfully", app => App}),
            ok = unload_module_and_app(App);
        {error, {not_started, App}} ->
            ?SLOG(debug, #{msg => "plugin_not_started", app => App}),
            ok = unload_module_and_app(App);
        {error, Reason} ->
            throw(#{msg => "failed_to_stop_app", app => App, reason => Reason})
    end.

unload_module_and_app(App) ->
    case application:get_key(App, modules) of
        {ok, Modules} ->
            lists:foreach(fun code:soft_purge/1, Modules);
        _ ->
            ok
    end,
    _ = application:unload(App),
    ok.

is_needed_by_any(AppToStop, RunningApps) ->
    lists:any(
        fun({RunningApp, _RunningAppVsn}) ->
            is_needed_by(AppToStop, RunningApp)
        end,
        RunningApps
    ).

is_needed_by(AppToStop, AppToStop) ->
    false;
is_needed_by(AppToStop, RunningApp) ->
    case application:get_key(RunningApp, applications) of
        {ok, Deps} -> lists:member(AppToStop, Deps);
        undefined -> false
    end.

do_put_config_internal(Key, Value, ConfLocation) when is_atom(Key) ->
    do_put_config_internal([Key], Value, ConfLocation);
do_put_config_internal(Path, Values, _ConfLocation = local) when is_list(Path) ->
    Opts = #{rawconf_with_defaults => true, override_to => cluster},
    %% Already in cluster_rpc, don't use emqx_conf:update, dead calls
    case emqx:update_config([?CONF_ROOT | Path], bin_key(Values), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end;
do_put_config_internal(Path, Values, _ConfLocation = global) when is_list(Path) ->
    Opts = #{rawconf_with_defaults => true, override_to => cluster},
    case emqx_conf:update([?CONF_ROOT | Path], bin_key(Values), Opts) of
        {ok, _} -> ok;
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% `emqx_config_handler' API
%%--------------------------------------------------------------------

post_config_update([?CONF_ROOT], _Req, #{states := NewStates}, #{states := OldStates}, _Envs) ->
    NewStatesIndex = maps:from_list([{NV, S} || S = #{name_vsn := NV} <- NewStates]),
    OldStatesIndex = maps:from_list([{NV, S} || S = #{name_vsn := NV} <- OldStates]),
    #{changed := Changed} = emqx_utils_maps:diff_maps(NewStatesIndex, OldStatesIndex),
    maps:foreach(fun enable_disable_plugin/2, Changed),
    ok;
post_config_update(_Path, _Req, _NewConf, _OldConf, _Envs) ->
    ok.

enable_disable_plugin(NameVsn, {#{enable := true}, #{enable := false}}) ->
    %% errors are already logged in this fn
    _ = ensure_stopped(NameVsn),
    ok;
enable_disable_plugin(NameVsn, {#{enable := false}, #{enable := true}}) ->
    %% errors are already logged in this fn
    _ = ensure_started(NameVsn),
    ok;
enable_disable_plugin(_NameVsn, _Diff) ->
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

install_dir() ->
    get_config_interal(install_dir, "").

put_configured(Configured) ->
    put_configured(Configured, _ConfLocation = local).

put_configured(Configured, ConfLocation) ->
    ok = do_put_config_internal(states, bin_key(Configured), ConfLocation).

configured() ->
    get_config_interal(states, []).

for_plugins(ActionFun) ->
    case lists:flatmap(ActionFun, configured()) of
        [] ->
            ok;
        Errors ->
            ErrMeta = #{function => ActionFun, errors => Errors},
            ?tp(
                for_plugins_action_error_occurred,
                ErrMeta
            ),
            ?SLOG(error, ErrMeta#{msg => "for_plugins_action_error_occurred"}),
            ok
    end.

maybe_post_op_after_installed(NameVsn0, Mode) ->
    NameVsn = wrap_to_list(NameVsn0),
    _ = maybe_load_config_schema(NameVsn, Mode),
    ok = maybe_ensure_state(NameVsn).

maybe_ensure_state(NameVsn) ->
    EnsureStateFun = fun(#{name_vsn := NV, enable := Bool}, AccIn) ->
        case NV of
            NameVsn ->
                %% Configured, using existed cluster config
                _ = ensure_state(NV, no_move, Bool, global),
                AccIn#{ensured => true};
            _ ->
                AccIn
        end
    end,
    case lists:foldl(EnsureStateFun, #{ensured => false}, configured()) of
        #{ensured := true} ->
            ok;
        #{ensured := false} ->
            ?SLOG(info, #{msg => "plugin_not_configured", name_vsn => NameVsn}),
            %% Clean installation, no config, ensure with `Enable = false`
            _ = ensure_state(NameVsn, no_move, false, global),
            ok
    end,
    ok.

maybe_load_config_schema(NameVsn, Mode) ->
    AvscPath = avsc_file_path(NameVsn),
    _ =
        with_plugin_avsc(NameVsn) andalso
            filelib:is_regular(AvscPath) andalso
            do_load_config_schema(NameVsn, AvscPath),
    _ = maybe_create_config_dir(NameVsn, Mode).

do_load_config_schema(NameVsn, AvscPath) ->
    case emqx_plugins_serde:add_schema(bin(NameVsn), AvscPath) of
        ok -> ok;
        {error, already_exists} -> ok;
        {error, _Reason} -> ok
    end.

maybe_create_config_dir(NameVsn, Mode) ->
    with_plugin_avsc(NameVsn) andalso
        do_create_config_dir(NameVsn, Mode).

do_create_config_dir(NameVsn, Mode) ->
    case plugin_data_dir(NameVsn) of
        {error, Reason} ->
            {error, {gen_config_dir_failed, Reason}};
        ConfigDir ->
            case filelib:ensure_path(ConfigDir) of
                ok ->
                    %% get config from other nodes or get from tarball
                    _ = maybe_ensure_plugin_config(NameVsn, Mode),
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_create_plugin_config_dir",
                        dir => ConfigDir,
                        reason => Reason
                    }),
                    {error, {mkdir_failed, ConfigDir, Reason}}
            end
    end.

-spec maybe_ensure_plugin_config(name_vsn(), ?fresh_install | ?normal) -> ok.
maybe_ensure_plugin_config(NameVsn, Mode) ->
    maybe
        true ?= with_plugin_avsc(NameVsn),
        _ = ensure_plugin_config({NameVsn, Mode})
    else
        _ -> ok
    end.

-spec ensure_plugin_config({name_vsn(), ?fresh_install | ?normal}) -> ok.
ensure_plugin_config({NameVsn, ?normal}) ->
    ensure_plugin_config(NameVsn, [N || N <- mria:running_nodes(), N /= node()]);
ensure_plugin_config({NameVsn, ?fresh_install}) ->
    ?SLOG(debug, #{
        msg => "default_plugin_config_used",
        name_vsn => NameVsn,
        hint => "fresh_install"
    }),
    cp_default_config_file(NameVsn).

-spec ensure_plugin_config(name_vsn(), list()) -> ok.
ensure_plugin_config(NameVsn, []) ->
    ?SLOG(debug, #{
        msg => "local_plugin_config_used",
        name_vsn => NameVsn,
        reason => "no_other_running_nodes"
    }),
    cp_default_config_file(NameVsn);
ensure_plugin_config(NameVsn, Nodes) ->
    case get_plugin_config_from_any_node(Nodes, NameVsn, []) of
        {ok, ConfigMap} when is_map(ConfigMap) ->
            HoconBin = hocon_pp:do(ConfigMap, #{}),
            Path = plugin_config_file(NameVsn),
            ok = filelib:ensure_dir(Path),
            ok = file:write_file(Path, HoconBin),
            ensure_config_map(NameVsn);
        _ ->
            ?SLOG(error, #{msg => "config_not_found_from_cluster", name_vsn => NameVsn}),
            cp_default_config_file(NameVsn)
    end.

-spec cp_default_config_file(name_vsn()) -> ok.
cp_default_config_file(NameVsn) ->
    %% always copy default hocon file into config dir when can not get config from other nodes
    Source = default_plugin_config_file(NameVsn),
    Destination = plugin_config_file(NameVsn),
    maybe
        true ?= filelib:is_regular(Source),
        %% destination path not existed (not configured)
        false ?=
            case filelib:is_regular(Destination) of
                true ->
                    ?SLOG(debug, #{msg => "plugin_config_file_already_existed", name_vsn => NameVsn});
                false ->
                    false
            end,
        ok = filelib:ensure_dir(Destination),
        case file:copy(Source, Destination) of
            {ok, _} ->
                ensure_config_map(NameVsn);
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "failed_to_copy_plugin_default_hocon_config",
                    source => Source,
                    destination => Destination,
                    reason => Reason
                })
        end
    else
        _ -> ensure_config_map(NameVsn)
    end.

ensure_config_map(NameVsn) ->
    case read_plugin_hocon(NameVsn, #{read_mode => ?JSON_MAP}) of
        {ok, ConfigJsonMap} ->
            case with_plugin_avsc(NameVsn) of
                true ->
                    do_ensure_config_map(NameVsn, ConfigJsonMap);
                false ->
                    ?SLOG(debug, #{
                        msg => "put_plugin_config_directly",
                        hint => "plugin_without_config_schema",
                        name_vsn => NameVsn
                    }),
                    put_config(NameVsn, ConfigJsonMap, ?plugin_without_config_schema)
            end;
        _ ->
            ?SLOG(warning, #{msg => "failed_to_read_plugin_config_hocon", name_vsn => NameVsn}),
            ok
    end.

do_ensure_config_map(NameVsn, ConfigJsonMap) ->
    case decode_plugin_config_map(NameVsn, ConfigJsonMap) of
        {ok, AvroValue} ->
            put_config(NameVsn, ConfigJsonMap, AvroValue);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "plugin_config_validation_failed",
                name_vsn => NameVsn,
                reason => Reason
            }),
            ok
    end.

%% @private Backup the current config to a file with a timestamp suffix and
%% then save the new config to the config file.
backup_and_write_hocon_bin(NameVsn, HoconBin) ->
    %% this may fail, but we don't care
    %% e.g. read-only file system
    Path = plugin_config_file(NameVsn),
    _ = filelib:ensure_dir(Path),
    TmpFile = Path ++ ".tmp",
    case file:write_file(TmpFile, HoconBin) of
        ok ->
            backup_and_replace(Path, TmpFile);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_save_plugin_conf_file",
                hint =>
                    "The updated cluster config is not saved on this node, please check the file system.",
                filename => TmpFile,
                reason => Reason
            }),
            %% e.g. read-only, it's not the end of the world
            ok
    end.

backup_and_replace(Path, TmpPath) ->
    Backup = Path ++ "." ++ emqx_utils_calendar:now_time(millisecond) ++ ".bak",
    case file:rename(Path, Backup) of
        ok ->
            ok = file:rename(TmpPath, Path),
            ok = prune_backup_files(Path);
        {error, enoent} ->
            %% not created yet
            ok = file:rename(TmpPath, Path);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_backup_plugin_conf_file",
                filename => Backup,
                reason => Reason
            }),
            ok
    end.

prune_backup_files(Path) ->
    Files0 = filelib:wildcard(Path ++ ".*"),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = lists:filter(fun(F) -> re:run(F, Re) =/= nomatch end, Files0),
    Sorted = lists:reverse(lists:sort(Files)),
    {_Keeps, Deletes} = lists:split(min(?MAX_KEEP_BACKUP_CONFIGS, length(Sorted)), Sorted),
    lists:foreach(
        fun(F) ->
            case file:delete(F) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_delete_backup_plugin_conf_file",
                        filename => F,
                        reason => Reason
                    }),
                    ok
            end
        end,
        Deletes
    ).

read_file_fun(Path, Msg, #{read_mode := ?RAW_BIN}) ->
    fun() ->
        case file:read_file(Path) of
            {ok, Bin} ->
                {ok, Bin};
            {error, Reason} ->
                ErrMeta = #{msg => Msg, reason => Reason},
                throw(ErrMeta)
        end
    end;
read_file_fun(Path, Msg, #{read_mode := ?JSON_MAP}) ->
    fun() ->
        case hocon:load(Path, #{format => richmap}) of
            {ok, RichMap} ->
                {ok, hocon_maps:ensure_plain(RichMap)};
            {error, Reason} ->
                ErrMeta = #{msg => Msg, reason => Reason},
                throw(ErrMeta)
        end
    end.

%% Directorys
-spec plugin_dir(name_vsn()) -> string().
plugin_dir(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), NameVsn])).

-spec plugin_priv_dir(name_vsn()) -> string().
plugin_priv_dir(NameVsn) ->
    maybe
        {ok, #{<<"name">> := Name, <<"rel_apps">> := Apps}} ?=
            read_plugin_info(NameVsn, #{fill_readme => false}),
        {ok, AppDir} ?= app_dir(Name, Apps),
        wrap_to_list(filename:join([plugin_dir(NameVsn), AppDir, "priv"]))
    else
        %% Otherwise assume the priv directory is under the plugin root directory
        _ -> wrap_to_list(filename:join([install_dir(), NameVsn, "priv"]))
    end.

-spec plugin_data_dir(name_vsn()) -> string() | {error, Reason :: string()}.
plugin_data_dir(NameVsn) ->
    case parse_name_vsn(NameVsn) of
        {ok, NameAtom, _Vsn} ->
            wrap_to_list(filename:join([emqx:data_dir(), "plugins", atom_to_list(NameAtom)]));
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_generate_plugin_config_dir_for_plugin",
                plugin_namevsn => NameVsn,
                reason => Reason
            }),
            {error, Reason}
    end.

plugin_certs_dir(NameVsn) ->
    wrap_to_list(filename:join([plugin_data_dir(NameVsn), "certs"])).

%% Files
-spec pkg_file_path(name_vsn()) -> string().
pkg_file_path(NameVsn) ->
    wrap_to_list(filename:join([install_dir(), bin([NameVsn, ".tar.gz"])])).

-spec info_file_path(name_vsn()) -> string().
info_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_dir(NameVsn), "release.json"])).

-spec avsc_file_path(name_vsn()) -> string().
avsc_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config_schema.avsc"])).

-spec plugin_config_file(name_vsn()) -> string().
plugin_config_file(NameVsn) ->
    wrap_to_list(filename:join([plugin_data_dir(NameVsn), "config.hocon"])).

%% should only used when plugin installing
-spec default_plugin_config_file(name_vsn()) -> string().
default_plugin_config_file(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config.hocon"])).

-spec i18n_file_path(name_vsn()) -> string().
i18n_file_path(NameVsn) ->
    wrap_to_list(filename:join([plugin_priv_dir(NameVsn), "config_i18n.json"])).

-spec md5sum_file(name_vsn()) -> string().
md5sum_file(NameVsn) ->
    plugin_dir(NameVsn) ++ ".tar.gz.md5sum".

-spec readme_file(name_vsn()) -> string().
readme_file(NameVsn) ->
    wrap_to_list(filename:join([plugin_dir(NameVsn), "README.md"])).

running_apps() ->
    lists:map(
        fun({N, _, V}) ->
            {N, V}
        end,
        application:which_applications(infinity)
    ).

bin_key(Map) when is_map(Map) ->
    maps:fold(fun(K, V, Acc) -> Acc#{bin(K) => V} end, #{}, Map);
bin_key(List = [#{} | _]) ->
    lists:map(fun(M) -> bin_key(M) end, List);
bin_key(Term) ->
    Term.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> unicode:characters_to_binary(L, utf8);
bin(B) when is_binary(B) -> B.

wrap_to_list(Path) ->
    binary_to_list(iolist_to_binary(Path)).

run_with_timeout(Module, Function, Args, Timeout) ->
    Self = self(),
    Fun = fun() ->
        Result = apply(Module, Function, Args),
        Self ! {self(), Result}
    end,
    Pid = spawn(Fun),
    TimerRef = erlang:send_after(Timeout, self(), {timeout, Pid}),
    receive
        {Pid, Result} ->
            _ = erlang:cancel_timer(TimerRef),
            {ok, Result};
        {timeout, Pid} ->
            exit(Pid, kill),
            {error, timeout}
    end.
