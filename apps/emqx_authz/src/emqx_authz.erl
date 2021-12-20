%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz).
-behaviour(emqx_config_handler).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([ register_metrics/0
        , init/0
        , deinit/0
        , lookup/0
        , lookup/1
        , move/2
        , move/3
        , update/2
        , update/3
        , authorize/5
        ]).

-export([post_config_update/5, pre_config_update/3]).

-export([acl_conf_file/0]).

-export([ph_to_re/1]).

-type(source() :: map()).

-type(match_result() :: {matched, allow} | {matched, deny} | nomatch).

-type(default_result() :: allow | deny).

-type(authz_result() :: {stop, allow} | {ok, deny}).

-type(sources() :: [source()]).

-define(METRIC_ALLOW, 'client.authorize.allow').
-define(METRIC_DENY, 'client.authorize.deny').
-define(METRIC_NOMATCH, 'client.authorize.nomatch').

-define(METRICS, [?METRIC_ALLOW, ?METRIC_DENY, ?METRIC_NOMATCH]).

-define(IS_BOOL(Enable), ((Enable =:= true) or (Enable =:= <<"true">>))).

%% Initialize authz backend.
%% Populate the passed configuration map with necessary data,
%% like `ResourceID`s
-callback(init(source()) -> source()).

%% Get authz text description.
-callback(description() -> string()).

%% Destroy authz backend.
%% Make cleanup of all allocated data.
%% An authz backend will not be used after `destroy`.
-callback(destroy(source()) -> ok).

%% Check if a configuration map is valid for further
%% authz backend initialization.
%% The callback must deallocate all resources allocated
%% during verification.
-callback(dry_run(source()) -> ok | {error, term()}).

%% Authorize client action.
-callback(authorize(
            emqx_types:clientinfo(),
            emqx_types:pubsub(),
            emqx_types:topic(),
            source()) -> match_result()).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?METRICS).

init() ->
    ok = register_metrics(),
    emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    Sources = emqx_conf:get(?CONF_KEY_PATH, []),
    ok = check_dup_types(Sources),
    NSources = init_sources(Sources),
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NSources]}, -1).

deinit() ->
    ok = emqx_hooks:del('client.authorize', {?MODULE, authorize}),
    emqx_conf:remove_handler(?CONF_KEY_PATH),
    emqx_authz_utils:cleanup_resources().

lookup() ->
    {_M, _F, [A]}= find_action_in_hooks(),
    A.

lookup(Type) ->
    {Source, _Front, _Rear} = take(Type),
    Source.

move(Type, Cmd) ->
    move(Type, Cmd, #{}).

move(Type, #{<<"before">> := Before}, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), ?CMD_MOVE_BEFORE(type(Before))}, Opts);
move(Type, #{<<"after">> := After}, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), ?CMD_MOVE_AFTER(type(After))}, Opts);
move(Type, Position, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), Position}, Opts).

update(Cmd, Sources) ->
    update(Cmd, Sources, #{}).

update({?CMD_REPLACE, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{?CMD_REPLACE, type(Type)}, Sources}, Opts);
update({?CMD_DELETE, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{?CMD_DELETE, type(Type)}, Sources}, Opts);
update(Cmd, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {Cmd, Sources}, Opts).

do_update({?CMD_MOVE, Type, ?CMD_MOVE_TOP}, Conf) when is_list(Conf) ->
    {Source, Front, Rear} = take(Type, Conf),
    [Source | Front] ++ Rear;
do_update({?CMD_MOVE, Type, ?CMD_MOVE_BOTTOM}, Conf) when is_list(Conf) ->
    {Source, Front, Rear} = take(Type, Conf),
    Front ++ Rear ++ [Source];
do_update({?CMD_MOVE, Type, ?CMD_MOVE_BEFORE(Before)}, Conf) when is_list(Conf) ->
    {S1, Front1, Rear1} = take(Type, Conf),
    {S2, Front2, Rear2} = take(Before, Front1 ++ Rear1),
    Front2 ++ [S1, S2] ++ Rear2;
do_update({?CMD_MOVE, Type, ?CMD_MOVE_AFTER(After)}, Conf) when is_list(Conf) ->
    {S1, Front1, Rear1} = take(Type, Conf),
    {S2, Front2, Rear2} = take(After, Front1 ++ Rear1),
    Front2 ++ [S2, S1] ++ Rear2;
do_update({?CMD_PREPEND, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Sources ++ Conf,
    ok = check_dup_types(NConf),
    NConf;
do_update({?CMD_APPEND, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Conf ++ Sources,
    ok = check_dup_types(NConf),
    NConf;
do_update({{?CMD_REPLACE, Type}, #{<<"enable">> := Enable} = Source}, Conf)
  when is_map(Source), is_list(Conf), ?IS_BOOL(Enable) ->
    case create_dry_run(Type, Source)  of
        ok ->
            {_Old, Front, Rear} = take(Type, Conf),
            NConf = Front ++ [Source | Rear],
            ok = check_dup_types(NConf),
            NConf;
        {error, _} = Error -> Error
    end;
do_update({{?CMD_REPLACE, Type}, Source}, Conf) when is_map(Source), is_list(Conf) ->
    {_Old, Front, Rear} = take(Type, Conf),
    NConf = Front ++ [Source | Rear],
    ok = check_dup_types(NConf),
    NConf;
do_update({{?CMD_DELETE, Type}, _Source}, Conf) when is_list(Conf) ->
    {_Old, Front, Rear} = take(Type, Conf),
    NConf = Front ++ Rear,
    NConf;
do_update({_, Sources}, _Conf) when is_list(Sources)->
    %% overwrite the entire config!
    Sources;
do_update({Op, Sources}, Conf) ->
    error({bad_request, #{op => Op, sources => Sources, conf => Conf}}).

pre_config_update(_, Cmd, Conf) ->
    {ok, do_update(Cmd, Conf)}.


post_config_update(_, _, undefined, _Conf, _AppEnvs) ->
    ok;
post_config_update(_, Cmd, NewSources, _OldSource, _AppEnvs) ->
    ok = do_post_update(Cmd, NewSources),
    ok = emqx_authz_cache:drain_cache().

do_post_update({?CMD_MOVE, _Type, _Where} = Cmd, _NewSources) ->
    InitedSources = lookup(),
    MovedSources = do_update(Cmd, InitedSources),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [MovedSources]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({?CMD_PREPEND, Sources}, _NewSources) ->
    InitedSources = init_sources(check_sources(Sources)),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources ++ lookup()]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({?CMD_APPEND, Sources}, _NewSources) ->
    InitedSources = init_sources(check_sources(Sources)),
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [lookup() ++ InitedSources]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({{?CMD_REPLACE, Type}, Source}, _NewSources) when is_map(Source) ->
    OldInitedSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldInitedSources),
    ok = ensure_resource_deleted(OldSource),
    InitedSources = init_sources(check_sources([Source])),
    ok = emqx_hooks:put( 'client.authorize'
                       , {?MODULE, authorize, [Front ++ InitedSources ++ Rear]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({{?CMD_DELETE, Type}, _Source}, _NewSources) ->
    OldInitedSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldInitedSources),
    ok = ensure_resource_deleted(OldSource),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Front ++ Rear]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({?CMD_REPLACE, Sources}, _NewSources) ->
    %% overwrite the entire config!
    OldInitedSources = lookup(),
    InitedSources = init_sources(check_sources(Sources)),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources]}, -1),
    lists:foreach(fun ensure_resource_deleted/1, OldInitedSources),
    ok = emqx_authz_cache:drain_cache().

ensure_resource_deleted(#{enable := false}) -> ok;
ensure_resource_deleted(#{type := Type} = Source) ->
    Module = authz_module(Type),
    Module:destroy(Source).

check_dup_types(Sources) ->
    check_dup_types(Sources, []).

check_dup_types([], _Checked) -> ok;
check_dup_types([Source | Sources], Checked) ->
    %% the input might be raw or type-checked result, so lookup both 'type' and <<"type">>
    %% TODO: check: really?
    Type = case maps:get(<<"type">>, Source, maps:get(type, Source, undefined)) of
               undefined ->
                   %% this should never happen if the value is type checked by honcon schema
                   error({bad_source_input, Source});
               Type0 ->
                   type(Type0)
           end,
    case lists:member(Type, Checked) of
        true ->
            %% we have made it clear not to support more than one authz instance for each type
            error({duplicated_authz_source_type, Type});
        false ->
            check_dup_types(Sources, [Type | Checked])
    end.

create_dry_run(Type, Source) ->
    [CheckedSource] = check_sources([Source]),
    Module = authz_module(Type),
    Module:dry_run(CheckedSource).

init_sources(Sources) ->
    {_Enabled, Disabled} = lists:partition(fun(#{enable := Enable}) -> Enable end, Sources),
    case Disabled =/= [] of
        true -> ?SLOG(info, #{msg => "disabled_sources_ignored", sources => Disabled});
        false -> ok
    end,
    lists:map(fun init_source/1, Sources).

init_source(#{enable := false} = Source) -> Source;
init_source(#{type := Type} = Source) ->
    Module = authz_module(Type),
    Module:init(Source).

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize( emqx_types:clientinfo()
               , emqx_types:pubsub()
               , emqx_types:topic()
               , default_result()
               , sources())
      -> authz_result()).
authorize(#{username := Username,
            peerhost := IpAddress
           } = Client, PubSub, Topic, DefaultResult, Sources) ->
    case do_authorize(Client, PubSub, Topic, Sources) of
        {matched, allow} ->
            ?SLOG(info, #{msg => "authorization_permission_allowed",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic}),
            emqx_metrics:inc(?METRIC_ALLOW),
            {stop, allow};
        {matched, deny} ->
            ?SLOG(info, #{msg => "authorization_permission_denied",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic}),
            emqx_metrics:inc(?METRIC_DENY),
            {stop, deny};
        nomatch ->
            ?SLOG(info, #{msg => "authorization_failed_nomatch",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic,
                          reason => "no-match rule"}),
            emqx_metrics:inc(?METRIC_NOMATCH),
            {stop, DefaultResult}
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [#{enable := false} | Rest]) ->
    do_authorize(Client, PubSub, Topic, Rest);
do_authorize(Client, PubSub, Topic,
               [Connector = #{type := Type} | Tail] ) ->
    Module = authz_module(Type),
    case Module:authorize(Client, PubSub, Topic, Connector) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end.

%%--------------------------------------------------------------------
%% Internal function
%%--------------------------------------------------------------------

check_sources(RawSources) ->
    Schema = #{roots => emqx_authz_schema:fields("authorization"), fields => #{}},
    Conf = #{<<"sources">> => RawSources},
    #{sources := Sources} = hocon_schema:check_plain(Schema, Conf, #{atom_key => true}),
    Sources.

take(Type) -> take(Type, lookup()).

%% Take the source of give type, the sources list is split into two parts
%% front part and rear part.
take(Type, Sources) ->
    {Front, Rear} =  lists:splitwith(fun(T) -> type(T) =/= type(Type) end, Sources),
    case Rear =:= [] of
        true ->
            error({authz_source_of_type_not_found, Type});
        _ ->
            {hd(Rear), Front, tl(Rear)}
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

authz_module('built-in-database') ->
    emqx_authz_mnesia;
authz_module(Type) ->
    list_to_existing_atom("emqx_authz_" ++ atom_to_list(Type)).

type(#{type := Type}) -> type(Type);
type(#{<<"type">> := Type}) -> type(Type);
type(file) -> file;
type(<<"file">>) -> file;
type(http) -> http;
type(<<"http">>) -> http;
type(mongodb) -> mongodb;
type(<<"mongodb">>) -> mongodb;
type(mysql) -> mysql;
type(<<"mysql">>) -> mysql;
type(redis) -> redis;
type(<<"redis">>) -> redis;
type(postgresql) -> postgresql;
type(<<"postgresql">>) -> postgresql;
type('built-in-database') -> 'built-in-database';
type(<<"built-in-database">>) -> 'built-in-database';
%% should never happend if the input is type-checked by hocon schema
type(Unknown) -> error({unknown_authz_source_type, Unknown}).

%% @doc where the acl.conf file is stored.
acl_conf_file() ->
    filename:join([emqx:data_dir(), "authz", "acl.conf"]).

ph_to_re(VarPH) ->
    re:replace(VarPH, "[\\$\\{\\}]", "\\\\&", [global, {return, list}]).
