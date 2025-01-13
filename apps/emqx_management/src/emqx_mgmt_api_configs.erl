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

-module(emqx_mgmt_api_configs).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-behaviour(minirest_api).

-export([api_spec/0, namespace/0]).
-export([paths/0, schema/1, fields/1]).

-export([
    config/3,
    config_reset/3,
    configs/3,
    get_full_config/0,
    global_zone_configs/3,
    get_raw_config/1
]).
-export([request_config/3]).

-define(PREFIX, "/configs/").
-define(PREFIX_RESET, "/configs_reset/").
-define(ERR_MSG(MSG), list_to_binary(io_lib:format("~0p", [MSG]))).
-define(OPTS, #{rawconf_with_defaults => true, override_to => cluster}).
-define(TAGS, ["Configs"]).

-if(?EMQX_RELEASE_EDITION == ee).
-define(ROOT_KEYS_EE, [
    <<"file_transfer">>
]).
-else.
-define(ROOT_KEYS_EE, []).
-endif.

-define(ROOT_KEYS, [
    <<"dashboard">>,
    <<"alarm">>,
    <<"sys_topics">>,
    <<"sysmon">>,
    <<"log">>,
    <<"broker">>
    | ?ROOT_KEYS_EE
]).

%% erlfmt-ignore
-define(SYSMON_EXAMPLE,
    <<"
    sysmon {
      os {
        cpu_check_interval = 60s
        cpu_high_watermark = 80%
        cpu_low_watermark = 60%
        mem_check_interval = 60s
        procmem_high_watermark = 5%
        sysmem_high_watermark = 70%
        }
        vm {
        busy_dist_port = true
        busy_port = true
        large_heap = 32MB
        long_gc = disabled
        long_schedule = 240ms
        process_check_interval = 30s
        process_high_watermark = 80%
        process_low_watermark = 60%
        }
    }
    ">>
).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

namespace() -> "configuration".

paths() ->
    [
        "/configs",
        "/configs_reset/:rootname",
        "/configs/global_zone"
    ] ++
        lists:map(fun({Name, _Type}) -> ?PREFIX ++ binary_to_list(Name) end, config_list()).

schema("/configs") ->
    #{
        'operationId' => configs,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_configs),
            parameters => [
                {key,
                    hoconsc:mk(
                        hoconsc:enum([binary_to_atom(K) || K <- emqx_conf_cli:keys()]),
                        #{in => query, example => <<"sysmon">>, required => false}
                    )},
                {node,
                    hoconsc:mk(
                        typerefl:atom(),
                        #{
                            in => query,
                            required => false,
                            description => ?DESC(node_name),
                            hidden => true
                        }
                    )}
            ],
            responses => #{
                200 => #{
                    content =>
                        %% use proplists( not map) to make user text/plain is default in swagger
                        [
                            {'text/plain', #{
                                schema => #{type => string, example => ?SYSMON_EXAMPLE}
                            }},
                            {'application/json', #{
                                schema => #{type => object, example => #{<<"deprecated">> => true}}
                            }}
                        ]
                },
                400 => emqx_dashboard_swagger:error_codes(['INVALID_ACCEPT']),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND']),
                500 => emqx_dashboard_swagger:error_codes(['BAD_NODE'])
            }
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(update_configs),
            parameters => [
                {mode,
                    hoconsc:mk(
                        hoconsc:enum([replace, merge]),
                        #{in => query, default => merge, required => false}
                    )},
                {ignore_readonly,
                    hoconsc:mk(boolean(), #{in => query, default => false, required => false})}
            ],
            'requestBody' => #{
                content =>
                    #{
                        'text/plain' =>
                            #{schema => #{type => string, example => ?SYSMON_EXAMPLE}}
                    }
            },
            responses => #{
                200 => <<"Configurations updated">>,
                400 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED'])
            }
        }
    };
schema("/configs_reset/:rootname") ->
    Paths = lists:map(fun({Path, _}) -> binary_to_atom(Path) end, config_list()),
    #{
        'operationId' => config_reset,
        post => #{
            tags => ?TAGS,
            description => ?DESC(rest_conf_query),
            %% We only return "200" rather than the new configs that has been changed, as
            %% the schema of the changed configs is depends on the request parameter
            %% `conf_path`, it cannot be defined here.
            parameters => [
                {rootname,
                    hoconsc:mk(
                        hoconsc:enum(Paths),
                        #{in => path, example => <<"sysmon">>}
                    )},
                {conf_path,
                    hoconsc:mk(
                        typerefl:binary(),
                        #{
                            in => query,
                            required => false,
                            example => <<"os.sysmem_high_watermark">>,
                            desc => <<"The config path separated by '.' character">>
                        }
                    )}
            ],
            responses => #{
                200 => <<"Rest config successfully">>,
                400 => emqx_dashboard_swagger:error_codes(['NO_DEFAULT_VALUE', 'REST_FAILED']),
                403 => emqx_dashboard_swagger:error_codes(['REST_FAILED'])
            }
        }
    };
schema("/configs/global_zone") ->
    Schema = global_zone_schema(),
    #{
        'operationId' => global_zone_configs,
        get => #{
            tags => ?TAGS,
            description => ?DESC(get_global_zone_configs),
            responses => #{200 => Schema}
        },
        put => #{
            tags => ?TAGS,
            description => ?DESC(update_global_zone_configs),
            'requestBody' => Schema,
            responses => #{
                200 => Schema,
                400 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED']),
                403 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED'])
            }
        }
    };
schema(Path) ->
    {RootKey, {_Root, Schema}} = find_schema(Path),
    GetDesc = iolist_to_binary([
        <<"Get the sub-configurations under *">>,
        RootKey,
        <<"*">>
    ]),
    PutDesc = iolist_to_binary([
        <<"Update the sub-configurations under *">>,
        RootKey,
        <<"*">>
    ]),
    #{
        'operationId' => config,
        get => #{
            tags => ?TAGS,
            desc => GetDesc,
            summary => GetDesc,
            responses => #{
                200 => Schema,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'], <<"config not found">>)
            }
        },
        put => #{
            tags => ?TAGS,
            desc => PutDesc,
            summary => PutDesc,
            'requestBody' => Schema,
            responses => #{
                200 => Schema,
                400 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED', 'INVALID_CONFIG']),
                403 => emqx_dashboard_swagger:error_codes(['UPDATE_FAILED'])
            }
        }
    }.

find_schema(Path) ->
    [_, _Prefix, Root | _] = string:split(Path, "/", all),
    Configs = config_list(),
    lists:keyfind(list_to_binary(Root), 1, Configs).

%% we load all configs from emqx_*_schema, some of them are defined as local ref
%% we need redirect to emqx_*_schema.
%% such as hoconsc:ref("node") to hoconsc:ref(emqx_*_schema, "node")
fields(Field) ->
    Mod = emqx_conf:schema_module(),
    apply(Mod, fields, [Field]).

%%%==============================================================================================
%% HTTP API Callbacks
config(Method, Data, Req) ->
    Path = conf_path(Req),
    request_config(Path, Method, Data).

request_config([ConfigRoot | Path], get, _Params) ->
    [] =/= Path andalso throw("deep config get is not supported"),
    {200, get_raw_config(ConfigRoot)};
request_config(Path, put, #{body := NewConf}) ->
    OldConf = emqx:get_raw_config(Path, #{}),
    UpdateConf = emqx_utils:deobfuscate(NewConf, OldConf),
    case emqx_conf:update(Path, UpdateConf, ?OPTS) of
        {ok, #{raw_config := RawConf}} ->
            [ConfigRoot] = Path,
            {200, obfuscate_raw_config(ConfigRoot, RawConf)};
        {error, Reason} ->
            {400, #{code => 'UPDATE_FAILED', message => ?ERR_MSG(Reason)}}
    end.

global_zone_configs(get, _Params, _Req) ->
    {200, get_zones()};
global_zone_configs(put, #{body := Body}, _Req) ->
    PrevZones = get_zones(),
    {Res, Error} =
        maps:fold(
            fun(Path, Value, {Acc, Error}) ->
                PrevValue = maps:get(Path, PrevZones),
                case Value =/= PrevValue of
                    true ->
                        case emqx_conf:update([Path], Value, ?OPTS) of
                            {ok, #{raw_config := RawConf}} ->
                                {Acc#{Path => RawConf}, Error};
                            {error, Reason} ->
                                ?SLOG(error, #{
                                    msg => "update_global_zone_failed",
                                    reason => Reason,
                                    path => Path,
                                    value => Value
                                }),
                                {Acc, Error#{Path => Reason}}
                        end;
                    false ->
                        {Acc#{Path => Value}, Error}
                end
            end,
            {#{}, #{}},
            Body
        ),
    case maps:size(Res) =:= maps:size(Body) of
        true -> {200, Res};
        false -> {400, #{code => 'UPDATE_FAILED', message => ?ERR_MSG(Error)}}
    end.

config_reset(post, _Params, Req) ->
    %% reset the config specified by the query string param 'conf_path'
    Path = conf_path_reset(Req) ++ conf_path_from_querystr(Req),
    case emqx_conf:reset(Path, ?OPTS) of
        {ok, _} ->
            {200};
        {error, no_default_value} ->
            {400, #{code => 'NO_DEFAULT_VALUE', message => <<"No Default Value.">>}};
        {error, Reason} ->
            {400, #{code => 'REST_FAILED', message => ?ERR_MSG(Reason)}}
    end.

configs(get, #{query_string := QueryStr, headers := Headers}, _Req) ->
    %% Should deprecated json v1 since 5.2.0
    case find_suitable_accept(Headers, [<<"text/plain">>, <<"application/json">>]) of
        {ok, <<"application/json">>} -> get_configs_v1(QueryStr);
        {ok, <<"text/plain">>} -> get_configs_v2(QueryStr);
        {error, _} = Error -> {400, #{code => 'INVALID_ACCEPT', message => ?ERR_MSG(Error)}}
    end;
configs(put, #{body := Conf, query_string := #{<<"mode">> := Mode} = QS}, _Req) ->
    IgnoreReadonly = maps:get(<<"ignore_readonly">>, QS, false),
    case
        emqx_conf_cli:load_config(Conf, #{
            mode => Mode, log => none, ignore_readonly => IgnoreReadonly
        })
    of
        ok ->
            {200};
        %% bad hocon format
        {error, Errors} ->
            Msg = emqx_logger_jsonfmt:best_effort_json_obj(#{errors => Errors}),
            {400, #{<<"content-type">> => <<"text/plain">>}, Msg}
    end.

find_suitable_accept(Headers, Preferences) when is_list(Preferences), length(Preferences) > 0 ->
    AcceptVal = maps:get(<<"accept">>, Headers, <<"*/*">>),
    %% Multiple types, weighted with the quality value syntax:
    %% Accept: text/html, application/xhtml+xml, application/xml;q=0.9, image/webp, */*;q=0.8
    Accepts = lists:map(
        fun(S) ->
            [T | _] = binary:split(string:trim(S), <<";">>),
            T
        end,
        re:split(AcceptVal, ",")
    ),
    case lists:member(<<"*/*">>, Accepts) of
        true ->
            {ok, lists:nth(1, Preferences)};
        false ->
            Found = lists:filter(fun(Accept) -> lists:member(Accept, Accepts) end, Preferences),
            case Found of
                [] -> {error, no_suitable_accept};
                _ -> {ok, lists:nth(1, Found)}
            end
    end.

%% To return a JSON formatted configuration file, which is used to be compatible with the already
%% implemented `GET /configs` in the old versions 5.0 and 5.1.
%%
%% In e5.1.1, we support to return a hocon configuration file by `get_configs_v2/1`. It's more
%% useful for the user to read or reload the configuration file via HTTP API.
%%
%% The `get_configs_v1/1` should be deprecated since 5.2.0.
get_configs_v1(QueryStr) ->
    Node = maps:get(<<"node">>, QueryStr, node()),
    case
        lists:member(Node, emqx:running_nodes()) andalso
            emqx_management_proto_v5:get_full_config(Node)
    of
        false ->
            Message = list_to_binary(io_lib:format("Bad node ~p, reason not found", [Node])),
            {404, #{code => 'NOT_FOUND', message => Message}};
        {badrpc, R} ->
            Message = list_to_binary(io_lib:format("Bad node ~p, reason ~p", [Node, R])),
            {500, #{code => 'BAD_NODE', message => Message}};
        Res ->
            {200, Res}
    end.

get_configs_v2(QueryStr) ->
    Node = maps:get(<<"node">>, QueryStr, node()),
    Conf =
        case maps:find(<<"key">>, QueryStr) of
            error ->
                emqx_conf_proto_v4:get_hocon_config(Node);
            {ok, Key} ->
                emqx_conf_proto_v4:get_hocon_config(Node, atom_to_binary(Key))
        end,
    {
        200,
        #{<<"content-type">> => <<"text/plain">>},
        iolist_to_binary(hocon_pp:do(Conf, #{}))
    }.

conf_path_reset(Req) ->
    <<"/api/v5", ?PREFIX_RESET, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

get_full_config() ->
    emqx_config:fill_defaults(
        maps:with(
            ?ROOT_KEYS,
            emqx:get_raw_config([])
        ),
        #{obfuscate_sensitive_values => true}
    ).

get_raw_config(Path) ->
    obfuscate_raw_config(Path, emqx:get_raw_config([Path])).

obfuscate_raw_config(Path, Raw) ->
    #{Path := Conf} =
        emqx_config:fill_defaults(
            #{Path => Raw},
            #{obfuscate_sensitive_values => true}
        ),
    Conf.

get_zones() ->
    lists:foldl(
        fun(Path, Acc) ->
            maps:merge(Acc, get_config_with_default(Path))
        end,
        #{},
        global_zone_roots()
    ).

get_config_with_default(Path) ->
    emqx_config:fill_defaults(#{Path => emqx:get_raw_config([Path])}).

conf_path_from_querystr(Req) ->
    case proplists:get_value(<<"conf_path">>, cowboy_req:parse_qs(Req)) of
        undefined -> [];
        Path -> string:lexemes(Path, ". ")
    end.

config_list() ->
    Mod = emqx_conf:schema_module(),
    Roots = hocon_schema:roots(Mod),
    lists:foldl(fun(Key, Acc) -> [lists:keyfind(Key, 1, Roots) | Acc] end, [], ?ROOT_KEYS).

conf_path(Req) ->
    <<"/api/v5", ?PREFIX, Path/binary>> = cowboy_req:path(Req),
    string:lexemes(Path, "/ ").

global_zone_roots() ->
    lists:map(fun({K, _}) -> atom_to_binary(K) end, global_zone_schema()).

global_zone_schema() ->
    emqx_zone_schema:global_zone_with_default().
