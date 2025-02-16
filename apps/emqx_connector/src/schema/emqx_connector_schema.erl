%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_connector_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-import(hoconsc, [mk/2, ref/2]).

-export([
    transform_bridges_v1_to_connectors_and_bridges_v2/1,
    transform_bridge_v1_config_to_action_config/4,
    top_level_common_connector_keys/0,
    project_to_connector_resource_opts/1,
    api_ref/3
]).

-export([roots/0, fields/1, desc/1, namespace/0, tags/0]).

-export([get_response/0, put_request/0, post_request/0]).

-export([connector_type_to_bridge_types/1]).

-export([
    api_fields/3,
    common_fields/0,
    connector_values/3,
    status_and_actions_fields/0,
    type_and_name_fields/1
]).

-export([
    common_resource_opts_subfields/0,
    common_resource_opts_subfields_bin/0,
    resource_opts/0,
    resource_opts_fields/0,
    resource_opts_fields/1,
    resource_opts_ref/2,
    resource_opts_ref/3
]).

-export([examples/1]).

api_ref(Module, Type, Method) ->
    {Type, ref(Module, Method)}.

examples(Method) ->
    MergeFun =
        fun(Example, Examples) ->
            maps:merge(Examples, Example)
        end,
    Fun =
        fun(Module, Examples) ->
            ConnectorExamples = erlang:apply(Module, connector_examples, [Method]),
            lists:foldl(MergeFun, Examples, ConnectorExamples)
        end,
    lists:foldl(Fun, #{}, schema_modules()).

schema_modules() ->
    ConnectorTypes = emqx_connector_info:connector_types(),
    [
        emqx_connector_info:schema_module(Type)
     || Type <- ConnectorTypes
    ].

%% @doc Return old bridge(v1) and/or connector(v2) type
%% from the latest connector type name.
connector_type_to_bridge_types(Type) ->
    emqx_connector_info:bridge_types(Type).

actions_config_name(action) -> <<"actions">>;
actions_config_name(source) -> <<"sources">>.

has_connector_field(BridgeConf, ConnectorFields) ->
    lists:any(
        fun({ConnectorFieldName, _Spec}) ->
            maps:is_key(to_bin(ConnectorFieldName), BridgeConf)
        end,
        ConnectorFields
    ).

bridge_configs_to_transform(
    _BridgeType, [] = _BridgeNameBridgeConfList, _ConnectorFields, _RawConfig
) ->
    [];
bridge_configs_to_transform(
    BridgeType, [{BridgeName, BridgeConf} | Rest], ConnectorFields, RawConfig
) ->
    case has_connector_field(BridgeConf, ConnectorFields) of
        true ->
            PreviousRawConfig =
                emqx_utils_maps:deep_get(
                    [<<"actions">>, to_bin(BridgeType), to_bin(BridgeName)],
                    RawConfig,
                    emqx_utils_maps:deep_get(
                        [<<"sources">>, to_bin(BridgeType), to_bin(BridgeName)],
                        RawConfig,
                        undefined
                    )
                ),
            [
                {BridgeType, BridgeName, BridgeConf, ConnectorFields, PreviousRawConfig}
                | bridge_configs_to_transform(BridgeType, Rest, ConnectorFields, RawConfig)
            ];
        false ->
            bridge_configs_to_transform(BridgeType, Rest, ConnectorFields, RawConfig)
    end.

split_bridge_to_connector_and_action(
    {
        {ConnectorsMap, OrgConnectorType},
        {BridgeType, BridgeName, BridgeV1Conf, ConnectorFields, PreviousRawConfig}
    }
) ->
    {ConnectorMap, ConnectorType} =
        case emqx_action_info:has_custom_bridge_v1_config_to_connector_config(BridgeType) of
            true ->
                case
                    emqx_action_info:bridge_v1_config_to_connector_config(
                        BridgeType, BridgeV1Conf
                    )
                of
                    {ConType, ConMap} ->
                        {ConMap, ConType};
                    ConMap ->
                        {ConMap, OrgConnectorType}
                end;
            false ->
                %% We do an automatic transformation to get the connector config
                %% if the callback is not defined.
                %% Get connector fields from bridge config
                NewCConMap =
                    lists:foldl(
                        fun({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
                            ConnectorFieldNameBin = to_bin(ConnectorFieldName),
                            case maps:is_key(ConnectorFieldNameBin, BridgeV1Conf) of
                                true ->
                                    PrevFieldConfig =
                                        maybe_project_to_connector_resource_opts(
                                            ConnectorFieldNameBin,
                                            maps:get(ConnectorFieldNameBin, BridgeV1Conf)
                                        ),
                                    maps:put(
                                        ConnectorFieldNameBin,
                                        PrevFieldConfig,
                                        ToTransformSoFar
                                    );
                                false ->
                                    ToTransformSoFar
                            end
                        end,
                        #{},
                        ConnectorFields
                    ),
                {NewCConMap, OrgConnectorType}
        end,
    %% Generate a connector name, if needed.  Avoid doing so if there was a previous config.
    ConnectorName =
        case PreviousRawConfig of
            #{<<"connector">> := ConnectorName0} -> ConnectorName0;
            _ -> generate_connector_name(ConnectorsMap, BridgeName, 0)
        end,

    OrgActionType = emqx_action_info:bridge_v1_type_to_action_type(BridgeType),
    {ActionMap, ActionType, ActionOrSource} =
        case emqx_action_info:has_custom_bridge_v1_config_to_action_config(BridgeType) of
            true ->
                case
                    emqx_action_info:bridge_v1_config_to_action_config(
                        BridgeType, BridgeV1Conf, ConnectorName
                    )
                of
                    {ActionOrSource0, ActionType0, ActionMap0} ->
                        {ActionMap0, ActionType0, ActionOrSource0};
                    ActionMap0 ->
                        {ActionMap0, OrgActionType, action}
                end;
            false ->
                ActionMap0 =
                    transform_bridge_v1_config_to_action_config(
                        BridgeV1Conf, ConnectorName, ConnectorFields
                    ),
                {ActionMap0, OrgActionType, action}
        end,
    {BridgeType, BridgeName, ActionMap, ActionType, ActionOrSource, ConnectorName, ConnectorMap,
        ConnectorType}.

maybe_project_to_connector_resource_opts(<<"resource_opts">>, OldResourceOpts) ->
    project_to_connector_resource_opts(OldResourceOpts);
maybe_project_to_connector_resource_opts(_, OldConfig) ->
    OldConfig.

project_to_connector_resource_opts(OldResourceOpts) ->
    Subfields = common_resource_opts_subfields_bin(),
    maps:with(Subfields, OldResourceOpts).

transform_bridge_v1_config_to_action_config(
    BridgeV1Conf, ConnectorName, ConnectorConfSchemaMod, ConnectorConfSchemaName
) ->
    ConnectorFields = ConnectorConfSchemaMod:fields(ConnectorConfSchemaName),
    transform_bridge_v1_config_to_action_config(
        BridgeV1Conf, ConnectorName, ConnectorFields
    ).

top_level_common_connector_keys() ->
    [
        <<"enable">>,
        <<"connector">>,
        <<"local_topic">>,
        <<"resource_opts">>,
        <<"description">>,
        <<"parameters">>
    ].

transform_bridge_v1_config_to_action_config(
    BridgeV1Conf, ConnectorName, ConnectorFields
) ->
    TopKeys = top_level_common_connector_keys(),
    TopKeysMap = maps:from_keys(TopKeys, true),
    %% Remove connector fields
    ActionMap0 = lists:foldl(
        fun
            ({enable, _Spec}, ToTransformSoFar) ->
                %% Enable field is used in both
                ToTransformSoFar;
            ({ConnectorFieldName, _Spec}, ToTransformSoFar) ->
                ConnectorFieldNameBin = to_bin(ConnectorFieldName),
                case
                    maps:is_key(ConnectorFieldNameBin, BridgeV1Conf) andalso
                        (not maps:is_key(ConnectorFieldNameBin, TopKeysMap))
                of
                    true ->
                        maps:remove(ConnectorFieldNameBin, ToTransformSoFar);
                    false ->
                        ToTransformSoFar
                end
        end,
        BridgeV1Conf,
        ConnectorFields
    ),
    %% Add the connector field
    ActionMap1 = maps:put(<<"connector">>, ConnectorName, ActionMap0),
    TopMap = maps:with(TopKeys, ActionMap1),
    RestMap = maps:without(TopKeys, ActionMap1),
    %% Other parameters should be stuffed into `parameters'
    emqx_utils_maps:update_if_present(
        <<"resource_opts">>,
        fun emqx_bridge_v2_schema:project_to_actions_resource_opts/1,
        emqx_utils_maps:deep_merge(TopMap, #{<<"parameters">> => RestMap})
    ).

generate_connector_name(ConnectorsMap, BridgeName, Attempt) ->
    ConnectorNameList =
        case Attempt of
            0 ->
                io_lib:format("~s", [BridgeName]);
            _ ->
                io_lib:format("~s_~p", [BridgeName, Attempt + 1])
        end,
    ConnectorName = iolist_to_binary(ConnectorNameList),
    case maps:is_key(ConnectorName, ConnectorsMap) of
        true ->
            generate_connector_name(ConnectorsMap, BridgeName, Attempt + 1);
        false ->
            ConnectorName
    end.

transform_old_style_bridges_to_connector_and_actions_of_type(
    {ConnectorType, #{type := ?MAP(_Name, ?UNION(UnionFun))}},
    RawConfig
) when is_function(UnionFun, 1) ->
    AllMembers = UnionFun(all_union_members),
    lists:foldl(
        fun(
            ?R_REF(ConnectorConfSchemaMod, ConnectorConfSchemaName),
            RawConfigSoFar
        ) ->
            transform_old_style_bridges_to_connector_and_actions_of_type(
                {ConnectorType, #{
                    type => ?MAP(name, ?R_REF(ConnectorConfSchemaMod, ConnectorConfSchemaName))
                }},
                RawConfigSoFar
            )
        end,
        RawConfig,
        AllMembers
    );
transform_old_style_bridges_to_connector_and_actions_of_type(
    {ConnectorType, #{type := ?MAP(_Name, ?R_REF(ConnectorConfSchemaMod, ConnectorConfSchemaName))}},
    RawConfig
) ->
    ConnectorFields = ConnectorConfSchemaMod:fields(ConnectorConfSchemaName),
    BridgeTypes = ?MODULE:connector_type_to_bridge_types(ConnectorType),
    BridgesConfMap = maps:get(<<"bridges">>, RawConfig, #{}),
    ConnectorsConfMap = maps:get(<<"connectors">>, RawConfig, #{}),
    BridgeConfigsToTransform =
        lists:flatmap(
            fun(BridgeType) ->
                BridgeNameToBridgeMap = maps:get(to_bin(BridgeType), BridgesConfMap, #{}),
                BridgeNameBridgeConfList = maps:to_list(BridgeNameToBridgeMap),
                bridge_configs_to_transform(
                    BridgeType, BridgeNameBridgeConfList, ConnectorFields, RawConfig
                )
            end,
            BridgeTypes
        ),
    ConnectorsWithTypeMap = maps:get(to_bin(ConnectorType), ConnectorsConfMap, #{}),
    BridgeConfigsToTransformWithConnectorConf = lists:zip(
        lists:duplicate(
            length(BridgeConfigsToTransform),
            {ConnectorsWithTypeMap, ConnectorType}
        ),
        BridgeConfigsToTransform
    ),
    ActionConnectorTuples = lists:map(
        fun split_bridge_to_connector_and_action/1,
        BridgeConfigsToTransformWithConnectorConf
    ),
    %% Add connectors and actions and remove bridges
    lists:foldl(
        fun(
            {BridgeType, BridgeName, ActionMap, NewActionType, ActionOrSource, ConnectorName,
                ConnectorMap, NewConnectorType},
            RawConfigSoFar
        ) ->
            %% Add connector
            RawConfigSoFar1 = emqx_utils_maps:deep_put(
                [<<"connectors">>, to_bin(NewConnectorType), ConnectorName],
                RawConfigSoFar,
                ConnectorMap
            ),
            %% Remove bridge (v1)
            RawConfigSoFar2 = emqx_utils_maps:deep_remove(
                [<<"bridges">>, to_bin(BridgeType), BridgeName],
                RawConfigSoFar1
            ),
            %% Add action
            RawConfigSoFar3 =
                case ActionMap of
                    none ->
                        RawConfigSoFar2;
                    _ ->
                        emqx_utils_maps:deep_put(
                            [
                                actions_config_name(ActionOrSource),
                                to_bin(NewActionType),
                                BridgeName
                            ],
                            RawConfigSoFar2,
                            ActionMap
                        )
                end,
            RawConfigSoFar3
        end,
        RawConfig,
        ActionConnectorTuples
    );
transform_old_style_bridges_to_connector_and_actions_of_type(
    {_ConnectorType, #{type := ?MAP(_Name, _)}},
    RawConfig
) ->
    RawConfig.

transform_bridges_v1_to_connectors_and_bridges_v2(RawConfig) ->
    ConnectorFields = ?MODULE:fields(connectors),
    NewRawConf = lists:foldl(
        fun transform_old_style_bridges_to_connector_and_actions_of_type/2,
        RawConfig,
        ConnectorFields
    ),
    NewRawConf.

%%======================================================================================
%% HOCON Schema Callbacks
%%======================================================================================

%% For HTTP APIs
get_response() ->
    api_schema("get").

put_request() ->
    api_schema("put").

post_request() ->
    api_schema("post").

api_schema(Method) ->
    InfoModSchemas = emqx_connector_info_api_schemas(Method),
    hoconsc:union(connector_api_union(InfoModSchemas)).

emqx_connector_info_api_schemas(Method) ->
    ConnectorTypes = emqx_connector_info:connector_types(),
    [
        emqx_connector_info:api_schema(Type, Method)
     || Type <- ConnectorTypes
    ].

connector_api_union(Refs) ->
    Index = maps:from_list(Refs),
    fun
        (all_union_members) ->
            maps:values(Index);
        ({value, V}) ->
            case V of
                #{<<"type">> := T} ->
                    case maps:get(T, Index, undefined) of
                        undefined ->
                            throw(#{
                                field_name => type,
                                value => T,
                                reason => <<"unknown connector type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    maps:values(Index)
            end
    end.

%% general config
namespace() -> "connector".

tags() ->
    [<<"Connector">>].

-dialyzer({nowarn_function, roots/0}).

roots() ->
    %% TODO: drop this clause and check
    case fields(connectors) of
        [] ->
            [
                {connectors,
                    ?HOCON(
                        hoconsc:map(name, typerefl:map()),
                        #{
                            importance => ?IMPORTANCE_LOW,
                            validator => fun validator/1
                        }
                    )}
            ];
        _ ->
            [
                {connectors,
                    ?HOCON(
                        ?R_REF(connectors),
                        #{
                            importance => ?IMPORTANCE_LOW,
                            validator => fun validator/1
                        }
                    )}
            ]
    end.

fields(connectors) ->
    connector_info_fields_connectors();
fields(resource_opts) ->
    resource_opts_fields();
fields("node_status") ->
    [
        node_name(),
        {"status", mk(status(), #{})},
        {"status_reason",
            mk(binary(), #{
                required => false,
                desc => ?DESC("desc_status_reason"),
                example => <<"Connection refused">>
            })}
    ].

connector_info_fields_connectors() ->
    ConnectorTypes = emqx_connector_info:connector_types(),
    [
        emqx_connector_info:config_schema(Type)
     || Type <- ConnectorTypes
    ].

desc(connectors) ->
    ?DESC("desc_connectors");
desc("node_status") ->
    ?DESC("desc_node_status");
desc(resource_opts) ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_) ->
    undefined.

api_fields("get_connector", Type, Fields) ->
    lists:append(
        [
            type_and_name_fields(Type),
            common_fields(),
            status_and_actions_fields(),
            Fields
        ]
    );
api_fields("post_connector", Type, Fields) ->
    lists:append(
        [
            type_and_name_fields(Type),
            common_fields(),
            Fields
        ]
    );
api_fields("put_connector", _Type, Fields) ->
    lists:append(
        [
            common_fields(),
            Fields
        ]
    ).

common_fields() ->
    [
        {enable,
            mk(boolean(), #{
                desc => ?DESC("config_enable"),
                importance => ?IMPORTANCE_NO_DOC,
                default => true
            })},
        {tags, emqx_schema:tags_schema()},
        {description, emqx_schema:description_schema()}
    ].

type_and_name_fields(ConnectorType) ->
    [
        {type, mk(ConnectorType, #{required => true, desc => ?DESC("desc_type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}
    ].

status_and_actions_fields() ->
    [
        {"status", mk(status(), #{desc => ?DESC("desc_status")})},
        {"status_reason",
            mk(binary(), #{
                required => false,
                desc => ?DESC("desc_status_reason"),
                example => <<"Connection refused">>
            })},
        {"node_status",
            mk(
                hoconsc:array(ref(?MODULE, "node_status")),
                #{desc => ?DESC("desc_node_status")}
            )},
        {"actions",
            mk(
                hoconsc:array(binary()),
                #{
                    desc => ?DESC("connector_actions"),
                    example => [<<"my_action">>]
                }
            )}
    ].
resource_opts_ref(Module, RefName) ->
    resource_opts_ref(Module, RefName, undefined).

resource_opts_ref(Module, RefName, ConverterFun) ->
    Meta =
        case ConverterFun of
            undefined ->
                emqx_resource_schema:resource_opts_meta();
            _ ->
                M = emqx_resource_schema:resource_opts_meta(),
                M#{converter => ConverterFun}
        end,
    [
        {resource_opts,
            mk(
                ref(Module, RefName),
                Meta
            )}
    ].

common_resource_opts_subfields() ->
    [
        health_check_interval,
        start_after_created,
        start_timeout
    ].

common_resource_opts_subfields_bin() ->
    lists:map(fun atom_to_binary/1, common_resource_opts_subfields()).

resource_opts() ->
    resource_opts_ref(?MODULE, resource_opts).

resource_opts_fields() ->
    resource_opts_fields(_Overrides = []).

resource_opts_fields(Overrides) ->
    %% Note: these don't include buffer-related configurations because buffer workers are
    %% tied to the action.
    ConnectorROFields = common_resource_opts_subfields(),
    lists:filter(
        fun({Key, _Sc}) -> lists:member(Key, ConnectorROFields) end,
        emqx_resource_schema:create_opts(Overrides)
    ).

-type http_method() :: get | post | put.
-type schema_example_map() :: #{atom() => term()}.

-spec connector_values(http_method(), atom(), schema_example_map()) -> schema_example_map().
connector_values(Method, Type, ConnectorValues) ->
    TypeBin = atom_to_binary(Type),
    lists:foldl(
        fun(M1, M2) ->
            maps:merge(M1, M2)
        end,
        #{
            description => <<"My example ", TypeBin/binary, " connector">>
        },
        [
            ConnectorValues,
            method_values(Method, Type)
        ]
    ).

method_values(post, Type) ->
    TypeBin = atom_to_binary(Type),
    #{
        name => <<TypeBin/binary, "_connector">>,
        type => TypeBin
    };
method_values(get, Type) ->
    maps:merge(
        method_values(post, Type),
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        }
    );
method_values(put, _Type) ->
    #{}.

%%======================================================================================
%% Helper Functions
%%======================================================================================

to_bin(Atom) when is_atom(Atom) ->
    list_to_binary(atom_to_list(Atom));
to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Something) ->
    Something.

node_name() ->
    {"node", mk(binary(), #{desc => ?DESC("desc_node_name"), example => "emqx@127.0.0.1"})}.

status() ->
    hoconsc:enum([connected, disconnected, connecting, inconsistent]).

validator(ConnectorsRoot) ->
    ValidatorFns = emqx_schema_hooks:list_injection_point('connectors.validators', []),
    emqx_utils:foldl_while(
        fun(Fn, _Acc) ->
            case Fn(ConnectorsRoot) of
                ok ->
                    {cont, ok};
                true ->
                    {cont, ok};
                Error ->
                    {halt, Error}
            end
        end,
        ok,
        ValidatorFns
    ).

-ifdef(TEST).
-include_lib("hocon/include/hocon_types.hrl").
schema_homogeneous_test() ->
    case
        lists:foldl(
            fun({_Name, Schema}, Bads) ->
                is_bad_schema(Schema, Bads)
            end,
            [],
            fields(connectors)
        )
    of
        [] ->
            ok;
        List ->
            throw(List)
    end.

is_bad_schema(#{type := ?MAP(_, ?R_REF(Module, TypeName))}, Bads) ->
    is_bad_schema_type(Module, TypeName, Bads);
is_bad_schema(#{type := ?MAP(_, ?UNION(Types))}, Bads) when is_list(Types) ->
    is_bad_schema_types(Types, Bads);
is_bad_schema(#{type := ?MAP(_, ?UNION(Func))}, Bads) when is_function(Func, 1) ->
    Types = Func(all_union_members),
    is_bad_schema_types(Types, Bads).

is_bad_schema_types(Types, Bads) ->
    lists:foldl(
        fun
            (?R_REF(Module, TypeName), Acc) ->
                is_bad_schema_type(Module, TypeName, Acc);
            (Type, Acc) ->
                [
                    #{
                        type => Type
                    }
                    | Acc
                ]
        end,
        Bads,
        Types
    ).

is_bad_schema_type(Module, TypeName, Bads) ->
    Fields = Module:fields(TypeName),
    ExpectedFieldNames = common_field_names(),
    MissingFields = lists:filter(
        fun(Name) -> lists:keyfind(Name, 1, Fields) =:= false end, ExpectedFieldNames
    ),
    case MissingFields of
        [] ->
            Bads;
        _ ->
            [
                #{
                    schema_module => Module,
                    type_name => TypeName,
                    missing_fields => MissingFields
                }
                | Bads
            ]
    end.

common_field_names() ->
    [
        enable, description
    ].

-endif.
