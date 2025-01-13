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

-module(emqx_mgmt_api_listeners).

-behaviour(minirest_api).

-export([namespace/0, api_spec/0, paths/0, schema/1, fields/1]).

-export([
    listener_type_status/2,
    list_listeners/2,
    crud_listeners_by_id/2,
    stop_listeners_by_id/2,
    start_listeners_by_id/2,
    restart_listeners_by_id/2,
    action_listeners_by_id/2
]).

%% for rpc call
-export([
    do_list_listeners/0
]).

-import(emqx_dashboard_swagger, [error_codes/2, error_codes/1]).

-import(emqx_mgmt_listeners_conf, [
    action/4,
    create/3,
    ensure_remove/2,
    get_raw/2,
    update/3
]).

-include_lib("hocon/include/hoconsc.hrl").

-define(LISTENER_NOT_FOUND, <<"Listener id not found">>).
-define(LISTENER_ID_INCONSISTENT, <<"Path and body's listener id not match">>).

namespace() -> "listeners".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/listeners_status",
        "/listeners",
        "/listeners/:id",
        "/listeners/:id/stop",
        "/listeners/:id/start",
        "/listeners/:id/restart"
    ].

schema("/listeners_status") ->
    #{
        'operationId' => listener_type_status,
        get => #{
            tags => [<<"listeners">>],
            description => ?DESC(list_node_live_statuses),
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?ARRAY(?R_REF(listener_type_status)),
                        listener_type_status_example()
                    )
            }
        }
    };
schema("/listeners") ->
    #{
        'operationId' => list_listeners,
        get => #{
            tags => [<<"listeners">>],
            description => ?DESC(list_listeners),
            parameters => [
                {type,
                    ?HOCON(
                        ?ENUM(listeners_type()),
                        #{desc => "Listener type", in => query, required => false, example => tcp}
                    )}
            ],
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(
                        ?ARRAY(?R_REF(listener_id_status)),
                        listener_id_status_example()
                    )
            }
        },
        post => #{
            tags => [<<"listeners">>],
            description => ?DESC(create_on_all_nodes),
            parameters => [],
            'requestBody' => create_listener_schema(#{bind => true}),
            responses => #{
                200 => listener_schema(#{bind => true}),
                400 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'])
            }
        }
    };
schema("/listeners/:id") ->
    #{
        'operationId' => crud_listeners_by_id,
        get => #{
            tags => [<<"listeners">>],
            description => ?DESC(list_by_id),
            parameters => [?R_REF(listener_id)],
            responses => #{
                200 => listener_schema(#{bind => true}),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        put => #{
            tags => [<<"listeners">>],
            description => ?DESC(update_lisener),
            parameters => [?R_REF(listener_id)],
            'requestBody' => listener_schema(#{bind => false}),
            responses => #{
                200 => listener_schema(#{bind => true}),
                400 => error_codes(['BAD_REQUEST']),
                404 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'], ?LISTENER_NOT_FOUND)
            }
        },
        post => #{
            tags => [<<"listeners">>],
            description => ?DESC(create_on_all_nodes),
            parameters => [?R_REF(listener_id)],
            'requestBody' => listener_schema(#{bind => true}),
            responses => #{
                200 => listener_schema(#{bind => true}),
                400 => error_codes(['BAD_LISTENER_ID', 'BAD_REQUEST'])
            },
            deprecated => true
        },
        delete => #{
            tags => [<<"listeners">>],
            description => ?DESC(delete_on_all_nodes),
            parameters => [?R_REF(listener_id)],
            responses => #{
                204 => <<"Listener deleted">>,
                404 => error_codes(['BAD_LISTENER_ID'])
            }
        }
    };
schema("/listeners/:id/start") ->
    #{
        'operationId' => start_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            description => ?DESC(start_on_all_nodes),
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    };
schema("/listeners/:id/stop") ->
    #{
        'operationId' => stop_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            description => ?DESC(stop_on_all_nodes),
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    };
schema("/listeners/:id/restart") ->
    #{
        'operationId' => restart_listeners_by_id,
        post => #{
            tags => [<<"listeners">>],
            description => ?DESC(restart_on_all_nodes),
            parameters => [
                ?R_REF(listener_id)
            ],
            responses => #{
                200 => <<"Updated">>,
                400 => error_codes(['BAD_REQUEST', 'BAD_LISTENER_ID'])
            }
        }
    }.

fields(listener_id) ->
    [
        {id,
            ?HOCON(atom(), #{
                desc => "Listener id",
                example => 'tcp:demo',
                validator => fun validate_id/1,
                required => true,
                in => path
            })}
    ];
fields(node) ->
    [
        {"node",
            ?HOCON(atom(), #{
                desc => "Node name",
                example => "emqx@127.0.0.1",
                in => path
            })}
    ];
fields(listener_type_status) ->
    [
        {type, ?HOCON(?ENUM(listeners_type()), #{desc => "Listener type", required => true})},
        {enable, ?HOCON(boolean(), #{desc => "Listener enable", required => true})},
        {ids, ?HOCON(?ARRAY(string()), #{desc => "Listener Ids", required => true})},
        {status, ?HOCON(?R_REF(status))},
        {node_status, ?HOCON(?ARRAY(?R_REF(node_status)))}
    ];
fields(listener_id_status) ->
    fields(listener_id) ++
        [
            {type, ?HOCON(?ENUM(listeners_type()), #{desc => "Listener type", required => true})},
            {name, ?HOCON(string(), #{desc => "Listener name", required => true})},
            {enable, ?HOCON(boolean(), #{desc => "Listener enable", required => true})},
            {number, ?HOCON(typerefl:pos_integer(), #{desc => "ListenerId counter"})},
            {bind,
                ?HOCON(
                    emqx_schema:ip_port(),
                    #{desc => "Listener bind addr", required => true}
                )},
            {acceptors, ?HOCON(typerefl:pos_integer(), #{desc => "ListenerId acceptors"})},
            {status, ?HOCON(?R_REF(status))},
            {node_status, ?HOCON(?ARRAY(?R_REF(node_status)))}
        ];
fields(status) ->
    [
        {running,
            ?HOCON(
                hoconsc:union([inconsistent, boolean()]),
                #{desc => "Listener running status", required => true}
            )},
        {max_connections,
            ?HOCON(hoconsc:union([infinity, integer()]), #{desc => "Max connections"})},
        {current_connections, ?HOCON(non_neg_integer(), #{desc => "Current connections"})}
    ];
fields(node_status) ->
    [
        {"node",
            ?HOCON(atom(), #{
                desc => "Node name",
                example => "emqx@127.0.0.1"
            })},
        {status, ?HOCON(?R_REF(status))}
    ];
fields("with_name_" ++ Type) ->
    listener_struct_with_name(Type);
fields(Type) ->
    listener_struct(Type).

listener_schema(Opts) ->
    emqx_dashboard_swagger:schema_with_example(
        hoconsc:union(listener_union_member_selector(Opts)),
        tcp_schema_example()
    ).

listener_union_member_selector(Opts) ->
    ListenersInfo = listeners_info(Opts),
    Index = maps:from_list([
        {iolist_to_binary(ListenerType), Ref}
     || #{listener_type := ListenerType, ref := Ref} <- ListenersInfo
    ]),
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
                                reason => <<"unknown listener type">>
                            });
                        Ref ->
                            [Ref]
                    end;
                _ ->
                    throw(#{
                        field_name => type,
                        reason => <<"unknown listener type">>
                    })
            end
    end.

create_listener_schema(Opts) ->
    Schemas = [
        ?R_REF(Mod, "with_name_" ++ Type)
     || #{ref := ?R_REF(Mod, Type)} <- listeners_info(Opts)
    ],
    Example = maps:remove(id, tcp_schema_example()),
    emqx_dashboard_swagger:schema_with_example(
        hoconsc:union(Schemas),
        Example#{name => <<"demo">>}
    ).

listeners_type() ->
    lists:map(fun({Type, _}) -> list_to_existing_atom(Type) end, emqx_schema:listeners()).

listeners_info(Opts) ->
    Listeners = emqx_schema:listeners(),
    lists:map(
        fun({ListenerType, Schema}) ->
            Type = emqx_schema:get_tombstone_map_value_type(Schema),
            ?R_REF(Mod, StructName) = Type,
            Fields0 = hocon_schema:fields(Mod, StructName),
            Fields1 = lists:keydelete("authentication", 1, Fields0),
            Fields3 = required_bind(Fields1, Opts),
            Ref = listeners_ref(ListenerType, Opts),
            TypeAtom = list_to_existing_atom(ListenerType),
            #{
                ref => ?R_REF(Ref),
                listener_type => ListenerType,
                schema => [
                    {type, ?HOCON(?ENUM([TypeAtom]), #{desc => "Listener type", required => true})},
                    {running, ?HOCON(boolean(), #{desc => "Listener status", required => false})},
                    {id,
                        ?HOCON(atom(), #{
                            desc => "Listener id",
                            required => true,
                            validator => fun validate_id/1
                        })},
                    {current_connections,
                        ?HOCON(
                            non_neg_integer(),
                            #{desc => "Current connections", required => false}
                        )}
                    | Fields3
                ]
            }
        end,
        Listeners
    ).

required_bind(Fields, #{bind := true}) ->
    Fields;
required_bind(Fields, #{bind := false}) ->
    {value, {_, Hocon}, Fields1} = lists:keytake("bind", 1, Fields),
    [{"bind", Hocon#{required => false}} | Fields1].

listeners_ref(Type, #{bind := Bind}) ->
    Suffix =
        case Bind of
            true -> "_required_bind";
            false -> "_not_required_bind"
        end,
    Type ++ Suffix.

validate_id(Id) ->
    case emqx_listeners:parse_listener_id(Id) of
        {error, Reason} -> {error, Reason};
        {ok, _} -> ok
    end.

%% api
listener_type_status(get, _Request) ->
    Listeners = maps:to_list(listener_status_by_type(list_listeners(), #{})),
    List = lists:map(
        fun({Type, L}) ->
            L1 = maps:without([bind, acceptors, name], L),
            L1#{type => Type}
        end,
        Listeners
    ),
    {200, List}.

list_listeners(get, #{query_string := Query}) ->
    Listeners = list_listeners(),
    NodeL =
        case maps:find(<<"type">>, Query) of
            {ok, Type} -> listener_type_filter(atom_to_binary(Type), Listeners);
            error -> Listeners
        end,
    {200, listener_status_by_id(NodeL)};
list_listeners(post, #{body := Body}) ->
    create_listener(name, Body).

crud_listeners_by_id(get, #{bindings := #{id := Id}}) ->
    case find_listeners_by_id(Id) of
        [] -> {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
        [L] -> {200, L}
    end;
crud_listeners_by_id(put, #{bindings := #{id := Id}, body := Body0}) ->
    case parse_listener_conf(id, Body0) of
        {Id, Type, Name, Conf} ->
            case get_raw(Type, Name) of
                undefined ->
                    {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                PrevConf ->
                    MergeConf = emqx_utils_maps:deep_merge(PrevConf, Conf),
                    case update(Type, Name, MergeConf) of
                        {ok, #{raw_config := _RawConf}} ->
                            crud_listeners_by_id(get, #{bindings => #{id => Id}});
                        {error, not_found} ->
                            {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                        {error, Reason} ->
                            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
                    end
            end;
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}};
        _ ->
            {400, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_ID_INCONSISTENT}}
    end;
crud_listeners_by_id(post, #{body := Body}) ->
    create_listener(id, Body);
crud_listeners_by_id(delete, #{bindings := #{id := Id}}) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case find_listeners_by_id(Id) of
        [_L] ->
            {ok, _Res} = ensure_remove(Type, Name),
            {204};
        [] ->
            {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}}
    end.

parse_listener_conf(id, Conf0) ->
    Conf1 = maps:without([<<"running">>, <<"current_connections">>], Conf0),
    {TypeBin, Conf2} = maps:take(<<"type">>, Conf1),
    TypeAtom = binary_to_existing_atom(TypeBin),
    case maps:take(<<"id">>, Conf2) of
        {IdBin, Conf3} ->
            {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(IdBin),
            case Type =:= TypeAtom of
                true -> {binary_to_existing_atom(IdBin), TypeAtom, Name, Conf3};
                false -> {error, listener_type_inconsistent}
            end;
        _ ->
            {error, listener_config_invalid}
    end;
parse_listener_conf(name, Conf0) ->
    Conf1 = maps:without([<<"running">>, <<"current_connections">>], Conf0),
    {TypeBin, Conf2} = maps:take(<<"type">>, Conf1),
    TypeAtom = binary_to_existing_atom(TypeBin),
    case maps:take(<<"name">>, Conf2) of
        {Name, Conf3} ->
            IdBin = <<TypeBin/binary, $:, Name/binary>>,
            {binary_to_atom(IdBin), TypeAtom, Name, Conf3};
        _ ->
            {error, listener_config_invalid}
    end.

stop_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, stop, Bindings)}
    ).
start_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, start, Bindings)}
    ).
restart_listeners_by_id(Method, Body = #{bindings := Bindings}) ->
    action_listeners_by_id(
        Method,
        Body#{bindings := maps:put(action, restart, Bindings)}
    ).

action_listeners_by_id(post, #{bindings := #{id := Id, action := Action}}) ->
    {ok, #{type := Type, name := Name}} = emqx_listeners:parse_listener_id(Id),
    case get_raw(Type, Name) of
        undefined ->
            {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
        _PrevConf ->
            case action(Type, Name, Action, enabled(Action)) of
                {ok, #{raw_config := _RawConf}} ->
                    {200};
                {error, not_found} ->
                    {404, #{code => 'BAD_LISTENER_ID', message => ?LISTENER_NOT_FOUND}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
            end
    end.

%%%==============================================================================================

enabled(start) -> #{<<"enable">> => true};
enabled(stop) -> #{<<"enable">> => false};
enabled(restart) -> #{<<"enable">> => true}.

err_msg(Atom) when is_atom(Atom) -> atom_to_binary(Atom);
err_msg(Reason) -> list_to_binary(err_msg_str(Reason)).

err_msg_str(Reason) ->
    io_lib:format("~p", [Reason]).

list_listeners() ->
    %% prioritize displaying the bind of the current node
    %% when each node's same type's bind is different.
    %% e.g: tcp bind on node1 is 1883, but node2 is 1884.
    Self = node(),
    lists:map(fun list_listeners/1, [Self | lists:delete(Self, emqx:running_nodes())]).

list_listeners(Node) ->
    wrap_rpc(emqx_management_proto_v5:list_listeners(Node)).

listener_status_by_id(NodeL) ->
    Listeners = maps:to_list(listener_status_by_id(NodeL, #{})),
    lists:map(
        fun({Id, L}) ->
            L1 = maps:remove(ids, L),
            #{node_status := Nodes} = L1,
            L1#{number => length(Nodes), id => Id}
        end,
        Listeners
    ).

listener_status_by_type([], Acc) ->
    Acc;
listener_status_by_type([NodeL | Rest], Acc) ->
    #{<<"node">> := Node, <<"listeners">> := Listeners} = NodeL,
    Acc1 = lists:foldl(
        fun(L, Acc0) -> format_status(<<"type">>, Node, L, Acc0) end,
        Acc,
        Listeners
    ),
    listener_status_by_type(Rest, Acc1).

listener_status_by_id([], Acc) ->
    Acc;
listener_status_by_id([NodeL | Rest], Acc) ->
    #{<<"node">> := Node, <<"listeners">> := Listeners} = NodeL,
    Acc1 = lists:foldl(
        fun(L, Acc0) -> format_status(<<"id">>, Node, L, Acc0) end,
        Acc,
        Listeners
    ),
    listener_status_by_id(Rest, Acc1).

listener_type_filter(Type0, Listeners) ->
    lists:map(
        fun(Conf = #{<<"listeners">> := Listeners0}) ->
            Conf#{
                <<"listeners">> =>
                    [C || C = #{<<"type">> := Type} <- Listeners0, Type =:= Type0]
            }
        end,
        Listeners
    ).

-spec do_list_listeners() -> map().
do_list_listeners() ->
    Listeners = [
        Conf#{<<"id">> => Id, <<"type">> => Type}
     || {Id, Type, Conf} <- emqx_listeners:list_raw()
    ],
    #{
        <<"node">> => node(),
        <<"listeners">> => Listeners
    }.

find_listeners_by_id(Id) ->
    [
        Conf#{
            <<"id">> => Id0,
            <<"type">> => Type,
            <<"bind">> := iolist_to_binary(
                emqx_listeners:format_bind(maps:get(<<"bind">>, Conf))
            )
        }
     || {Id0, Type, Conf} <- emqx_listeners:list_raw(),
        Id0 =:= Id
    ].

wrap_rpc({badrpc, Reason}) ->
    {error, Reason};
wrap_rpc(Res) ->
    Res.

format_status(Key, Node, Listener, Acc) ->
    #{
        <<"id">> := Id,
        <<"type">> := Type,
        <<"enable">> := Enable,
        <<"running">> := Running,
        <<"max_connections">> := MaxConnections,
        <<"current_connections">> := CurrentConnections,
        <<"acceptors">> := Acceptors,
        <<"bind">> := Bind
    } = Listener,
    {ok, #{name := Name}} = emqx_listeners:parse_listener_id(Id),
    GroupKey = maps:get(Key, Listener),
    case maps:find(GroupKey, Acc) of
        error ->
            Acc#{
                GroupKey => #{
                    name => Name,
                    type => Type,
                    enable => Enable,
                    ids => [Id],
                    acceptors => Acceptors,
                    bind => iolist_to_binary(emqx_listeners:format_bind(Bind)),
                    status => #{
                        running => Running,
                        max_connections => MaxConnections,
                        current_connections => CurrentConnections
                    },
                    node_status => [
                        #{
                            node => Node,
                            status => #{
                                running => Running,
                                max_connections => MaxConnections,
                                current_connections => CurrentConnections
                            }
                        }
                    ]
                }
            };
        {ok, GroupValue} ->
            #{
                ids := Ids,
                status := #{
                    running := Running0,
                    max_connections := MaxConnections0,
                    current_connections := CurrentConnections0
                },
                node_status := NodeStatus0
            } = GroupValue,
            NodeStatus = [
                #{
                    node => Node,
                    status => #{
                        running => Running,
                        max_connections => MaxConnections,
                        current_connections => CurrentConnections
                    }
                }
                | NodeStatus0
            ],
            NRunning =
                case Running == Running0 of
                    true -> Running0;
                    _ -> inconsistent
                end,
            Acc#{
                GroupKey =>
                    GroupValue#{
                        ids => lists:usort([Id | Ids]),
                        status => #{
                            running => NRunning,
                            max_connections => max_conn(MaxConnections0, MaxConnections),
                            current_connections => CurrentConnections0 + CurrentConnections
                        },
                        node_status => NodeStatus
                    }
            }
    end.

max_conn(_Int1, <<"infinity">>) -> <<"infinity">>;
max_conn(<<"infinity">>, _Int) -> <<"infinity">>;
max_conn(Int1, Int2) -> Int1 + Int2.

listener_type_status_example() ->
    [
        #{
            enable => false,
            ids => ["tcp:demo"],
            node_status =>
                [
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 11,
                            max_connections => 1024000
                        }
                    },
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 10,
                            max_connections => 1024000
                        }
                    }
                ],
            status => #{
                running => true,
                current_connections => 21,
                max_connections => 2048000
            },
            type => tcp
        },
        #{
            enable => false,
            ids => ["ssl:default"],
            node_status =>
                [
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 31,
                            max_connections => infinity
                        }
                    },
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 40,
                            max_connections => infinity
                        }
                    }
                ],

            status => #{
                running => true,
                current_connections => 71,
                max_connections => infinity
            },
            type => ssl
        }
    ].

listener_id_status_example() ->
    [
        #{
            acceptors => 16,
            bind => <<"0.0.0.0:1884">>,
            enable => true,
            id => <<"tcp:demo">>,
            type => <<"tcp">>,
            name => <<"demo">>,
            node_status =>
                [
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 100,
                            max_connections => 1024000
                        }
                    },
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 101,
                            max_connections => 1024000
                        }
                    }
                ],
            number => 2,
            status => #{
                running => true,
                current_connections => 201,
                max_connections => 2048000
            }
        },
        #{
            acceptors => 32,
            bind => <<"0.0.0.0:1883">>,
            enable => true,
            id => <<"tcp:default">>,
            type => <<"tcp">>,
            name => <<"default">>,
            node_status =>
                [
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 200,
                            max_connections => infinity
                        }
                    },
                    #{
                        node => 'emqx@127.0.0.1',
                        status => #{
                            running => true,
                            current_connections => 301,
                            max_connections => infinity
                        }
                    }
                ],
            number => 2,
            status => #{
                running => true,
                current_connections => 501,
                max_connections => infinity
            }
        }
    ].

tcp_schema_example() ->
    #{
        type => tcp,
        acceptors => 16,
        access_rules => ["allow all"],
        bind => <<"0.0.0.0:1884">>,
        current_connections => 10240,
        id => <<"tcp:demo">>,
        max_connections => 204800,
        mountpoint => <<"/">>,
        proxy_protocol => false,
        proxy_protocol_timeout => <<"3s">>,
        running => true,
        zone => default,
        tcp_options => #{
            active_n => 100,
            backlog => 1024,
            buffer => <<"4KB">>,
            high_watermark => <<"1MB">>,
            nodelay => false,
            reuseaddr => true,
            send_timeout => <<"15s">>,
            send_timeout_close => true
        }
    }.

create_listener(From, Body) ->
    case parse_listener_conf(From, Body) of
        {Id, Type, Name, Conf} ->
            case create(Type, Name, Conf) of
                {ok, #{raw_config := _RawConf}} ->
                    crud_listeners_by_id(get, #{bindings => #{id => Id}});
                {error, already_exist} ->
                    {400, #{code => 'BAD_LISTENER_ID', message => <<"Already Exist">>}};
                {error, Reason} ->
                    {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
            end;
        {error, Reason} ->
            {400, #{code => 'BAD_REQUEST', message => err_msg(Reason)}}
    end.

listener_struct(Type) ->
    Listeners = listeners_info(#{bind => true}) ++ listeners_info(#{bind => false}),
    [Schema] = [S || #{ref := ?R_REF(_, T), schema := S} <- Listeners, T =:= Type],
    Schema.

listener_struct_with_name(Type) ->
    BaseSchema = listener_struct(Type),
    lists:keyreplace(
        id,
        1,
        BaseSchema,
        {name,
            ?HOCON(binary(), #{
                desc => "Listener name",
                required => true
            })}
    ).
