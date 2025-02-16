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
-module(emqx_bridge_mqtt_connector).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(emqx_resource).
-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

-export([on_message_received/3]).
-export([handle_disconnect/1]).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1
]).

-export([on_async_result/2]).

-type name() :: term().

-type option() ::
    {name, name()}
    | {ingress, map()}
    %% see `emqtt:option()`
    | {client_opts, map()}.

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HEALTH_CHECK_TIMEOUT, 1000).
-define(NO_PREFIX, <<>>).
-define(IS_NO_PREFIX(P), (P =:= undefined orelse P =:= ?NO_PREFIX)).
-define(MAX_PREFIX_BYTES, 19).
-define(AUTO_RECONNECT_INTERVAL_S, 2).

-type clientid() :: binary().
-type channel_resource_id() :: action_resource_id() | source_resource_id().
-type connector_state() :: #{
    pool_name := connector_resource_id(),
    installed_channels := #{channel_resource_id() => channel_state()},
    clean_start := boolean(),
    available_clientids := [clientid()],
    topic_to_handler_index := ets:table(),
    server := string()
}.
-type channel_state() :: _Todo :: map().

%% ===================================================================
%% When use this bridge as a data source, ?MODULE:on_message_received will be called
%% if the bridge received msgs from the remote broker.

on_message_received(Msg, HookPoints, ResId) ->
    emqx_resource_metrics:received_inc(ResId),
    lists:foreach(
        fun(HookPoint) ->
            emqx_hooks:run(HookPoint, [Msg])
        end,
        HookPoints
    ),
    ok.

%% ===================================================================
resource_type() -> mqtt.

callback_mode() -> async_if_possible.

-spec on_start(connector_resource_id(), map()) -> {ok, connector_state()} | {error, term()}.
on_start(ResourceId, #{server := Server} = Conf) ->
    ?SLOG(info, #{
        msg => "starting_mqtt_connector",
        connector => ResourceId,
        config => emqx_utils:redact(Conf)
    }),
    TopicToHandlerIndex = emqx_topic_index:new(),
    StartConf = Conf#{topic_to_handler_index => TopicToHandlerIndex},
    case start_mqtt_clients(ResourceId, StartConf) of
        {ok, Result1} ->
            {ok, Result1#{
                installed_channels => #{},
                clean_start => maps:get(clean_start, Conf),
                topic_to_handler_index => TopicToHandlerIndex,
                server => Server
            }};
        {error, Reason} ->
            {error, emqx_maybe:define(explain_error(Reason), Reason)}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        clean_start := CleanStart
    } = OldState,
    ChannelId,
    #{config_root := actions} = ChannelConfig
) ->
    %% Publisher channel
    %% make a warning if clean_start is set to false
    case CleanStart of
        false ->
            ?tp(
                mqtt_clean_start_egress_action_warning,
                #{
                    channel_id => ChannelId,
                    resource_id => _InstId
                }
            ),
            ?SLOG(warning, #{
                msg => "mqtt_publisher_clean_start_false",
                reason => "clean_start is set to false when using MQTT publisher action, " ++
                    "which may cause unexpected behavior. " ++
                    "For example, if the client ID is already subscribed to topics, " ++
                    "we might receive messages that are unhanded.",
                channel => ChannelId,
                config => emqx_utils:redact(ChannelConfig)
            });
        true ->
            ok
    end,
    RemoteParams0 = maps:get(parameters, ChannelConfig),
    {LocalParams, RemoteParams} = take(local, RemoteParams0, #{}),
    ChannelState = emqx_bridge_mqtt_egress:config(#{remote => RemoteParams, local => LocalParams}),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState};
on_add_channel(
    _ResourceId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName,
        topic_to_handler_index := TopicToHandlerIndex,
        server := Server
    } = OldState,
    ChannelId,
    #{hookpoints := HookPoints} = ChannelConfig
) ->
    %% Add ingress channel
    RemoteParams0 = maps:get(parameters, ChannelConfig),
    {LocalParams, RemoteParams} = take(local, RemoteParams0, #{}),
    ChannelState0 = #{
        hookpoints => HookPoints,
        server => Server,
        config_root => sources,
        local => LocalParams,
        remote => RemoteParams
    },
    ChannelState1 = mk_ingress_config(ChannelId, ChannelState0, TopicToHandlerIndex),
    ok = emqx_bridge_mqtt_ingress:subscribe_channel(PoolName, ChannelState1),
    NewInstalledChannels = maps:put(ChannelId, ChannelState1, InstalledChannels),
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName,
        topic_to_handler_index := TopicToHandlerIndex
    } = OldState,
    ChannelId
) ->
    case maps:find(ChannelId, InstalledChannels) of
        error ->
            %% maybe the channel failed to be added, just ignore it
            {ok, OldState};
        {ok, ChannelState} ->
            case ChannelState of
                #{config_root := sources} ->
                    ok = emqx_bridge_mqtt_ingress:unsubscribe_channel(
                        PoolName, ChannelState, ChannelId, TopicToHandlerIndex
                    );
                _ ->
                    ok
            end,
            NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
            %% Update state
            NewState = OldState#{installed_channels => NewInstalledChannels},
            {ok, NewState}
    end.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        available_clientids := AvailableClientids,
        installed_channels := Channels
    } = _State
) when is_map_key(ChannelId, Channels) ->
    case AvailableClientids of
        [] ->
            %% We should mark this connector as unhealthy so messages fail fast and an
            %% alarm is raised.
            {?status_disconnected, {unhealthy_target, <<"No clientids assigned to this node">>}};
        [_ | _] ->
            %% The channel should be ok as long as the MQTT client is ok
            ?status_connected
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

start_mqtt_clients(ResourceId, Conf) ->
    ClientOpts = mk_ecpool_client_opts(ResourceId, Conf),
    start_mqtt_clients(ResourceId, Conf, ClientOpts).

find_my_static_clientids(#{static_clientids := [_ | _] = Entries}) ->
    NodeBin = atom_to_binary(node()),
    MyConfig =
        lists:filtermap(
            fun(#{node := N, ids := Ids}) ->
                case N =:= NodeBin of
                    true ->
                        {true, Ids};
                    false ->
                        false
                end
            end,
            Entries
        ),
    {ok, lists:flatten(MyConfig)};
find_my_static_clientids(#{} = _Conf) ->
    error.

start_mqtt_clients(ResourceId, StartConf, ClientOpts) ->
    PoolName = ResourceId,
    PoolSize = get_pool_size(StartConf),
    AvailableClientids = get_available_clientids(StartConf, ClientOpts),
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {available_clientids, AvailableClientids},
        {client_opts, ClientOpts},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL_S}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, ?MODULE, Options) of
        ok ->
            {ok, #{pool_name => PoolName, available_clientids => AvailableClientids}};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

get_pool_size(#{static_clientids := [_ | _]} = Conf) ->
    {ok, Ids} = find_my_static_clientids(Conf),
    length(Ids);
get_pool_size(#{pool_size := PoolSize}) ->
    PoolSize.

get_available_clientids(#{} = Conf, ClientOpts) ->
    case find_my_static_clientids(Conf) of
        {ok, Ids} ->
            Ids;
        error ->
            #{pool_size := PoolSize} = Conf,
            #{clientid := ClientIdPrefix} = ClientOpts,
            lists:map(
                fun(WorkerId) ->
                    mk_clientid(WorkerId, ClientIdPrefix)
                end,
                lists:seq(1, PoolSize)
            )
    end.

on_stop(ResourceId, State) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        resource_id => ResourceId
    }),
    %% on_stop can be called with State = undefined
    StateMap =
        case State of
            Map when is_map(State) ->
                Map;
            _ ->
                #{}
        end,
    case maps:get(topic_to_handler_index, StateMap, undefined) of
        undefined ->
            ok;
        TopicToHandlerIndex ->
            ets:delete(TopicToHandlerIndex)
    end,
    Allocated = emqx_resource:get_allocated_resources(ResourceId),
    ok = stop_helper(Allocated),
    ?tp(mqtt_connector_stopped, #{instance_id => ResourceId}),
    ok.

stop_helper(#{pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName).

on_query(
    ResourceId,
    {ChannelId, Msg},
    #{pool_name := PoolName} = State
) ->
    ?TRACE(
        "QUERY",
        "send_msg_to_remote_node",
        #{
            message => Msg,
            connector => ResourceId,
            channel_id => ChannelId
        }
    ),
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    case is_expected_to_have_workers(State) of
        true ->
            handle_send_result(with_egress_client(ChannelId, PoolName, send, [Msg, ChannelConfig]));
        false ->
            {error, {unrecoverable_error, <<"This node has no assigned static clientid.">>}}
    end;
on_query(ResourceId, {_ChannelId, Msg}, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        resource_id => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

on_query_async(
    ResourceId,
    {ChannelId, Msg},
    CallbackIn,
    #{pool_name := PoolName} = State
) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    case is_expected_to_have_workers(State) of
        true ->
            Result = with_egress_client(ChannelId, PoolName, send_async, [
                Msg, Callback, ChannelConfig
            ]),
            case Result of
                ok ->
                    ok;
                {ok, Pid} when is_pid(Pid) ->
                    {ok, Pid};
                {error, Reason} ->
                    {error, classify_error(Reason)}
            end;
        false ->
            {error, {unrecoverable_error, <<"This node has no assigned static clientid.">>}}
    end;
on_query_async(ResourceId, {_ChannelId, Msg}, _Callback, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        resource_id => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

with_egress_client(ActionID, ResourceId, Fun, Args) ->
    TraceRenderedCTX = emqx_trace:make_rendered_action_template_trace_context(ActionID),
    ecpool:pick_and_do(
        ResourceId, {emqx_bridge_mqtt_egress, Fun, [TraceRenderedCTX | Args]}, no_handover
    ).

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

handle_send_result(ok) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_SUCCESS}}) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}) ->
    ok;
handle_send_result({ok, Reply}) ->
    {error, classify_reply(Reply)};
handle_send_result({error, Reason}) ->
    {error, classify_error(Reason)}.

classify_reply(Reply = #{reason_code := ?RC_PACKET_IDENTIFIER_IN_USE}) ->
    %% If `emqtt' client restarted, it may re-use packet ids that the remote broker still
    %% has memory of.  We should retry.
    {recoverable_error, Reply};
classify_reply(Reply = #{reason_code := _}) ->
    {unrecoverable_error, Reply}.

classify_error(disconnected = Reason) ->
    {recoverable_error, Reason};
classify_error(ecpool_empty) ->
    {recoverable_error, disconnected};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error(closed = Reason) ->
    {recoverable_error, Reason};
classify_error(tcp_closed = Reason) ->
    {recoverable_error, Reason};
classify_error(einval = Reason) ->
    {recoverable_error, Reason};
classify_error({unrecoverable_error, _Reason} = Error) ->
    Error;
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

on_get_status(_ResourceId, State) ->
    Pools = maps:to_list(maps:with([pool_name], State)),
    Workers = [{Pool, Worker} || {Pool, PN} <- Pools, {_Name, Worker} <- ecpool:workers(PN)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses, State)
    catch
        exit:timeout ->
            ?status_connecting
    end.

get_status({_Pool, Worker}) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} ->
            emqx_bridge_mqtt_ingress:status(Client);
        {error, _} ->
            ?status_disconnected
    end.

combine_status(Statuses, ConnState) ->
    %% NOTE
    %% Natural order of statuses: [connected, connecting, disconnected]
    %% * `disconnected` wins over any other status
    %% * `connecting` wins over `connected`
    #{available_clientids := AvailableClientids} = ConnState,
    ExpectedNoClientids =
        case AvailableClientids of
            _ when length(AvailableClientids) == 0 ->
                true;
            _ ->
                false
        end,
    ToStatus = fun
        ({S, _Reason}) -> S;
        (S) when is_atom(S) -> S
    end,
    CompareFn = fun(S1A, S2A) ->
        S1 = ToStatus(S1A),
        S2 = ToStatus(S2A),
        S1 > S2
    end,
    case lists:usort(CompareFn, Statuses) of
        [{Status, Reason} | _] ->
            case explain_error(Reason) of
                undefined -> Status;
                Msg -> {Status, Msg}
            end;
        [Status | _] ->
            Status;
        [] when ExpectedNoClientids ->
            {?status_disconnected,
                {unhealthy_target, <<"Connector has no assigned static clientids">>}};
        [] ->
            ?status_disconnected
    end.

mk_ingress_config(
    ChannelId,
    IngressChannelConfig,
    TopicToHandlerIndex
) ->
    HookPoints = maps:get(hookpoints, IngressChannelConfig, []),
    NewConf = IngressChannelConfig#{
        on_message_received => {?MODULE, on_message_received, [HookPoints, ChannelId]},
        ingress_list => [IngressChannelConfig]
    },
    emqx_bridge_mqtt_ingress:config(NewConf, ChannelId, TopicToHandlerIndex).

mk_ecpool_client_opts(
    ResourceId,
    Config = #{
        server := Server,
        keepalive := KeepAlive,
        ssl := #{enable := EnableSsl} = Ssl
    }
) ->
    HostPort = emqx_bridge_mqtt_connector_schema:parse_server(Server),
    Options = maps:with(
        [
            proto_ver,
            username,
            password,
            clean_start,
            retry_interval,
            max_inflight,
            % Opening a connection in bridge mode will form a non-standard mqtt connection message.
            % A load balancing server (such as haproxy) is often set up before the emqx broker server.
            % When the load balancing server enables mqtt connection packet inspection,
            % non-standard mqtt connection packets might be filtered out by LB.
            bridge_mode,
            topic_to_handler_index
        ],
        Config
    ),
    Name = parse_id_to_name(ResourceId),
    mk_client_opt_password(Options#{
        hosts => [HostPort],
        clientid => clientid(Name, Config),
        connect_timeout => 30,
        keepalive => ms_to_s(KeepAlive),
        force_ping => true,
        ssl => EnableSsl,
        ssl_opts => maps:to_list(maps:remove(enable, Ssl))
    }).

parse_id_to_name(Id) ->
    {_Type, Name} = emqx_connector_resource:parse_connector_id(Id, #{atom_name => false}),
    Name.

mk_client_opt_password(Options = #{password := Secret}) ->
    %% TODO: Teach `emqtt` to accept 0-arity closures as passwords.
    Options#{password := emqx_secret:unwrap(Secret)};
mk_client_opt_password(Options) ->
    Options.

ms_to_s(Ms) ->
    erlang:ceil(Ms / 1000).

clientid(Name, _Conf = #{clientid_prefix := Prefix}) when
    is_binary(Prefix) andalso Prefix =/= <<>>
->
    {Prefix, emqx_bridge_mqtt_lib:clientid_base(Name)};
clientid(Name, _Conf) ->
    {?NO_PREFIX, emqx_bridge_mqtt_lib:clientid_base(Name)}.

%% @doc Start an ingress bridge worker.
-spec connect([option() | {ecpool_worker_id, pos_integer()}]) ->
    {ok, pid()} | {error, _Reason}.
connect(Options) ->
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    ?SLOG(debug, #{
        msg => "ingress_client_starting",
        options => emqx_utils:redact(Options)
    }),
    Name = proplists:get_value(name, Options),
    ClientOpts = proplists:get_value(client_opts, Options),
    AvailableClientids = proplists:get_value(available_clientids, Options),
    case emqtt:start_link(mk_emqtt_client_opts(Name, WorkerId, AvailableClientids, ClientOpts)) of
        {ok, Pid} ->
            connect(Pid, Name);
        {error, Reason} = Error ->
            IsDryRun = emqx_resource:is_dry_run(Name),
            ?SLOG(?LOG_LEVEL(IsDryRun), #{
                msg => "client_start_failed",
                resource_id => Name,
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_emqtt_client_opts(
    Name,
    WorkerId,
    AvailableClientids,
    ClientOpts = #{
        topic_to_handler_index := TopicToHandlerIndex
    }
) ->
    %% WorkerId :: 1..inf
    ClientOpts#{
        clientid := lists:nth(WorkerId, AvailableClientids),
        msg_handler => mk_client_event_handler(Name, TopicToHandlerIndex)
    }.

mk_clientid(WorkerId, {Prefix, ClientId}) when ?IS_NO_PREFIX(Prefix) ->
    %% When there is no prefix, try to keep the client ID length within 23 bytes
    emqx_bridge_mqtt_lib:bytes23(ClientId, WorkerId);
mk_clientid(WorkerId, {Prefix, ClientId}) when
    size(Prefix) =< ?MAX_PREFIX_BYTES
->
    %% Try to respect client ID prefix when it's no more than 19 bytes,
    %% meaning there are at least 4 bytes as hash space.
    emqx_bridge_mqtt_lib:bytes23_with_prefix(Prefix, ClientId, WorkerId);
mk_clientid(WorkerId, {Prefix, ClientId}) ->
    %% There is no other option but to use a long client ID
    iolist_to_binary([Prefix, ClientId, $:, integer_to_binary(WorkerId)]).

mk_client_event_handler(Name, TopicToHandlerIndex) ->
    #{
        publish => {fun emqx_bridge_mqtt_ingress:handle_publish/3, [Name, TopicToHandlerIndex]},
        disconnected => {fun ?MODULE:handle_disconnect/1, []}
    }.

-spec connect(pid(), name()) ->
    {ok, pid()} | {error, _Reason}.
connect(Pid, Name) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            {ok, Pid};
        {error, Reason} = Error ->
            IsDryRun = emqx_resource:is_dry_run(Name),
            log_connect_error_reason(?LOG_LEVEL(IsDryRun), Reason, Name),
            _ = catch emqtt:stop(Pid),
            Error
    end.

log_connect_error_reason(Level, {tcp_closed, _} = Reason, Name) ->
    ?tp(emqx_bridge_mqtt_connector_tcp_closed, #{}),
    ?SLOG(Level, #{
        msg => "ingress_client_connect_failed",
        reason => Reason,
        name => Name,
        explain => explain_error(Reason)
    });
log_connect_error_reason(Level, econnrefused = Reason, Name) ->
    ?tp(emqx_bridge_mqtt_connector_econnrefused_error, #{}),
    ?SLOG(Level, #{
        msg => "ingress_client_connect_failed",
        reason => Reason,
        name => Name,
        explain => explain_error(Reason)
    });
log_connect_error_reason(Level, Reason, Name) ->
    ?SLOG(Level, #{
        msg => "ingress_client_connect_failed",
        reason => Reason,
        name => Name
    }).

explain_error(econnrefused) ->
    <<
        "Connection refused. "
        "This error indicates that your connection attempt to the MQTT server was rejected. "
        "In simpler terms, the server you tried to connect to refused your request. "
        "There can be multiple reasons for this. "
        "For example, the MQTT server you're trying to connect to might be down or not "
        "running at all or you might have provided the wrong address "
        "or port number for the server."
    >>;
explain_error({tcp_closed, _}) ->
    <<
        "Your MQTT connection attempt was unsuccessful. "
        "It might be at its maximum capacity for handling new connections. "
        "To diagnose the issue further, you can check the server logs for "
        "any specific messages related to the unavailability or connection limits."
    >>;
explain_error(_Reason) ->
    undefined.

handle_disconnect(_Reason) ->
    ok.

take(Key, Map0, Default) ->
    case maps:take(Key, Map0) of
        {Value, Map} ->
            {Value, Map};
        error ->
            {Default, Map0}
    end.

is_expected_to_have_workers(#{available_clientids := []} = _ConnState) ->
    false;
is_expected_to_have_workers(_ConnState) ->
    true.
