%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_impl_producer).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    query_mode/1,
    query_opts/1,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([
    on_kafka_ack/3,
    handle_telemetry_event/4
]).

-ifdef(TEST).
-export([replayq_dir/2]).
-endif.

-include_lib("emqx/include/logger.hrl").

%% Allocatable resources
-define(kafka_telemetry_id, kafka_telemetry_id).
-define(kafka_client_id, kafka_client_id).
-define(kafka_producers, kafka_producers).
-define(PROBE_TOPIC_NAME, <<"emqx-connector-connectivity-probe">>).

resource_type() -> kafka_producer.

query_mode(#{parameters := #{query_mode := sync}}) ->
    simple_sync_internal_buffer;
query_mode(_) ->
    simple_async_internal_buffer.

query_opts(#{parameters := #{query_mode := sync, sync_query_timeout := Timeout}}) ->
    #{timeout => Timeout};
query_opts(_) ->
    #{}.

callback_mode() -> async_if_possible.

check_config(Key, Config) when is_map_key(Key, Config) ->
    tr_config(Key, maps:get(Key, Config));
check_config(Key, _Config) ->
    throw(#{
        reason => missing_required_config,
        missing_config => Key
    }).

tr_config(bootstrap_hosts, Hosts) ->
    emqx_bridge_kafka_impl:hosts(Hosts);
tr_config(authentication, Auth) ->
    emqx_bridge_kafka_impl:sasl(Auth);
tr_config(ssl, Ssl) ->
    ssl(Ssl);
tr_config(socket_opts, Opts) ->
    emqx_bridge_kafka_impl:socket_opts(Opts);
tr_config(_Key, Value) ->
    Value.

%% @doc Config schema is defined in emqx_bridge_kafka.
on_start(InstId, Config) ->
    ?SLOG(debug, #{
        msg => "kafka_client_starting",
        instance_id => InstId,
        config => emqx_utils:redact(Config)
    }),
    C = fun(Key) -> check_config(Key, Config) end,
    Hosts = C(bootstrap_hosts),
    ClientConfig = #{
        min_metadata_refresh_interval => C(min_metadata_refresh_interval),
        connect_timeout => C(connect_timeout),
        request_timeout => C(metadata_request_timeout),
        extra_sock_opts => C(socket_opts),
        sasl => C(authentication),
        ssl => C(ssl)
    },
    ClientId = InstId,
    emqx_resource:allocate_resource(InstId, ?kafka_client_id, ClientId),
    ok = ensure_client(ClientId, Hosts, ClientConfig),
    %% Note: we must return `{error, _}' here if the client cannot connect so that the
    %% connector will immediately enter the `?status_disconnected' state, and then avoid
    %% giving the impression that channels/actions may be added immediately and start
    %% buffering, which won't happen if it's `?status_connecting'.  That would lead to
    %% data loss, since Kafka Producer uses wolff's internal buffering, which is started
    %% only when its producers start.
    HealthCheckTopic = maps:get(health_check_topic, Config, ?PROBE_TOPIC_NAME),
    case check_client_connectivity(InstId, ClientId, HealthCheckTopic) of
        ok ->
            ConnectorState = #{
                client_id => ClientId,
                health_check_topic => HealthCheckTopic,
                installed_bridge_v2s => #{}
            },
            {ok, ConnectorState};
        {error, Reason} ->
            {error, Reason}
    end.

on_add_channel(
    InstId,
    #{
        client_id := ClientId,
        installed_bridge_v2s := InstalledBridgeV2s
    } = OldState,
    BridgeV2Id,
    BridgeV2Config
) ->
    %% The following will throw an exception if the bridge producers fails to start
    {ok, BridgeV2State} = create_producers_for_bridge_v2(
        InstId, BridgeV2Id, ClientId, BridgeV2Config
    ),
    NewInstalledBridgeV2s = maps:put(BridgeV2Id, BridgeV2State, InstalledBridgeV2s),
    %% Update state
    NewState = OldState#{installed_bridge_v2s => NewInstalledBridgeV2s},
    {ok, NewState}.

create_producers_for_bridge_v2(
    ConnResId,
    ActionResId,
    ClientId,
    #{
        bridge_type := BridgeType,
        parameters := KafkaConfig
    }
) ->
    #{
        message := MessageTemplate,
        topic := KafkaTopic0,
        sync_query_timeout := SyncQueryTimeout
    } = KafkaConfig,
    TopicTemplate = {TopicType, TopicOrTemplate} = maybe_preproc_topic(KafkaTopic0),
    MKafkaTopic =
        case TopicType of
            fixed -> TopicOrTemplate;
            dynamic -> dynamic
        end,
    KafkaHeadersTokens = preproc_kafka_headers(maps:get(kafka_headers, KafkaConfig, undefined)),
    KafkaExtHeadersTokens = preproc_ext_headers(maps:get(kafka_ext_headers, KafkaConfig, [])),
    KafkaHeadersValEncodeMode = maps:get(kafka_header_value_encode_mode, KafkaConfig, none),
    MaxPartitions = maps:get(partitions_limit, KafkaConfig, all_partitions),
    #{name := BridgeName} = emqx_bridge_v2:parse_id(ActionResId),
    IsDryRun = emqx_resource:is_dry_run(ActionResId),
    ok = assert_topic_and_leader_connections(ActionResId, ClientId, MKafkaTopic, MaxPartitions),
    WolffProducerConfig =
        #{replayq_dir := ReplayqDir} = producers_config(
            BridgeType, BridgeName, KafkaConfig, IsDryRun, ActionResId
        ),
    maybe_migrate_old_replayq_dir(ReplayqDir, ActionResId, TopicType, MKafkaTopic),
    case wolff:ensure_supervised_dynamic_producers(ClientId, WolffProducerConfig) of
        {ok, Producers} ->
            case add_fixed_topic(TopicType, MKafkaTopic, Producers) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "kafka_producer_failed_to_add_fixed_topic",
                        instance_id => ConnResId,
                        kafka_client_id => ClientId,
                        kafka_topic => MKafkaTopic,
                        reason => Reason
                    }),
                    wolff:stop_and_delete_supervised_producers(Producers),
                    throw(
                        "Failed to start producers. Please check the logs for errors and check"
                        " the configuration parameters."
                    )
            end,
            ok = emqx_resource:allocate_resource(
                ConnResId, {?kafka_producers, ActionResId}, Producers
            ),
            ok = emqx_resource:allocate_resource(
                ConnResId, {?kafka_telemetry_id, ActionResId}, ActionResId
            ),
            _ = maybe_install_wolff_telemetry_handlers(ActionResId),
            {ok, #{
                message_template => compile_message_template(MessageTemplate),
                kafka_client_id => ClientId,
                topic_template => TopicTemplate,
                topic => MKafkaTopic,
                producers => Producers,
                resource_id => ActionResId,
                connector_resource_id => ConnResId,
                sync_query_timeout => SyncQueryTimeout,
                kafka_config => KafkaConfig,
                headers_tokens => KafkaHeadersTokens,
                ext_headers_tokens => KafkaExtHeadersTokens,
                headers_val_encode_mode => KafkaHeadersValEncodeMode,
                partitions_limit => MaxPartitions
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer",
                instance_id => ConnResId,
                kafka_client_id => ClientId,
                kafka_topic => MKafkaTopic,
                reason => Reason2
            }),
            throw(
                "Failed to start Kafka client. Please check the logs for errors and check"
                " the connection parameters."
            )
    end.

on_stop(InstanceId, _State) ->
    AllocatedResources = emqx_resource:get_allocated_resources(InstanceId),
    ClientId = maps:get(?kafka_client_id, AllocatedResources, undefined),
    case ClientId of
        undefined ->
            ok;
        ClientId ->
            deallocate_client(ClientId)
    end,
    maps:foreach(
        fun
            ({?kafka_producers, _BridgeV2Id}, Producers) ->
                deallocate_producers(ClientId, Producers);
            ({?kafka_telemetry_id, _BridgeV2Id}, TelemetryId) ->
                deallocate_telemetry_handlers(TelemetryId);
            (_, _) ->
                ok
        end,
        AllocatedResources
    ),
    ?tp(kafka_producer_stopped, #{instance_id => InstanceId}),
    ok.

ensure_client(ClientId, Hosts, ClientConfig) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, _Pid} ->
            ok;
        {error, #{reason := no_such_client}} ->
            case wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig) of
                {ok, _} ->
                    ?SLOG(info, #{
                        msg => "kafka_client_started",
                        client_id => ClientId,
                        kafka_hosts => Hosts
                    });
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => failed_to_start_kafka_client,
                        client_id => ClientId,
                        kafka_hosts => Hosts,
                        reason => Reason
                    }),
                    throw(failed_to_start_kafka_client)
            end;
        {error, Reason} ->
            deallocate_client(ClientId),
            throw({failed_to_find_created_client, Reason})
    end.

deallocate_client(ClientId) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
        #{
            msg => "failed_to_delete_kafka_client",
            client_id => ClientId
        }
    ),
    ok.

deallocate_producers(ClientId, Producers) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_producers(Producers) end,
        #{
            msg => "failed_to_delete_kafka_producer",
            client_id => ClientId
        }
    ).

deallocate_telemetry_handlers(TelemetryId) ->
    _ = with_log_at_error(
        fun() -> uninstall_telemetry_handlers(TelemetryId) end,
        #{
            msg => "failed_to_uninstall_telemetry_handlers",
            resource_id => TelemetryId
        }
    ).

remove_producers_for_bridge_v2(
    InstId, BridgeV2Id
) ->
    AllocatedResources = emqx_resource:get_allocated_resources(InstId),
    ClientId = maps:get(?kafka_client_id, AllocatedResources, no_client_id),
    maps:foreach(
        fun
            ({?kafka_producers, BridgeV2IdCheck}, Producers) when
                BridgeV2IdCheck =:= BridgeV2Id
            ->
                deallocate_producers(ClientId, Producers);
            ({?kafka_telemetry_id, BridgeV2IdCheck}, TelemetryId) when
                BridgeV2IdCheck =:= BridgeV2Id
            ->
                deallocate_telemetry_handlers(TelemetryId);
            (_, _) ->
                ok
        end,
        AllocatedResources
    ),
    ok.

on_remove_channel(
    InstId,
    #{
        client_id := _ClientId,
        installed_bridge_v2s := InstalledBridgeV2s
    } = OldState,
    BridgeV2Id
) ->
    ok = remove_producers_for_bridge_v2(InstId, BridgeV2Id),
    NewInstalledBridgeV2s = maps:remove(BridgeV2Id, InstalledBridgeV2s),
    %% Update state
    NewState = OldState#{installed_bridge_v2s => NewInstalledBridgeV2s},
    {ok, NewState}.

on_query(
    InstId,
    {MessageTag, Message},
    #{installed_bridge_v2s := BridgeV2Configs} = _ConnectorState
) ->
    #{
        message_template := MessageTemplate,
        topic_template := TopicTemplate,
        producers := Producers,
        sync_query_timeout := SyncTimeout,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    } = maps:get(MessageTag, BridgeV2Configs),
    KafkaHeaders = #{
        headers_tokens => KafkaHeadersTokens,
        ext_headers_tokens => KafkaExtHeadersTokens,
        headers_val_encode_mode => KafkaHeadersValEncodeMode
    },
    try
        KafkaTopic = render_topic(TopicTemplate, Message),
        KafkaMessage = render_message(MessageTemplate, KafkaHeaders, Message),
        ?tp(
            emqx_bridge_kafka_impl_producer_sync_query,
            #{headers_config => KafkaHeaders, instance_id => InstId}
        ),
        emqx_trace:rendered_action_template(MessageTag, #{
            message => KafkaMessage
        }),
        do_send_msg(sync, KafkaTopic, KafkaMessage, Producers, SyncTimeout)
    catch
        throw:bad_topic ->
            ?tp("kafka_producer_failed_to_render_topic", #{}),
            {error, {unrecoverable_error, failed_to_render_topic}};
        throw:#{cause := unknown_topic_or_partition, topic := Topic} ->
            ?tp("kafka_producer_resolved_to_unknown_topic", #{}),
            {error, {unrecoverable_error, {resolved_to_unknown_topic, Topic}}};
        throw:#{cause := invalid_partition_count, count := Count} ->
            ?tp("kafka_producer_invalid_partition_count", #{
                action_id => MessageTag,
                query_mode => sync
            }),
            {error, {unrecoverable_error, {invalid_partition_count, Count}}};
        throw:{bad_kafka_header, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_sync_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}};
        throw:{bad_kafka_headers, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_sync_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}}
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

%% @doc The callback API for rule-engine (or bridge without rules)
%% The input argument `Message' is an enriched format (as a map())
%% of the original #message{} record.
%% The enrichment is done by rule-engine or by the data bridge framework.
%% E.g. the output of rule-engine process chain
%% or the direct mapping from an MQTT message.
on_query_async(
    InstId,
    {MessageTag, Message},
    AsyncReplyFn,
    #{installed_bridge_v2s := BridgeV2Configs} = _ConnectorState
) ->
    #{
        message_template := Template,
        topic_template := TopicTemplate,
        producers := Producers,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    } = maps:get(MessageTag, BridgeV2Configs),
    KafkaHeaders = #{
        headers_tokens => KafkaHeadersTokens,
        ext_headers_tokens => KafkaExtHeadersTokens,
        headers_val_encode_mode => KafkaHeadersValEncodeMode
    },
    try
        KafkaTopic = render_topic(TopicTemplate, Message),
        KafkaMessage = render_message(Template, KafkaHeaders, Message),
        ?tp(
            emqx_bridge_kafka_impl_producer_async_query,
            #{headers_config => KafkaHeaders, instance_id => InstId}
        ),
        emqx_trace:rendered_action_template(MessageTag, #{
            message => KafkaMessage
        }),
        do_send_msg(async, KafkaTopic, KafkaMessage, Producers, AsyncReplyFn)
    catch
        throw:bad_topic ->
            ?tp("kafka_producer_failed_to_render_topic", #{}),
            {error, {unrecoverable_error, failed_to_render_topic}};
        throw:#{cause := unknown_topic_or_partition, topic := Topic} ->
            ?tp("kafka_producer_resolved_to_unknown_topic", #{}),
            {error, {unrecoverable_error, {resolved_to_unknown_topic, Topic}}};
        throw:#{cause := invalid_partition_count, count := Count} ->
            ?tp("kafka_producer_invalid_partition_count", #{
                action_id => MessageTag,
                query_mode => async
            }),
            {error, {unrecoverable_error, {invalid_partition_count, Count}}};
        throw:{bad_kafka_header, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_async_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}};
        throw:{bad_kafka_headers, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_async_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}}
    end.

compile_message_template(T) ->
    KeyTemplate = maps:get(key, T, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, T, <<"${.}">>),
    TimestampTemplate = maps:get(timestamp, T, <<"${.timestamp}">>),
    #{
        key => preproc_tmpl(KeyTemplate),
        value => preproc_tmpl(ValueTemplate),
        timestamp => preproc_tmpl(TimestampTemplate)
    }.

maybe_preproc_topic(Topic) ->
    Template = emqx_template:parse(Topic),
    case emqx_template:placeholders(Template) of
        [] ->
            {fixed, bin(Topic)};
        [_ | _] ->
            {dynamic, Template}
    end.

preproc_tmpl(Tmpl) ->
    emqx_placeholder:preproc_tmpl(Tmpl).

render_topic({fixed, KafkaTopic}, _Message) ->
    KafkaTopic;
render_topic({dynamic, Template}, Message) ->
    try
        iolist_to_binary(emqx_template:render_strict(Template, {emqx_jsonish, Message}))
    catch
        error:_Errors ->
            throw(bad_topic)
    end.

render_message(
    #{key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate},
    #{
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    },
    Message
) ->
    ExtHeaders = proc_ext_headers(KafkaExtHeadersTokens, Message),
    KafkaHeaders =
        case KafkaHeadersTokens of
            undefined -> ExtHeaders;
            HeadersTks -> merge_kafka_headers(HeadersTks, ExtHeaders, Message)
        end,
    Headers = formalize_kafka_headers(KafkaHeaders, KafkaHeadersValEncodeMode),
    #{
        key => render(KeyTemplate, Message),
        value => render(ValueTemplate, Message),
        headers => Headers,
        ts => render_timestamp(TimestampTemplate, Message)
    }.

render(Template, Message) ->
    Opts = #{
        var_trans => fun
            (undefined) -> <<"">>;
            (X) -> emqx_utils_conv:bin(X)
        end,
        return => full_binary
    },
    emqx_placeholder:proc_tmpl(Template, Message, Opts).

render_timestamp(Template, Message) ->
    try
        binary_to_integer(render(Template, Message))
    catch
        _:_ ->
            erlang:system_time(millisecond)
    end.

do_send_msg(sync, KafkaTopic, KafkaMessage, Producers, SyncTimeout) ->
    try
        {_Partition, _Offset} = wolff:send_sync2(
            Producers, KafkaTopic, [KafkaMessage], SyncTimeout
        ),
        ok
    catch
        error:{producer_down, _} = Reason ->
            {error, Reason};
        error:timeout ->
            {error, timeout}
    end;
do_send_msg(async, KafkaTopic, KafkaMessage, Producers, AsyncReplyFn) ->
    %% * Must be a batch because wolff send and cast are batch APIs
    %% * Must be a single element batch because wolff books calls, but not batch sizes
    %%   for counters and gauges.
    Batch = [KafkaMessage],
    {_Partition, Pid} = wolff:send2(
        Producers, KafkaTopic, Batch, {fun ?MODULE:on_kafka_ack/3, [AsyncReplyFn]}
    ),
    %% This Pid is returned, but not monitored by caller
    %% See emqx_resource_buffer_worker:simple_async_internal_buffer
    {ok, Pid}.

%% Wolff producer never gives up retrying
%% so there can only be 'ok' results.
on_kafka_ack(_Partition, Offset, ReplyFnAndArgs) when is_integer(Offset) ->
    %% `emqx_rule_runtime:inc_action_metrics/2' is embedded inside reply function
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, ok);
on_kafka_ack(_Partition, buffer_overflow_discarded, ReplyFnAndArgs) ->
    %% wolff should bump the dropped_queue_full counter in handle_telemetry_event/4
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, {error, buffer_overflow});
on_kafka_ack(_Partition, message_too_large, ReplyFnAndArgs) ->
    %% wolff should bump the message 'dropped' counter with handle_telemetry_event/4.
    %% however 'dropped' is not mapped to EMQX metrics name
    %% so we reply error here
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, {error, message_too_large}).

%% Note: since wolff client has its own replayq that is not managed by
%% `emqx_resource_buffer_worker', we must avoid returning `disconnected' here.  Otherwise,
%% `emqx_resource_manager' will kill the wolff producers and messages might be lost.
on_get_status(
    ConnResId,
    #{client_id := ClientId} = State
) ->
    %% Note: we must avoid returning `?status_disconnected' here if the connector ever was
    %% connected.  If the connector ever connected, wolff producers might have been
    %% successfully started, and returning `?status_disconnected' will make resource
    %% manager try to restart the producers / connector, thus potentially dropping data
    %% held in wolff producer's replayq.
    HealthCheckTopic = maps:get(health_check_topic, State),
    case check_client_connectivity(ConnResId, ClientId, HealthCheckTopic) of
        ok ->
            ?status_connected;
        {error, #{reason := cannot_find_kafka_client}} ->
            %% Kafka client is starting
            ?status_connecting;
        {error, Reason} ->
            {?status_connecting, Reason}
    end.

on_get_channel_status(
    _ConnResId,
    ActionResId,
    #{
        client_id := ClientId,
        installed_bridge_v2s := Channels
    } = _ConnState
) ->
    %% Note: we must avoid returning `?status_disconnected' here. Returning
    %% `?status_disconnected' will make resource manager try to restart the producers /
    %% connector, thus potentially dropping data held in wolff producer's replayq.  The
    %% only exception is if the topic does not exist ("unhealthy target").
    #{
        topic := MKafkaTopic,
        partitions_limit := MaxPartitions
    } = maps:get(ActionResId, Channels),
    try
        ok = assert_topic_and_leader_connections(ActionResId, ClientId, MKafkaTopic, MaxPartitions),
        ?tp("kafka_producer_action_connected", #{}),
        ?status_connected
    catch
        throw:{unhealthy_target, Msg} ->
            %% Once a producer is started, we don't want to delete it.  Apparently,
            %% metadata queries may report false negatives about topic existence.
            {?status_connecting, Msg};
        K:E ->
            {?status_connecting, {K, E}}
    end.

assert_topic_and_leader_connections(ActionResId, ClientId, MKafkaTopic, MaxPartitions) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            maybe
                true ?= is_binary(MKafkaTopic),
                ok = check_topic_status(ClientId, Pid, MKafkaTopic),
                ok = check_if_healthy_leaders(
                    ActionResId, ClientId, Pid, MKafkaTopic, MaxPartitions
                )
            else
                false -> ok
            end;
        {error, #{reason := no_such_client}} ->
            throw(#{
                reason => cannot_find_kafka_client,
                kafka_client => ClientId,
                kafka_topic => MKafkaTopic
            });
        {error, #{reason := client_supervisor_not_initialized}} ->
            throw(#{
                reason => restarting,
                kafka_client => ClientId,
                kafka_topic => MKafkaTopic
            })
    end.

-spec check_client_connectivity(binary(), wolff:client_id(), binary()) -> ok | {error, term()}.
check_client_connectivity(ConnResId, ClientId, HealthCheckTopic) ->
    MaxPartitions = all_partitions,
    try
        assert_topic_and_leader_connections(ConnResId, ClientId, HealthCheckTopic, MaxPartitions)
    catch
        throw:{unhealthy_target, Msg} ->
            {error, Msg};
        throw:#{reason := {connection_down, _} = Reason} ->
            {error, Reason};
        throw:#{reason := Reason} ->
            {error, Reason}
    end.

is_alive(Pid) ->
    is_pid(Pid) andalso erlang:is_process_alive(Pid).

error_summary(Map, [Error]) ->
    Map#{error => Error};
error_summary(Map, [Error | More]) ->
    Map#{first_error => Error, total_errors => length(More) + 1}.

check_if_healthy_leaders(_ActionResId, _ClientId, _ClientPid, ?PROBE_TOPIC_NAME, _MaxPartitions) ->
    %% do not check probe topic leaders
    ok;
check_if_healthy_leaders(ActionResId, ClientId, ClientPid, KafkaTopic, MaxPartitions) when
    is_pid(ClientPid)
->
    case wolff_client:get_leader_connections(ClientPid, ActionResId, KafkaTopic, MaxPartitions) of
        {ok, Leaders} ->
            %% Kafka is considered healthy as long as any of the partition leader is reachable.
            case lists:partition(fun({_Partition, Pid}) -> is_alive(Pid) end, Leaders) of
                {[], Errors} ->
                    throw(
                        error_summary(
                            #{
                                cause => "no_connected_partition_leader",
                                kafka_client => ClientId,
                                kafka_topic => KafkaTopic
                            },
                            Errors
                        )
                    );
                {_, []} ->
                    ok;
                {_, Errors} ->
                    ?SLOG(
                        warning,
                        "not_all_kafka_partitions_connected",
                        error_summary(
                            #{
                                kafka_client => ClientId,
                                kafka_topic => KafkaTopic
                            },
                            Errors
                        )
                    ),
                    ok
            end;
        {error, Reason} ->
            %% If failed to fetch metadata, wolff_client logs a warning level message
            %% which includes the reason for each seed host
            throw(#{
                cause => Reason,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            })
    end.

check_topic_status(ClientId, WolffClientPid, KafkaTopic) ->
    case wolff_client:check_topic_exists_with_client_pid(WolffClientPid, KafkaTopic) of
        ok ->
            ok;
        {error, unknown_topic_or_partition} when KafkaTopic =:= ?PROBE_TOPIC_NAME ->
            %% The probing topic is only used to check if metada request can be sent
            ok;
        {error, unknown_topic_or_partition} ->
            Msg = iolist_to_binary([<<"Unknown topic or partition: ">>, KafkaTopic]),
            ?tp("kafka_producer_action_unknown_topic", #{}),
            throw({unhealthy_target, Msg});
        {error, Reason} ->
            throw(#{
                error => failed_to_check_topic_status,
                kafka_client_id => ClientId,
                reason => Reason,
                kafka_topic => KafkaTopic
            })
    end.

ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    false.

producers_config(BridgeType, BridgeName, Input, IsDryRun, ActionResId) ->
    #{
        max_linger_time := MaxLingerTime,
        max_linger_bytes := MaxLingerBytes,
        max_batch_bytes := MaxBatchBytes,
        compression := Compression,
        partition_strategy := PartitionStrategy,
        required_acks := RequiredAcks,
        partition_count_refresh_interval := PCntRefreshInterval,
        max_inflight := MaxInflight,
        partitions_limit := MaxPartitions,
        buffer := #{
            mode := BufferMode0,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemOLP
        }
    } = Input,
    %% avoid creating dirs for probing producers
    BufferMode =
        case IsDryRun of
            true ->
                memory;
            false ->
                BufferMode0
        end,
    {OffloadMode, ReplayqDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(BridgeType, BridgeName)};
            hybrid -> {true, replayq_dir(BridgeType, BridgeName)}
        end,
    #{
        partitioner => partitioner(PartitionStrategy),
        partition_count_refresh_interval_seconds => PCntRefreshInterval,
        replayq_dir => ReplayqDir,
        replayq_offload_mode => OffloadMode,
        replayq_max_total_bytes => PerPartitionLimit,
        replayq_seg_bytes => SegmentBytes,
        drop_if_highmem => MemOLP,
        required_acks => RequiredAcks,
        max_linger_ms => MaxLingerTime,
        max_linger_bytes => MaxLingerBytes,
        max_batch_bytes => MaxBatchBytes,
        max_send_ahead => MaxInflight - 1,
        compression => Compression,
        group => ActionResId,
        telemetry_meta_data => #{bridge_id => ActionResId},
        max_partitions => MaxPartitions
    }.

%% Wolff API is a batch API.
%% key_dispatch only looks at the first element, so it's named 'first_key_dispatch'
partitioner(random) -> random;
partitioner(key_dispatch) -> first_key_dispatch.

replayq_dir(BridgeType, BridgeName) ->
    DirName = iolist_to_binary([
        maybe_v1_type_name(BridgeType),
        ":",
        BridgeName,
        ":",
        atom_to_list(node())
    ]),
    filename:join([emqx:data_dir(), "kafka", DirName]).

%% new (wolff >= 2.0.0):
%% Dir = filename:join([BaseDir, PathSegment, integer_to_list(Partition)]),
%% old:
%% Dir = filename:join([BaseDir, Topic, integer_to_list(Partition)]),
maybe_migrate_old_replayq_dir(false, _ActionResId, _TopicType, _Topic) ->
    ok;
maybe_migrate_old_replayq_dir(ReplayqDir, ActionResId, fixed = _TopicType, Topic) ->
    OldWolffDir = filename:join([ReplayqDir, Topic]),
    maybe
        true ?= is_old_replayq_dir(OldWolffDir),
        NewWolffDir = filename:join([ReplayqDir, <<ActionResId/binary, $_, Topic/binary>>]),
        ?tp(info, "migrating_old_wolff_dirs", #{
            action_id => ActionResId,
            from => OldWolffDir,
            to => NewWolffDir
        }),
        ok = file:rename(OldWolffDir, NewWolffDir),
        ok
    else
        _ -> ok
    end;
maybe_migrate_old_replayq_dir(_ReplayqDir, _ActionResId, _TopicType, _Topic) ->
    ok.

%% new (wolff >= 2.0.0):
is_old_replayq_dir(OldWolffDir) ->
    maybe
        true ?= filelib:is_dir(OldWolffDir),
        {ok, Files} ?= file:list_dir_all(OldWolffDir),
        %% Each partition number has a sub-directory.
        PartitionDirs = lists:filtermap(
            fun(File) ->
                PartitionDir = filename:join([OldWolffDir, File]),
                IsDir = filelib:is_dir(PartitionDir),
                case IsDir andalso string:to_integer(File) of
                    {_Int, Rest} when Rest =:= ""; Rest =:= <<>> ->
                        {true, PartitionDir};
                    _ ->
                        false
                end
            end,
            Files
        ),
        [_ | _] ?= PartitionDirs,
        true
    else
        _ -> false
    end.

add_fixed_topic(fixed, Topic, Producers) ->
    wolff:add_topic(Producers, Topic);
add_fixed_topic(_TopicType, _TopicTemplate, _Producers) ->
    ok.

%% To avoid losing queued data on disk, we must use the same directory as the old v1
%% bridges, if any.  Among the Kafka-based bridges that exist since v1, only Kafka changed
%% its type name.  Other bridges are either unchanged, or v2-only, and should use their v2
%% type names.
maybe_v1_type_name(Type) when is_atom(Type) -> maybe_v1_type_name(atom_to_binary(Type));
maybe_v1_type_name(<<"kafka_producer">>) -> <<"kafka">>;
maybe_v1_type_name(Type) -> Type.

with_log_at_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

%% we *must* match the bridge id in the event metadata with that in
%% the handler config; otherwise, multiple kafka producer bridges will
%% install multiple handlers to the same wolff events, multiplying the
handle_telemetry_event(
    [wolff, dropped_queue_full],
    #{counter_inc := Val},
    #{bridge_id := ID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_queue_full_inc(ID, Val);
handle_telemetry_event(
    [wolff, queuing],
    #{gauge_set := Val},
    #{bridge_id := ID, partition_id := PartitionID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_set(ID, PartitionID, Val);
handle_telemetry_event(
    [wolff, queuing_bytes],
    #{gauge_set := Val},
    #{bridge_id := ID, partition_id := PartitionID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_bytes_set(ID, PartitionID, Val);
handle_telemetry_event(
    [wolff, retried],
    #{counter_inc := Val},
    #{bridge_id := ID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:retried_inc(ID, Val);
handle_telemetry_event(
    [wolff, inflight],
    #{gauge_set := Val},
    #{bridge_id := ID, partition_id := PartitionID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:inflight_set(ID, PartitionID, Val);
handle_telemetry_event(_EventId, _Metrics, _MetaData, _HandlerConfig) ->
    %% Event that we do not handle
    ok.

%% Note: don't use the instance/manager ID, as that changes everytime
%% the bridge is recreated, and will lead to multiplication of
%% metrics.
-spec telemetry_handler_id(action_resource_id()) -> binary().
telemetry_handler_id(ActionResId) ->
    ActionResId.

uninstall_telemetry_handlers(TelemetryId) ->
    telemetry:detach(TelemetryId).

maybe_install_wolff_telemetry_handlers(TelemetryId) ->
    %% Attach event handlers for Kafka telemetry events. If a handler with the
    %% handler id already exists, the attach_many function does nothing
    telemetry:attach_many(
        %% unique handler id
        telemetry_handler_id(TelemetryId),
        [
            [wolff, dropped_queue_full],
            [wolff, queuing],
            [wolff, queuing_bytes],
            [wolff, retried],
            [wolff, inflight]
        ],
        fun ?MODULE:handle_telemetry_event/4,
        %% we *must* keep track of the same id that is handed down to
        %% wolff producers; otherwise, multiple kafka producer bridges
        %% will install multiple handlers to the same wolff events,
        %% multiplying the metric counts...
        #{bridge_id => TelemetryId}
    ).

preproc_kafka_headers(HeadersTmpl) when HeadersTmpl =:= <<>>; HeadersTmpl =:= undefined ->
    undefined;
preproc_kafka_headers(HeadersTmpl) ->
    %% the value is already validated by schema, so we do not validate it again.
    emqx_placeholder:preproc_tmpl(HeadersTmpl).

preproc_ext_headers(Headers) ->
    [
        {emqx_placeholder:preproc_tmpl(K), preproc_ext_headers_value(V)}
     || #{kafka_ext_header_key := K, kafka_ext_header_value := V} <- Headers
    ].

preproc_ext_headers_value(ValTmpl) ->
    %% the value is already validated by schema, so we do not validate it again.
    emqx_placeholder:preproc_tmpl(ValTmpl).

proc_ext_headers(ExtHeaders, Msg) ->
    lists:filtermap(
        fun({KTks, VTks}) ->
            try
                Key = proc_ext_headers_key(KTks, Msg),
                Val = proc_ext_headers_value(VTks, Msg),
                {true, {Key, Val}}
            catch
                throw:placeholder_not_found -> false
            end
        end,
        ExtHeaders
    ).

proc_ext_headers_key(KeyTks, Msg) ->
    RawList = emqx_placeholder:proc_tmpl(KeyTks, Msg, #{return => rawlist}),
    list_to_binary(
        lists:map(
            fun
                (undefined) -> throw(placeholder_not_found);
                (Key) -> bin(Key)
            end,
            RawList
        )
    ).

proc_ext_headers_value(ValTks, Msg) ->
    case emqx_placeholder:proc_tmpl(ValTks, Msg, #{return => rawlist}) of
        [undefined] -> throw(placeholder_not_found);
        [Value] -> Value
    end.

kvlist_headers([], Acc) ->
    lists:reverse(Acc);
kvlist_headers([#{key := K, value := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([#{<<"key">> := K, <<"value">> := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([{K, V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([KVList | Headers], Acc) when is_list(KVList) ->
    %% for instance, when user sets a json list as headers like '[{"foo":"bar"}, {"foo2":"bar2"}]'.
    kvlist_headers(KVList ++ Headers, Acc);
kvlist_headers([BadHeader | _], _) ->
    throw({bad_kafka_header, BadHeader}).

-define(IS_STR_KEY(K), (is_list(K) orelse is_atom(K) orelse is_binary(K))).
formalize_kafka_headers(Headers, none) ->
    %% Note that we will drop all the non-binary values in the NONE mode
    [{bin(K), V} || {K, V} <- Headers, is_binary(V) andalso ?IS_STR_KEY(K)];
formalize_kafka_headers(Headers, json) ->
    lists:filtermap(
        fun({K, V}) ->
            try
                {true, {bin(K), emqx_utils_json:encode(V)}}
            catch
                _:_ -> false
            end
        end,
        Headers
    ).

merge_kafka_headers(HeadersTks, ExtHeaders, Msg) ->
    case emqx_placeholder:proc_tmpl(HeadersTks, Msg, #{return => rawlist}) of
        % Headers by map object.
        [Map] when is_map(Map) ->
            maps:to_list(Map) ++ ExtHeaders;
        [KVList] when is_list(KVList) ->
            kvlist_headers(KVList, []) ++ ExtHeaders;
        %% the placeholder cannot be found in Msg
        [undefined] ->
            ExtHeaders;
        [MaybeJson] when is_binary(MaybeJson) ->
            case emqx_utils_json:safe_decode(MaybeJson, [return_maps]) of
                {ok, JsonTerm} when is_map(JsonTerm) ->
                    maps:to_list(JsonTerm) ++ ExtHeaders;
                {ok, JsonTerm} when is_list(JsonTerm) ->
                    kvlist_headers(JsonTerm, []) ++ ExtHeaders;
                _ ->
                    throw({bad_kafka_headers, MaybeJson})
            end;
        BadHeaders ->
            throw({bad_kafka_headers, BadHeaders})
    end.

bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> erlang:list_to_binary(L);
bin(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8).
