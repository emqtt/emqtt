%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-include("emqx_conf.hrl").

-behaviour(hocon_schema).

-export([
    namespace/0, roots/0, fields/1, translations/0, translation/1, validations/0, desc/1, tags/0
]).

-export([log_level/0]).

-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([upgrade_raw_conf/1]).
-export([tr_prometheus_collectors/1]).

%% internal exports for `emqx_enterprise_schema' only.
-export([
    log_file_path_converter/2,
    fix_bad_log_path/1,
    ensure_unicode_path/2,
    convert_rotation/2,
    log_handler_common_confs/2
]).

-define(DEFAULT_NODE_NAME, <<"emqx@127.0.0.1">>).

%% Static apps which merge their configs into the merged emqx.conf
%% The list can not be made a dynamic read at run-time as it is used
%% by nodetool to generate app.<time>.config before EMQX is started
-define(MERGED_CONFIGS, [
    emqx_bridge_schema,
    emqx_connector_schema,
    emqx_bridge_v2_schema,
    emqx_retainer_schema,
    emqx_authn_schema,
    emqx_authz_schema,
    emqx_auto_subscribe_schema,
    {emqx_telemetry_schema, ce},
    emqx_modules_schema,
    emqx_plugins_schema,
    emqx_dashboard_schema,
    emqx_gateway_schema,
    emqx_prometheus_schema,
    emqx_rule_engine_schema,
    emqx_exhook_schema,
    emqx_psk_schema,
    emqx_limiter_schema,
    emqx_slow_subs_schema,
    {emqx_otel_schema, ee},
    emqx_mgmt_api_key_schema
]).

%% 1 million default ports counter
-define(DEFAULT_MAX_PORTS, 1024 * 1024).

-define(LOG_THROTTLING_MSGS, [
    authentication_failure,
    authorization_permission_denied,
    cannot_publish_to_topic_due_to_not_authorized,
    cannot_publish_to_topic_due_to_quota_exceeded,
    connection_rejected_due_to_license_limit_reached,
    data_bridge_buffer_overflow,
    dropped_msg_due_to_mqueue_is_full,
    external_broker_crashed,
    failed_to_fetch_crl,
    failed_to_retain_message,
    handle_resource_metrics_failed,
    retain_failed_for_payload_size_exceeded_limit,
    retain_failed_for_rate_exceeded_limit,
    retained_delete_failed_for_rate_exceeded_limit,
    socket_receive_paused_by_rate_limit,
    %% ==== message transformation/validation ====
    transformation_failed,
    transformation_eval_operation_failure,
    transformation_eval_operation_exception,
    payload_encode_failed,
    payload_decode_failed,
    payload_decode_schema_not_found,
    payload_encode_schema_not_found,
    payload_decode_schema_failure,
    payload_encode_schema_failure,
    payload_decode_error,
    validation_failed,
    validation_sql_check_throw,
    validation_sql_check_failure,
    validation_schema_check_schema_not_found,
    validation_schema_check_failure,
    %% ==== message transformation/validation end ====
    unrecoverable_resource_error
]).

-define(DEFAULT_RPC_PORT, 5369).

%% Callback to upgrade config after loaded from config file but before validation.
upgrade_raw_conf(Raw0) ->
    Raw1 = emqx_connector_schema:transform_bridges_v1_to_connectors_and_bridges_v2(Raw0),
    emqx_bridge_v2_schema:actions_convert_from_connectors(Raw1).

namespace() -> emqx.

tags() ->
    [<<"EMQX">>].

roots() ->
    Injections = emqx_conf_schema_inject:schemas(),
    ok = emqx_schema_hooks:inject_from_modules(Injections),
    emqx_schema_high_prio_roots() ++
        [
            {node,
                sc(
                    ?R_REF("node"),
                    #{
                        translate_to => ["emqx"]
                    }
                )},
            {cluster,
                sc(
                    ?R_REF("cluster"),
                    #{translate_to => ["ekka"]}
                )},
            {log,
                sc(
                    ?R_REF("log"),
                    #{
                        translate_to => ["kernel"],
                        importance => ?IMPORTANCE_HIGH,
                        desc => ?DESC(log_root)
                    }
                )},
            {rpc,
                sc(
                    ?R_REF("rpc"),
                    #{
                        translate_to => ["gen_rpc"],
                        importance => ?IMPORTANCE_LOW
                    }
                )}
        ] ++
        emqx_schema:roots(medium) ++
        emqx_schema:roots(low) ++
        lists:flatmap(fun roots/1, common_apps()).

validations() ->
    [
        {check_node_name_and_discovery_strategy, fun validate_cluster_strategy/1},
        {validate_durable_sessions_strategy, fun validate_durable_sessions_strategy/1}
    ] ++
        hocon_schema:validations(emqx_schema) ++
        lists:flatmap(fun hocon_schema:validations/1, common_apps()).

validate_durable_sessions_strategy(Conf) ->
    DSEnabled = hocon_maps:get("durable_sessions.enable", Conf),
    DiscoveryStrategy = hocon_maps:get("cluster.discovery_strategy", Conf),
    DSBackend = hocon_maps:get("durable_storage.messages.backend", Conf),
    case {DSEnabled, DSBackend} of
        {true, builtin_local} when DiscoveryStrategy =/= singleton ->
            {error, <<
                "cluster discovery strategy must be 'singleton' when"
                " durable storage backend is 'builtin_local'"
            >>};
        _ ->
            ok
    end.

common_apps() ->
    Edition = emqx_release:edition(),
    lists:filtermap(
        fun
            ({N, E}) ->
                case E =:= Edition of
                    true -> {true, N};
                    false -> false
                end;
            (N) when is_atom(N) -> {true, N}
        end,
        ?MERGED_CONFIGS
    ).

fields("cluster") ->
    [
        {"name",
            sc(
                atom(),
                #{
                    mapping => "ekka.cluster_name",
                    default => emqxcl,
                    desc => ?DESC(cluster_name),
                    'readOnly' => true
                }
            )},
        {"discovery_strategy",
            sc(
                hoconsc:enum([manual, static, singleton, dns, etcd, k8s]),
                #{
                    default => manual,
                    desc => ?DESC(cluster_discovery_strategy),
                    'readOnly' => true
                }
            )},
        {"autoclean",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "mria.cluster_autoclean",
                    default => <<"24h">>,
                    desc => ?DESC(cluster_autoclean),
                    'readOnly' => true
                }
            )},
        {"autoheal",
            sc(
                boolean(),
                #{
                    mapping => "mria.cluster_autoheal",
                    default => true,
                    desc => ?DESC(cluster_autoheal),
                    'readOnly' => true
                }
            )},
        {"proto_dist",
            sc(
                hoconsc:enum([inet_tcp, inet6_tcp, inet_tls, inet6_tls]),
                #{
                    mapping => "ekka.proto_dist",
                    default => inet_tcp,
                    'readOnly' => true,
                    desc => ?DESC(cluster_proto_dist)
                }
            )},
        {"quic_lb_mode",
            sc(
                hoconsc:union([integer(), string()]),
                #{
                    mapping => "quicer.lb_mode",
                    default => 0,
                    'readOnly' => true,
                    desc => ?DESC(cluster_quic_lb_mode),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"static",
            sc(
                ?R_REF(cluster_static),
                #{}
            )},
        {"dns",
            sc(
                ?R_REF(cluster_dns),
                #{}
            )},
        {"etcd",
            sc(
                ?R_REF(cluster_etcd),
                #{}
            )},
        {"k8s",
            sc(
                ?R_REF(cluster_k8s),
                #{}
            )},
        {"prevent_overlapping_partitions",
            sc(
                boolean(),
                #{
                    mapping => "vm_args.-kernel prevent_overlapping_partitions",
                    desc => ?DESC(prevent_overlapping_partitions),
                    default => false,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ] ++ emqx_schema_hooks:list_injection_point(cluster);
fields(cluster_static) ->
    [
        {"seeds",
            sc(
                node_array(),
                #{
                    default => [],
                    desc => ?DESC(cluster_static_seeds),
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_dns) ->
    [
        {"name",
            sc(
                string(),
                #{
                    default => <<"localhost">>,
                    desc => ?DESC(cluster_dns_name),
                    'readOnly' => true
                }
            )},
        {"record_type",
            sc(
                hoconsc:enum([a, aaaa, srv]),
                #{
                    default => a,
                    desc => ?DESC(cluster_dns_record_type),
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_etcd) ->
    [
        {"server",
            sc(
                emqx_schema:comma_separated_list(),
                #{
                    desc => ?DESC(cluster_etcd_server),
                    'readOnly' => true
                }
            )},
        {"prefix",
            sc(
                string(),
                #{
                    default => <<"emqxcl">>,
                    desc => ?DESC(cluster_etcd_prefix),
                    'readOnly' => true
                }
            )},
        {"node_ttl",
            sc(
                emqx_schema:duration(),
                #{
                    default => <<"1m">>,
                    'readOnly' => true,
                    desc => ?DESC(cluster_etcd_node_ttl)
                }
            )},
        {"ssl_options",
            sc(
                ?R_REF(emqx_schema, "ssl_client_opts"),
                #{
                    desc => ?DESC(cluster_etcd_ssl),
                    aliases => [ssl],
                    'readOnly' => true
                }
            )}
    ];
fields(cluster_k8s) ->
    [
        {"apiserver",
            sc(
                string(),
                #{
                    default => <<"https://kubernetes.default.svc:443">>,
                    desc => ?DESC(cluster_k8s_apiserver),
                    'readOnly' => true
                }
            )},
        {"service_name",
            sc(
                string(),
                #{
                    default => <<"emqx">>,
                    desc => ?DESC(cluster_k8s_service_name),
                    'readOnly' => true
                }
            )},
        {"address_type",
            sc(
                hoconsc:enum([ip, dns, hostname]),
                #{
                    default => ip,
                    desc => ?DESC(cluster_k8s_address_type),
                    'readOnly' => true
                }
            )},
        {"namespace",
            sc(
                string(),
                #{
                    default => <<"default">>,
                    desc => ?DESC(cluster_k8s_namespace),
                    'readOnly' => true
                }
            )},
        {"suffix",
            sc(
                string(),
                #{
                    default => <<"pod.local">>,
                    'readOnly' => true,
                    desc => ?DESC(cluster_k8s_suffix)
                }
            )}
    ];
fields("node") ->
    [
        {"name",
            sc(
                string(),
                #{
                    default => ?DEFAULT_NODE_NAME,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIGH,
                    desc => ?DESC(node_name)
                }
            )},
        {"cookie",
            sc(
                string(),
                #{
                    mapping => "vm_args.-setcookie",
                    required => true,
                    'readOnly' => true,
                    sensitive => true,
                    desc => ?DESC(node_cookie),
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun emqx_schema:password_converter/2
                }
            )},
        {"process_limit",
            sc(
                range(1024, 134217727),
                #{
                    %% deprecated make sure it's disappeared in raw_conf user(HTTP API)
                    %% but still in vm.args via translation/1
                    %% ProcessLimit is always equal to MaxPort * 2 when translation/1.
                    deprecated => true,
                    desc => ?DESC(process_limit),
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"max_ports",
            sc(
                range(1024, 134217727),
                #{
                    mapping => "vm_args.+Q",
                    desc => ?DESC(max_ports),
                    default => ?DEFAULT_MAX_PORTS,
                    importance => ?IMPORTANCE_HIGH,
                    'readOnly' => true
                }
            )},
        {"dist_buffer_size",
            sc(
                range(1, 2097151),
                #{
                    mapping => "vm_args.+zdbbl",
                    desc => ?DESC(dist_buffer_size),
                    default => 8192,
                    importance => ?IMPORTANCE_LOW,
                    'readOnly' => true
                }
            )},
        {"max_ets_tables",
            sc(
                pos_integer(),
                #{
                    mapping => "vm_args.+e",
                    desc => ?DESC(max_ets_tables),
                    default => 262144,
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"data_dir",
            sc(
                string(),
                #{
                    required => true,
                    'readOnly' => true,
                    mapping => "emqx.data_dir",
                    %% for now, it's tricky to use a different data_dir
                    %% otherwise data paths in cluster config may differ
                    %% TODO: change configurable data file paths to relative
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(node_data_dir)
                }
            )},
        {"config_files",
            sc(
                hoconsc:array(string()),
                #{
                    mapping => "emqx.config_files",
                    importance => ?IMPORTANCE_HIDDEN,
                    required => false,
                    'readOnly' => true
                }
            )},
        {"global_gc_interval",
            sc(
                hoconsc:union([disabled, emqx_schema:duration()]),
                #{
                    mapping => "emqx_machine.global_gc_interval",
                    default => <<"15m">>,
                    desc => ?DESC(node_global_gc_interval),
                    importance => ?IMPORTANCE_LOW,
                    'readOnly' => true
                }
            )},
        {"crash_dump_file",
            sc(
                string(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP",
                    desc => ?DESC(node_crash_dump_file),
                    default => crash_dump_file_default(),
                    importance => ?IMPORTANCE_HIDDEN,
                    converter => fun ensure_unicode_path/2,
                    'readOnly' => true
                }
            )},
        {"crash_dump_seconds",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP_SECONDS",
                    default => <<"30s">>,
                    desc => ?DESC(node_crash_dump_seconds),
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"crash_dump_bytes",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "vm_args.-env ERL_CRASH_DUMP_BYTES",
                    default => <<"100MB">>,
                    desc => ?DESC(node_crash_dump_bytes),
                    importance => ?IMPORTANCE_HIDDEN,
                    'readOnly' => true
                }
            )},
        {"dist_net_ticktime",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "vm_args.-kernel net_ticktime",
                    default => <<"2m">>,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_dist_net_ticktime)
                }
            )},
        {"backtrace_depth",
            sc(
                integer(),
                #{
                    mapping => "emqx_machine.backtrace_depth",
                    default => 23,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_backtrace_depth)
                }
            )},
        {"applications",
            sc(
                emqx_schema:comma_separated_atoms(),
                #{
                    mapping => "emqx_machine.applications",
                    default => <<"">>,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(node_applications)
                }
            )},
        {"etc_dir",
            sc(
                string(),
                #{
                    desc => ?DESC(node_etc_dir),
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    deprecated => {since, "5.0.8"}
                }
            )},
        {"cluster_call",
            sc(
                ?R_REF("cluster_call"),
                #{
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"db_backend",
            sc(
                hoconsc:enum([mnesia, rlog]),
                #{
                    mapping => "mria.db_backend",
                    default => rlog,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_backend)
                }
            )},
        {"role",
            sc(
                hoconsc:enum(node_role_symbols()),
                #{
                    mapping => "mria.node_role",
                    default => core,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIGH,
                    aliases => [db_role],
                    desc => ?DESC(db_role),
                    validator => fun validate_node_role/1
                }
            )},
        {"rpc_module",
            sc(
                hoconsc:enum([gen_rpc, rpc]),
                #{
                    mapping => "mria.rlog_rpc_module",
                    default => rpc,
                    'readOnly' => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_rpc_module)
                }
            )},
        {"tlog_push_mode",
            sc(
                hoconsc:enum([sync, async]),
                #{
                    mapping => "mria.tlog_push_mode",
                    default => async,
                    'readOnly' => true,
                    deprecated => {since, "5.2.0"},
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(db_tlog_push_mode)
                }
            )},
        {"default_shard_transport",
            sc(
                hoconsc:enum([gen_rpc, distr]),
                #{
                    mapping => "mria.shard_transport",
                    importance => ?IMPORTANCE_HIDDEN,
                    default => distr,
                    desc => ?DESC(db_default_shard_transport)
                }
            )},
        {"shard_transports",
            sc(
                map(shard, hoconsc:enum([gen_rpc, distr])),
                #{
                    desc => ?DESC(db_shard_transports),
                    importance => ?IMPORTANCE_HIDDEN,
                    mapping => "emqx_machine.custom_shard_transports",
                    default => #{}
                }
            )},
        {"default_bootstrap_batch_size",
            sc(
                pos_integer(),
                #{
                    mapping => "mria.bootstrap_batch_size",
                    importance => ?IMPORTANCE_HIDDEN,
                    default => 500,
                    desc => ?DESC(db_default_bootstrap_batch_size)
                }
            )},
        {"broker_pool_size",
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    default => emqx_vm:schedulers() * 2,
                    'readOnly' => true,
                    desc => ?DESC(node_broker_pool_size)
                }
            )},
        {"generic_pool_size",
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    default => emqx_vm:schedulers(),
                    'readOnly' => true,
                    desc => ?DESC(node_generic_pool_size)
                }
            )},
        {"channel_cleanup_batch_size",
            sc(
                pos_integer(),
                #{
                    importance => ?IMPORTANCE_HIDDEN,
                    default => 100_000,
                    desc => ?DESC(node_channel_cleanup_batch_size)
                }
            )}
    ];
fields("cluster_call") ->
    [
        {"retry_interval",
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(cluster_call_retry_interval),
                    default => <<"1m">>
                }
            )},
        {"max_history",
            sc(
                range(100, 10240),
                #{
                    desc => ?DESC(cluster_call_max_history),
                    default => 1024
                }
            )},
        {"cleanup_interval",
            sc(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(cluster_call_cleanup_interval),
                    default => <<"24h">>
                }
            )}
    ];
fields("rpc") ->
    [
        {"mode",
            sc(
                hoconsc:enum([sync, async]),
                #{
                    default => async,
                    desc => ?DESC(rpc_mode)
                }
            )},
        {"protocol",
            sc(
                hoconsc:enum([tcp, ssl]),
                #{
                    mapping => "gen_rpc.driver",
                    aliases => [driver],
                    default => tcp,
                    desc => ?DESC(rpc_driver)
                }
            )},
        {"async_batch_size",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.max_batch_size",
                    default => 256,
                    desc => ?DESC(rpc_async_batch_size)
                }
            )},
        {"port_discovery",
            sc(
                hoconsc:enum([manual, stateless]),
                #{
                    mapping => "gen_rpc.port_discovery",
                    default => stateless,
                    desc => ?DESC(rpc_port_discovery)
                }
            )},
        {"server_port",
            sc(
                pos_integer(),
                #{
                    aliases => [tcp_server_port, ssl_server_port],
                    default => ?DEFAULT_RPC_PORT,
                    desc => ?DESC(rpc_server_port)
                }
            )},
        {"client_num",
            sc(
                range(1, 256),
                #{
                    aliases => [tcp_client_num],
                    default => 10,
                    desc => ?DESC(rpc_client_num)
                }
            )},
        {"connect_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.connect_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_connect_timeout)
                }
            )},
        {"certfile",
            sc(
                string(),
                #{
                    mapping => "gen_rpc.certfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_certfile)
                }
            )},
        {"keyfile",
            sc(
                string(),
                #{
                    mapping => "gen_rpc.keyfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_keyfile)
                }
            )},
        {"cacertfile",
            sc(
                string(),
                #{
                    mapping => "gen_rpc.cacertfile",
                    converter => fun ensure_unicode_path/2,
                    desc => ?DESC(rpc_cacertfile)
                }
            )},
        {"send_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.send_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_send_timeout)
                }
            )},
        {"authentication_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.authentication_timeout",
                    default => <<"5s">>,
                    desc => ?DESC(rpc_authentication_timeout)
                }
            )},
        {"call_receive_timeout",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "gen_rpc.call_receive_timeout",
                    default => <<"15s">>,
                    desc => ?DESC(rpc_call_receive_timeout)
                }
            )},
        {"socket_keepalive_idle",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "gen_rpc.socket_keepalive_idle",
                    default => <<"15m">>,
                    desc => ?DESC(rpc_socket_keepalive_idle)
                }
            )},
        {"socket_keepalive_interval",
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    mapping => "gen_rpc.socket_keepalive_interval",
                    default => <<"75s">>,
                    desc => ?DESC(rpc_socket_keepalive_interval)
                }
            )},
        {"socket_keepalive_count",
            sc(
                integer(),
                #{
                    mapping => "gen_rpc.socket_keepalive_count",
                    default => 9,
                    desc => ?DESC(rpc_socket_keepalive_count)
                }
            )},
        {"socket_sndbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_sndbuf",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_sndbuf)
                }
            )},
        {"socket_recbuf",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_recbuf",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_recbuf)
                }
            )},
        {"socket_buffer",
            sc(
                emqx_schema:bytesize(),
                #{
                    mapping => "gen_rpc.socket_buffer",
                    default => <<"1MB">>,
                    desc => ?DESC(rpc_socket_buffer)
                }
            )},
        {"insecure_fallback",
            sc(
                boolean(),
                #{
                    mapping => "gen_rpc.insecure_auth_fallback_allowed",
                    default => true,
                    desc => ?DESC(rpc_insecure_fallback)
                }
            )},
        {"ciphers", emqx_schema:ciphers_schema(tls_all_available)},
        {"tls_versions", emqx_schema:tls_versions_schema(tls_all_available)},
        {"listen_address",
            sc(
                string(),
                #{
                    default => <<"0.0.0.0">>,
                    desc => ?DESC(rpc_listen_address),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"ipv6_only",
            sc(
                boolean(),
                #{
                    default => false,
                    mapping => "gen_rpc.ipv6_only",
                    desc => ?DESC(rpc_ipv6_only),
                    importance => ?IMPORTANCE_LOW
                }
            )}
    ];
fields("log") ->
    [
        {"console",
            sc(?R_REF("console_handler"), #{
                aliases => [console_handler],
                importance => ?IMPORTANCE_HIGH
            })},
        {"file",
            sc(
                hoconsc:union([
                    ?R_REF("log_file_handler"),
                    ?MAP(handler_name, ?R_REF("log_file_handler"))
                ]),
                #{
                    desc => ?DESC("log_file_handlers"),
                    converter => fun ensure_file_handlers/2,
                    default => #{<<"level">> => <<"warning">>},
                    aliases => [file_handlers],
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {throttling,
            sc(?R_REF("log_throttling"), #{
                desc => ?DESC("log_throttling"),
                importance => ?IMPORTANCE_MEDIUM
            })}
    ];
fields("console_handler") ->
    log_handler_common_confs(console, #{});
fields("log_file_handler") ->
    [
        {"path",
            sc(
                string(),
                #{
                    desc => ?DESC("log_file_handler_file"),
                    default => <<"${EMQX_LOG_DIR}/emqx.log">>,
                    aliases => [file, to],
                    importance => ?IMPORTANCE_HIGH,
                    converter => fun log_file_path_converter/2
                }
            )},
        {"rotation_count",
            sc(
                range(1, 128),
                #{
                    aliases => [rotation],
                    default => 10,
                    converter => fun convert_rotation/2,
                    desc => ?DESC("log_rotation_count"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"rotation_size",
            sc(
                hoconsc:union([infinity, emqx_schema:bytesize()]),
                #{
                    default => <<"50MB">>,
                    desc => ?DESC("log_file_handler_max_size"),
                    aliases => [max_size],
                    importance => ?IMPORTANCE_MEDIUM
                }
            )}
    ] ++ log_handler_common_confs(file, #{});
fields("log_overload_kill") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC("log_overload_kill_enable")
                }
            )},
        {"mem_size",
            sc(
                emqx_schema:bytesize(),
                #{
                    default => <<"30MB">>,
                    desc => ?DESC("log_overload_kill_mem_size")
                }
            )},
        {"qlen",
            sc(
                pos_integer(),
                #{
                    default => 20000,
                    desc => ?DESC("log_overload_kill_qlen")
                }
            )},
        {"restart_after",
            sc(
                hoconsc:union([emqx_schema:timeout_duration_ms(), infinity]),
                #{
                    default => <<"5s">>,
                    desc => ?DESC("log_overload_kill_restart_after")
                }
            )}
    ];
fields("log_burst_limit") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC("log_burst_limit_enable")
                }
            )},
        {"max_count",
            sc(
                pos_integer(),
                #{
                    default => 10000,
                    desc => ?DESC("log_burst_limit_max_count")
                }
            )},
        {"window_time",
            sc(
                emqx_schema:duration(),
                #{
                    default => <<"1s">>,
                    desc => ?DESC("log_burst_limit_window_time")
                }
            )}
    ];
fields("log_throttling") ->
    [
        {time_window,
            sc(
                emqx_schema:timeout_duration_s(),
                #{
                    default => <<"1m">>,
                    desc => ?DESC("log_throttling_time_window"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        %% A static list of msgs used in ?SLOG_THROTTLE/2,3 macro.
        %% For internal (developer) use only.
        {msgs,
            sc(
                hoconsc:array(hoconsc:enum(?LOG_THROTTLING_MSGS)),
                #{
                    default => ?LOG_THROTTLING_MSGS,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields("authorization") ->
    emqx_schema:authz_fields() ++
        emqx_authz_schema:authz_fields().

desc("cluster") ->
    ?DESC("desc_cluster");
desc(cluster_static) ->
    ?DESC("desc_cluster_static");
desc(cluster_dns) ->
    ?DESC("desc_cluster_dns");
desc(cluster_etcd) ->
    ?DESC("desc_cluster_etcd");
desc(cluster_k8s) ->
    ?DESC("desc_cluster_k8s");
desc("node") ->
    ?DESC("desc_node");
desc("cluster_call") ->
    ?DESC("desc_cluster_call");
desc("rpc") ->
    ?DESC("desc_rpc");
desc("log") ->
    ?DESC("desc_log");
desc("console_handler") ->
    ?DESC("desc_console_handler");
desc("log_file_handler") ->
    ?DESC("desc_log_file_handler");
desc("log_rotation") ->
    ?DESC("desc_log_rotation");
desc("log_overload_kill") ->
    ?DESC("desc_log_overload_kill");
desc("log_burst_limit") ->
    ?DESC("desc_log_burst_limit");
desc("authorization") ->
    ?DESC("desc_authorization");
desc("log_throttling") ->
    ?DESC("desc_log_throttling");
desc(_) ->
    undefined.

translations() -> ["ekka", "kernel", "emqx", "gen_rpc", "prometheus", "vm_args"].

translation("ekka") ->
    [{"cluster_discovery", fun tr_cluster_discovery/1}];
translation("kernel") ->
    [
        {"logger_level", fun emqx_config_logger:tr_level/1},
        {"logger", fun emqx_config_logger:tr_handlers/1},
        {"error_logger", fun(_) -> silent end}
    ];
translation("emqx") ->
    [
        {"config_files", fun tr_config_files/1},
        {"cluster_override_conf_file", fun tr_cluster_override_conf_file/1},
        {"local_override_conf_file", fun tr_local_override_conf_file/1},
        {"cluster_hocon_file", fun tr_cluster_hocon_file/1}
    ];
translation("gen_rpc") ->
    [
        {"default_client_driver", fun tr_gen_rpc_default_client_driver/1},
        {"tcp_server_port", fun tr_gen_rpc_port/1},
        {"ssl_server_port", fun tr_gen_rpc_port/1},
        {"tcp_client_port", fun tr_gen_rpc_port/1},
        {"ssl_client_port", fun tr_gen_rpc_port/1},
        {"ssl_client_options", fun tr_gen_rpc_ssl_options/1},
        {"ssl_server_options", fun tr_gen_rpc_ssl_options/1},
        {"socket_ip", fun(Conf) ->
            Addr = conf_get("rpc.listen_address", Conf),
            case inet:parse_address(Addr) of
                {ok, Tuple} ->
                    Tuple;
                {error, _Reason} ->
                    throw(#{bad_ip_address => Addr})
            end
        end}
    ];
translation("prometheus") ->
    [
        {"collectors", fun tr_prometheus_collectors/1}
    ];
translation("vm_args") ->
    [
        {"+P", fun tr_vm_args_process_limit/1}
    ].

tr_vm_args_process_limit(Conf) ->
    2 * conf_get("node.max_ports", Conf, ?DEFAULT_MAX_PORTS).

tr_prometheus_collectors(Conf) ->
    [
        %% builtin collectors
        prometheus_boolean,
        prometheus_counter,
        prometheus_gauge,
        prometheus_histogram,
        prometheus_quantile_summary,
        prometheus_summary,
        %% emqx collectors
        emqx_prometheus,
        {'/prometheus/auth', emqx_prometheus_auth},
        {'/prometheus/data_integration', emqx_prometheus_data_integration}
        %% builtin vm collectors
        | prometheus_collectors(Conf)
    ].

prometheus_collectors(Conf) ->
    case conf_get("prometheus.enable_basic_auth", Conf, undefined) of
        %% legacy
        undefined ->
            tr_collector("prometheus.vm_dist_collector", prometheus_vm_dist_collector, Conf) ++
                tr_collector("prometheus.mnesia_collector", prometheus_mnesia_collector, Conf) ++
                tr_collector(
                    "prometheus.vm_statistics_collector", prometheus_vm_statistics_collector, Conf
                ) ++
                tr_collector(
                    "prometheus.vm_system_info_collector", prometheus_vm_system_info_collector, Conf
                ) ++
                tr_collector("prometheus.vm_memory_collector", prometheus_vm_memory_collector, Conf) ++
                tr_collector("prometheus.vm_msacc_collector", prometheus_vm_msacc_collector, Conf);
        %% new
        _ ->
            tr_collector("prometheus.collectors.vm_dist", prometheus_vm_dist_collector, Conf) ++
                tr_collector("prometheus.collectors.mnesia", prometheus_mnesia_collector, Conf) ++
                tr_collector(
                    "prometheus.collectors.vm_statistics", prometheus_vm_statistics_collector, Conf
                ) ++
                tr_collector(
                    "prometheus.collectors.vm_system_info",
                    prometheus_vm_system_info_collector,
                    Conf
                ) ++
                tr_collector(
                    "prometheus.collectors.vm_memory", prometheus_vm_memory_collector, Conf
                ) ++
                tr_collector("prometheus.collectors.vm_msacc", prometheus_vm_msacc_collector, Conf)
    end.

tr_collector(Key, Collect, Conf) ->
    Enabled = conf_get(Key, Conf, disabled),
    collector_enabled(Enabled, Collect).

collector_enabled(enabled, Collector) -> [Collector];
collector_enabled(disabled, _) -> [].

tr_gen_rpc_default_client_driver(Conf) ->
    conf_get("rpc.protocol", Conf).

tr_gen_rpc_port(Conf) ->
    conf_get("rpc.server_port", Conf).

tr_gen_rpc_ssl_options(Conf) ->
    Ciphers = conf_get("rpc.ciphers", Conf),
    Versions = conf_get("rpc.tls_versions", Conf),
    [{ciphers, Ciphers}, {versions, Versions}].

tr_config_files(_Conf) ->
    case os:getenv("EMQX_ETC_DIR") of
        false ->
            %% testing, or running emqx app as deps
            [filename:join([code:lib_dir(emqx), "etc", "emqx.conf"])];
        Dir ->
            [filename:join([Dir, "emqx.conf"])]
    end.

tr_cluster_override_conf_file(Conf) ->
    tr_conf_file(Conf, "cluster-override.conf").

tr_local_override_conf_file(Conf) ->
    tr_conf_file(Conf, "local-override.conf").

tr_cluster_hocon_file(Conf) ->
    tr_conf_file(Conf, "cluster.hocon").

tr_conf_file(Conf, Filename) ->
    DataDir = conf_get("node.data_dir", Conf),
    %% assert, this config is not nullable
    [_ | _] = DataDir,
    filename:join([DataDir, "configs", Filename]).

tr_cluster_discovery(Conf) ->
    Strategy = conf_get("cluster.discovery_strategy", Conf),
    {Strategy, filter(cluster_options(Strategy, Conf))}.

log_handler_common_confs(Handler, Default) ->
    %% We rarely support dynamic defaults like this.
    %% For this one, we have build-time default the same as runtime default so it's less tricky
    %% Build time default: "" (which is the same as "file")
    %% Runtime default: "file" (because .service file sets EMQX_DEFAULT_LOG_HANDLER to "file")
    EnableValues =
        case Handler of
            console -> ["console", "both"];
            file -> ["file", "both", "", false]
        end,
    EnvValue = os:getenv("EMQX_DEFAULT_LOG_HANDLER"),
    Enable = lists:member(EnvValue, EnableValues),
    LevelDesc = maps:get(level_desc, Default, "common_handler_level"),
    EnableImportance =
        case Enable of
            true -> ?IMPORTANCE_NO_DOC;
            false -> ?IMPORTANCE_MEDIUM
        end,
    [
        {"level",
            sc(
                log_level(),
                #{
                    default => maps:get(level, Default, warning),
                    desc => ?DESC(LevelDesc),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
        {"enable",
            sc(
                boolean(),
                #{
                    default => Enable,
                    desc => ?DESC("common_handler_enable"),
                    importance => EnableImportance
                }
            )},
        {"formatter",
            sc(
                hoconsc:enum([text, json]),
                #{
                    aliases => [format],
                    default => maps:get(formatter, Default, text),
                    desc => ?DESC("common_handler_formatter"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"timestamp_format",
            sc(
                hoconsc:enum([auto, epoch, rfc3339]),
                #{
                    default => auto,
                    desc => ?DESC("common_handler_timestamp_format"),
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {"time_offset",
            sc(
                string(),
                #{
                    default => <<"system">>,
                    desc => ?DESC("common_handler_time_offset"),
                    validator => fun validate_time_offset/1,
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"chars_limit",
            sc(
                hoconsc:union([unlimited, range(100, inf)]),
                #{
                    default => unlimited,
                    desc => ?DESC("common_handler_chars_limit"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"single_line",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("common_handler_single_line"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"sync_mode_qlen",
            sc(
                non_neg_integer(),
                #{
                    default => 100,
                    desc => ?DESC("common_handler_sync_mode_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"drop_mode_qlen",
            sc(
                pos_integer(),
                #{
                    default => 3000,
                    desc => ?DESC("common_handler_drop_mode_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"flush_qlen",
            sc(
                pos_integer(),
                #{
                    default => 8000,
                    desc => ?DESC("common_handler_flush_qlen"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"overload_kill", sc(?R_REF("log_overload_kill"), #{importance => ?IMPORTANCE_HIDDEN})},
        {"burst_limit", sc(?R_REF("log_burst_limit"), #{importance => ?IMPORTANCE_HIDDEN})},
        {"supervisor_reports",
            sc(
                hoconsc:enum([error, progress]),
                #{
                    default => error,
                    desc => ?DESC("common_handler_supervisor_reports"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"max_depth",
            sc(
                hoconsc:union([unlimited, non_neg_integer()]),
                #{
                    default => 100,
                    desc => ?DESC("common_handler_max_depth"),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"with_mfa",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => <<"Recording MFA and line in the log(usefully).">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"payload_encode",
            sc(hoconsc:enum([hex, text, hidden]), #{
                default => text,
                desc => ?DESC(emqx_schema, fields_trace_payload_encode)
            })}
    ].

crash_dump_file_default() ->
    case os:getenv("EMQX_LOG_DIR") of
        false ->
            %% testing, or running emqx app as deps
            <<"log/erl_crash.dump">>;
        Dir ->
            unicode:characters_to_binary(filename:join([Dir, "erl_crash.dump"]), utf8)
    end.

%% utils
-spec conf_get(string() | [string()], hocon:config()) -> term().
conf_get(Key, Conf) -> emqx_schema:conf_get(Key, Conf).

conf_get(Key, Conf, Default) -> emqx_schema:conf_get(Key, Conf, Default).

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private return a list of keys in a parent field
-spec keys(string(), hocon:config()) -> [string()].
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

cluster_options(static, Conf) ->
    [{seeds, conf_get("cluster.static.seeds", Conf, [])}];
cluster_options(dns, Conf) ->
    [
        {name, conf_get("cluster.dns.name", Conf)},
        {type, conf_get("cluster.dns.record_type", Conf)}
    ];
cluster_options(etcd, Conf) ->
    Namespace = "cluster.etcd.ssl_options",
    SslOpts = fun(C) ->
        Options = keys(Namespace, C),
        lists:map(fun(Key) -> {to_atom(Key), conf_get([Namespace, Key], Conf)} end, Options)
    end,
    [
        {server, conf_get("cluster.etcd.server", Conf)},
        {prefix, conf_get("cluster.etcd.prefix", Conf, "emqxcl")},
        {node_ttl, conf_get("cluster.etcd.node_ttl", Conf, 60)},
        {ssl_options, filter(SslOpts(Conf))}
    ];
cluster_options(k8s, Conf) ->
    [
        {apiserver, conf_get("cluster.k8s.apiserver", Conf)},
        {service_name, conf_get("cluster.k8s.service_name", Conf)},
        {address_type, conf_get("cluster.k8s.address_type", Conf, ip)},
        {namespace, conf_get("cluster.k8s.namespace", Conf)},
        {suffix, conf_get("cluster.k8s.suffix", Conf, "")}
    ];
cluster_options(manual, _Conf) ->
    [];
cluster_options(singleton, _Conf) ->
    [].

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

roots(Module) ->
    lists:map(fun({_BinName, Root}) -> Root end, hocon_schema:roots(Module)).

%% Like authentication schema, authorization schema is incomplete in emqx_schema
%% module, this function replaces the root field 'authorization' with a new schema
emqx_schema_high_prio_roots() ->
    Roots = emqx_schema:roots(high),
    Authz =
        {authorization,
            sc(
                ?R_REF("authorization"),
                #{
                    desc => ?DESC("authorization"),
                    importance => ?IMPORTANCE_HIGH
                }
            )},
    lists:keyreplace(authorization, 1, Roots, Authz).

validate_time_offset(Offset) ->
    ValidTimeOffset = "^([\\-\\+][0-1][0-9]:[0-6][0-9]|system|utc)$",
    Error =
        "Invalid time offset, should be of format: +[hh]:[mm], "
        "i.e. +08:00 or -02:00",
    validator_string_re(Offset, ValidTimeOffset, Error).

validator_string_re(Val, RE, Error) ->
    try re:run(Val, RE) of
        nomatch -> {error, Error};
        _ -> ok
    catch
        _:_ -> {error, Error}
    end.

node_array() ->
    hoconsc:union([emqx_schema:comma_separated_atoms(), hoconsc:array(atom())]).

ensure_file_handlers(Conf, _Opts) ->
    FileFields = lists:flatmap(
        fun({F, Schema}) ->
            Alias = [atom_to_binary(A) || A <- maps:get(aliases, Schema, [])],
            [list_to_binary(F) | Alias]
        end,
        fields("log_file_handler")
    ),
    HandlersWithoutName = maps:with(FileFields, Conf),
    HandlersWithName = maps:without(FileFields, Conf),
    %% Make sure the handler with name is high priority than the handler without name
    emqx_utils_maps:deep_merge(#{<<"default">> => HandlersWithoutName}, HandlersWithName).

convert_rotation(undefined, _Opts) -> undefined;
convert_rotation(#{} = Rotation, _Opts) -> maps:get(<<"count">>, Rotation, 10);
convert_rotation(Count, _Opts) when is_integer(Count) -> Count;
convert_rotation(Count, _Opts) -> throw({"bad_rotation", Count}).

log_file_path_converter(Path, Opts) ->
    Fixed = fix_bad_log_path(Path),
    ensure_unicode_path(Fixed, Opts).

%% Prior to 5.8.3, the log file paths are resolved by scehma module
%% and the interpolated paths (absolute paths) are exported.
%% When exported from docker but import to a non-docker environment,
%% the absolute paths are not valid anymore.
%% Here we try to fix non-existing log dir with default log dir.
fix_bad_log_path(Bin) when is_binary(Bin) ->
    try
        List = [_ | _] = unicode:characters_to_list(Bin, utf8),
        Fixed = fix_bad_log_path(List),
        unicode:characters_to_binary(Fixed, utf8)
    catch
        _:_ ->
            %% defer validation to ensure_unicode_path
            Bin
    end;
fix_bad_log_path(Path) when is_list(Path) ->
    Dir = filename:dirname(Path),
    Name = filename:basename(Path),
    maybe_subst_log_dir(Dir, Name);
fix_bad_log_path(Path) ->
    %% defer validation to ensure_unicode_path
    Path.

%% Substitute the log dir with environment variable EMQX_LOG_DIR
%% when possible
maybe_subst_log_dir("${" ++ _ = Dir, Name) ->
    %% the original path is already using environment variable
    filename:join([Dir, Name]);
maybe_subst_log_dir(Dir, Name) ->
    Env = os:getenv("EMQX_LOG_DIR"),
    IsEnvSet = (Env =/= false andalso Env =/= ""),
    case Env =:= Dir of
        true ->
            %% the path is the same as the environment variable
            %% substitute it with the environment variable
            filename:join(["${EMQX_LOG_DIR}", Name]);
        false ->
            case filelib:is_dir(Dir) of
                true ->
                    %% the path exists, keep it
                    filename:join(Dir, Name);
                false when IsEnvSet ->
                    %% the path does not exist, but the environment variable is set
                    %% substitute it with the environment variable
                    filename:join(["${EMQX_LOG_DIR}", Name]);
                false ->
                    %% the path does not exist, and the environment variable is not set
                    %% keep it
                    filename:join(Dir, Name)
            end
    end.

ensure_unicode_path(Path, Opts) ->
    emqx_schema:ensure_unicode_path(Path, Opts).

log_level() ->
    hoconsc:enum([debug, info, notice, warning, error, critical, alert, emergency, all]).

validate_cluster_strategy(#{<<"node">> := _, <<"cluster">> := _} = Conf) ->
    Name = hocon_maps:get("node.name", Conf),
    [_Prefix, Host] = re:split(Name, "@", [{return, list}, unicode]),
    Strategy = hocon_maps:get("cluster.discovery_strategy", Conf),
    Type = hocon_maps:get("cluster.dns.record_type", Conf),
    validate_dns_cluster_strategy(Strategy, Type, Host);
validate_cluster_strategy(_) ->
    true.

validate_dns_cluster_strategy(dns, srv, _Host) ->
    ok;
validate_dns_cluster_strategy(dns, Type, Host) ->
    case is_ip_addr(unicode:characters_to_list(Host), Type) of
        true ->
            ok;
        false ->
            throw(#{
                explain =>
                    "Node name must be of name@IP format "
                    "for DNS cluster discovery strategy with '" ++ atom_to_list(Type) ++
                    "' record type.",
                domain => unicode:characters_to_list(Host)
            })
    end;
validate_dns_cluster_strategy(_Other, _Type, _Name) ->
    true.

is_ip_addr(Host, Type) ->
    case inet:parse_address(Host) of
        {ok, Ip} ->
            AddrType = address_type(Ip),
            case
                (AddrType =:= ipv4 andalso Type =:= a) orelse
                    (AddrType =:= ipv6 andalso Type =:= aaaa)
            of
                true ->
                    true;
                false ->
                    throw(#{
                        explain => "Node name address " ++ atom_to_list(AddrType) ++
                            " is incompatible with DNS record type " ++ atom_to_list(Type),
                        record_type => Type,
                        address_type => address_type(Ip)
                    })
            end;
        _ ->
            false
    end.

address_type(IP) when tuple_size(IP) =:= 4 -> ipv4;
address_type(IP) when tuple_size(IP) =:= 8 -> ipv6.

node_role_symbols() ->
    [core] ++ emqx_schema_hooks:list_injection_point('node.role').

validate_node_role(Role) ->
    Allowed = node_role_symbols(),
    case lists:member(Role, Allowed) of
        true ->
            ok;
        false when Role =:= replicant ->
            throw("Node role 'replicant' is only allowed in Enterprise edition since 5.8.0");
        false ->
            throw("Invalid node role: " ++ atom_to_list(Role))
    end.
