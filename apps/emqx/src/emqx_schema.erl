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

-module(emqx_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-include("emqx_schema.hrl").
-include("emqx_access_control.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("logger.hrl").

-define(MAX_INT_MQTT_PACKET_SIZE, 268435455).
-define(MAX_INT_TIMEOUT_MS, 4294967295).
%% floor(?MAX_INT_TIMEOUT_MS / 1000).
-define(MAX_INT_TIMEOUT_S, 4294967).
-define(DEFAULT_WINDOW_TIME, <<"1m">>).

-type duration() :: integer().
-type duration_s() :: integer().
-type duration_ms() :: integer().
%% ?MAX_INT_TIMEOUT is defined loosely in some OTP modules like
%% `erpc', `rpc' `gen' and `peer', despite affecting `receive' blocks
%% as well.  It's `2^32 - 1'.
-type timeout_duration() :: 0..?MAX_INT_TIMEOUT_MS.
-type timeout_duration_s() :: 0..?MAX_INT_TIMEOUT_S.
-type timeout_duration_ms() :: 0..?MAX_INT_TIMEOUT_MS.
-type bytesize() :: integer().
-type wordsize() :: bytesize().
-type percent() :: float().
-type comma_separated_list() :: list(string()).
-type comma_separated_binary() :: [binary()].
-type comma_separated_atoms() :: [atom()].
-type ip_port() :: tuple() | integer().
-type cipher() :: map().
-type port_number() :: 1..65535.
-type server_parse_option() :: #{
    default_port => port_number(),
    no_port => boolean(),
    supported_schemes => [string()],
    default_scheme => string()
}.
-type url() :: binary().
-type json_binary() :: binary().
-type template() :: binary().
-type template_str() :: string().
-type binary_kv() :: #{binary() => binary()}.

-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({duration_ms/0, emqx_schema, to_duration_ms}).
-typerefl_from_string({timeout_duration/0, emqx_schema, to_timeout_duration}).
-typerefl_from_string({timeout_duration_s/0, emqx_schema, to_timeout_duration_s}).
-typerefl_from_string({timeout_duration_ms/0, emqx_schema, to_timeout_duration_ms}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({wordsize/0, emqx_schema, to_wordsize}).
-typerefl_from_string({percent/0, emqx_schema, to_percent}).
-typerefl_from_string({comma_separated_list/0, emqx_schema, to_comma_separated_list}).
-typerefl_from_string({comma_separated_binary/0, emqx_schema, to_comma_separated_binary}).
-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({cipher/0, emqx_schema, to_erl_cipher_suite}).
-typerefl_from_string({comma_separated_atoms/0, emqx_schema, to_comma_separated_atoms}).
-typerefl_from_string({url/0, emqx_schema, to_url}).
-typerefl_from_string({json_binary/0, emqx_schema, to_json_binary}).
-typerefl_from_string({template/0, emqx_schema, to_template}).
-typerefl_from_string({template_str/0, emqx_schema, to_template_str}).

-type parsed_server() :: #{
    hostname := string(),
    port => port_number(),
    scheme => string()
}.

-export([
    validate_heap_size/1,
    validate_max_packet_size/1,
    convert_max_packet_size/2,
    user_lookup_fun_tr/2,
    validate_keepalive_multiplier/1,
    non_empty_string/1,
    non_empty_array/1,
    validations/0,
    naive_env_interpolation/1,
    ensure_unicode_path/2,
    validate_server_ssl_opts/1,
    validate_tcp_keepalive/1,
    parse_tcp_keepalive/1
]).

-export([qos/0]).

% workaround: prevent being recognized as unused functions
-export([
    to_duration/1,
    to_duration_s/1,
    to_duration_ms/1,
    to_timeout_duration/1,
    to_timeout_duration_s/1,
    to_timeout_duration_ms/1,
    mk_duration/2,
    to_bytesize/1,
    to_wordsize/1,
    to_percent/1,
    to_comma_separated_list/1,
    to_comma_separated_binary/1,
    to_ip_port/1,
    to_erl_cipher_suite/1,
    to_comma_separated_atoms/1,
    to_url/1,
    to_json_binary/1,
    to_template/1,
    to_template_str/1
]).

-export([
    parse_server/2,
    parse_servers/2,
    servers_validator/2,
    servers_sc/2,
    convert_servers/1,
    convert_servers/2,
    mqtt_converter/2
]).

%% tombstone types
-export([
    tombstone_map/2,
    get_tombstone_map_value_type/1
]).

-export([listeners/0]).
-export([mkunion/2, mkunion/3]).

-behaviour(hocon_schema).

-reflect_type([
    duration/0,
    duration_s/0,
    duration_ms/0,
    timeout_duration/0,
    timeout_duration_s/0,
    timeout_duration_ms/0,
    bytesize/0,
    wordsize/0,
    percent/0,
    comma_separated_list/0,
    comma_separated_binary/0,
    ip_port/0,
    cipher/0,
    comma_separated_atoms/0,
    url/0,
    json_binary/0,
    port_number/0,
    template/0,
    template_str/0,
    binary_kv/0
]).

-export([namespace/0, roots/0, roots/1, fields/1, desc/1, tags/0]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([
    server_ssl_opts_schema/2,
    client_ssl_opts_schema/1,
    ciphers_schema/1,
    tls_versions_schema/1,
    description_schema/0,
    tags_schema/0
]).
-export([password_converter/2, bin_str_converter/2]).
-export([authz_fields/0]).
-export([sc/2, map/2]).

-elvis([{elvis_style, god_modules, disable}]).

-define(BIT(Bits), (1 bsl (Bits))).
-define(MAX_UINT(Bits), (?BIT(Bits) - 1)).
-define(DEFAULT_MULTIPLIER, 1.5).
-define(DEFAULT_BACKOFF, 0.75).

namespace() -> emqx.

tags() ->
    [<<"EMQX">>].

roots() ->
    %% TODO change config importance to a field metadata
    roots(high) ++ roots(medium) ++ roots(low).

roots(high) ->
    [
        {listeners,
            sc(
                ref("listeners"),
                #{importance => ?IMPORTANCE_HIGH}
            )},
        {mqtt,
            sc(
                ref("mqtt"),
                #{
                    desc => ?DESC(mqtt),
                    converter => fun ?MODULE:mqtt_converter/2,
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {zones, zones_field_schema()}
    ] ++
        emqx_schema_hooks:list_injection_point(
            'roots.high',
            [
                %% NOTE: authorization schema here is only to keep emqx app pure
                %% the full schema for EMQX node is injected in emqx_conf_schema.
                {?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_ATOM,
                    sc(
                        ref(?EMQX_AUTHORIZATION_CONFIG_ROOT_NAME),
                        #{importance => ?IMPORTANCE_HIDDEN}
                    )}
            ]
        );
roots(medium) ->
    [
        {broker,
            sc(
                ref("broker"),
                #{
                    desc => ?DESC(broker),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {sys_topics,
            sc(
                ref("sys_topics"),
                #{desc => ?DESC(sys_topics)}
            )},
        {force_shutdown,
            sc(
                ref("force_shutdown"),
                #{}
            )},
        {overload_protection,
            sc(
                ref("overload_protection"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {durable_storage,
            sc(
                ref(durable_storage),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    desc => ?DESC(durable_storage)
                }
            )}
    ];
roots(low) ->
    [
        {force_gc,
            sc(
                ref("force_gc"),
                #{}
            )},
        {conn_congestion,
            sc(
                ref("conn_congestion"),
                #{
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {stats,
            sc(
                ref("stats"),
                #{
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {sysmon,
            sc(
                ref("sysmon"),
                #{}
            )},
        {alarm,
            sc(
                ref("alarm"),
                #{}
            )},
        {flapping_detect,
            sc(
                ref("flapping_detect"),
                #{
                    importance => ?IMPORTANCE_MEDIUM,
                    converter => fun flapping_detect_converter/2
                }
            )},
        {durable_sessions,
            sc(
                ref("durable_sessions"),
                #{
                    importance => ?IMPORTANCE_MEDIUM
                }
            )},
        {trace,
            sc(
                ref("trace"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {crl_cache,
            sc(
                ref("crl_cache"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {banned,
            sc(
                ref("banned"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )}
    ].

fields("stats") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(stats_enable)
                }
            )}
    ];
fields("authorization") ->
    authz_fields();
fields("authz_cache") ->
    [
        {enable,
            sc(
                boolean(),
                #{
                    default => true,
                    required => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(fields_cache_enable)
                }
            )},
        {max_size,
            sc(
                range(1, 1048576),
                #{
                    default => 32,
                    desc => ?DESC(fields_cache_max_size)
                }
            )},
        {ttl,
            sc(
                duration(),
                #{
                    default => <<"1m">>,
                    desc => ?DESC(fields_cache_ttl)
                }
            )},
        {excludes,
            sc(hoconsc:array(binary()), #{
                default => [],
                desc => ?DESC(fields_authz_cache_excludes)
            })}
    ];
fields("mqtt") ->
    mqtt_general() ++ mqtt_session();
fields("zone") ->
    emqx_zone_schema:zones_without_default();
fields("flapping_detect") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => false,
                    %% importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(flapping_detect_enable)
                }
            )},
        {"window_time",
            sc(
                duration(),
                #{
                    default => ?DEFAULT_WINDOW_TIME,
                    importance => ?IMPORTANCE_HIGH,
                    desc => ?DESC(flapping_detect_window_time)
                }
            )},
        {"max_count",
            sc(
                non_neg_integer(),
                #{
                    default => 15,
                    desc => ?DESC(flapping_detect_max_count)
                }
            )},
        {"ban_time",
            sc(
                duration(),
                #{
                    default => <<"5m">>,
                    desc => ?DESC(flapping_detect_ban_time)
                }
            )}
    ];
fields("force_shutdown") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(force_shutdown_enable)
                }
            )},
        {"max_mailbox_size",
            sc(
                range(0, inf),
                #{
                    default => 1000,
                    aliases => [max_message_queue_len],
                    desc => ?DESC(force_shutdown_max_mailbox_size)
                }
            )},
        {"max_heap_size",
            sc(
                wordsize(),
                #{
                    default => <<"32MB">>,
                    desc => ?DESC(force_shutdown_max_heap_size),
                    validator => fun ?MODULE:validate_heap_size/1
                }
            )}
    ];
fields("overload_protection") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_enable),
                    %% importance => ?IMPORTANCE_NO_DOC,
                    default => false
                }
            )},
        {"backoff_delay",
            sc(
                range(0, inf),
                #{
                    desc => ?DESC(overload_protection_backoff_delay),
                    default => 1
                }
            )},
        {"backoff_gc",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_gc),
                    default => false
                }
            )},
        {"backoff_hibernation",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_hibernation),
                    default => true
                }
            )},
        {"backoff_new_conn",
            sc(
                boolean(),
                #{
                    desc => ?DESC(overload_protection_backoff_new_conn),
                    default => true
                }
            )}
    ];
fields("conn_congestion") ->
    [
        {"enable_alarm",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(conn_congestion_enable_alarm)
                }
            )},
        {"min_alarm_sustain_duration",
            sc(
                duration(),
                #{
                    default => <<"1m">>,
                    desc => ?DESC(conn_congestion_min_alarm_sustain_duration)
                }
            )}
    ];
fields("force_gc") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(force_gc_enable)
                }
            )},
        {"count",
            sc(
                range(0, inf),
                #{
                    default => 16000,
                    desc => ?DESC(force_gc_count)
                }
            )},
        {"bytes",
            sc(
                bytesize(),
                #{
                    default => <<"16MB">>,
                    desc => ?DESC(force_gc_bytes)
                }
            )}
    ];
fields("listeners") ->
    listeners();
fields("crl_cache") ->
    %% Note: we make the refresh interval and HTTP timeout global (not
    %% per-listener) because multiple SSL listeners might point to the
    %% same URL.  If they had diverging timeout options, it would be
    %% confusing.
    [
        {refresh_interval,
            sc(
                duration(),
                #{
                    default => <<"15m">>,
                    desc => ?DESC("crl_cache_refresh_interval")
                }
            )},
        {http_timeout,
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("crl_cache_refresh_http_timeout")
                }
            )},
        {capacity,
            sc(
                pos_integer(),
                #{
                    default => 100,
                    desc => ?DESC("crl_cache_capacity")
                }
            )}
    ];
fields("mqtt_tcp_listener") ->
    mqtt_listener(1883) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )}
        ];
fields("mqtt_ssl_listener") ->
    mqtt_listener(8883) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"ssl_options",
                sc(
                    ref("listener_ssl_opts"),
                    #{validator => fun mqtt_ssl_listener_ssl_options_validator/1}
                )}
        ];
fields("mqtt_ws_listener") ->
    mqtt_listener(8083) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"websocket",
                sc(
                    ref("ws_opts"),
                    #{}
                )}
        ];
fields("mqtt_wss_listener") ->
    mqtt_listener(8084) ++
        [
            {"tcp_options",
                sc(
                    ref("tcp_opts"),
                    #{}
                )},
            {"ssl_options",
                sc(
                    ref("listener_wss_opts"),
                    #{validator => fun validate_server_ssl_opts/1}
                )},
            {"websocket",
                sc(
                    ref("ws_opts"),
                    #{}
                )}
        ];
fields("mqtt_quic_listener") ->
    [
        {"certfile",
            sc(
                string(),
                #{
                    deprecated => {since, "5.1.0"},
                    desc => ?DESC(fields_mqtt_quic_listener_certfile),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"keyfile",
            sc(
                string(),
                #{
                    deprecated => {since, "5.1.0"},
                    desc => ?DESC(fields_mqtt_quic_listener_keyfile),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"ciphers", ciphers_schema(quic)},

        {"max_bytes_per_key",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(64),
                ?DESC(fields_mqtt_quic_listener_max_bytes_per_key)
            )},
        {"tls_server_max_send_buffer",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_tls_server_max_send_buffer)
            )},
        {"stream_recv_window_default",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_stream_recv_window_default)
            )},
        {"stream_recv_buffer_default",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_stream_recv_buffer_default)
            )},
        {"conn_flow_control_window",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_conn_flow_control_window)
            )},
        {"max_stateless_operations",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_max_stateless_operations)
            )},
        {"initial_window_packets",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_initial_window_packets)
            )},
        {"send_idle_timeout_ms",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_send_idle_timeout_ms)
            )},
        {"initial_rtt_ms",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_initial_rtt_ms)
            )},
        {"max_ack_delay_ms",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_max_ack_delay_ms)
            )},
        {"disconnect_timeout_ms",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_disconnect_timeout_ms)
            )},
        {"idle_timeout",
            sc(
                timeout_duration_ms(),
                #{
                    default => 0,
                    desc => ?DESC(fields_mqtt_quic_listener_idle_timeout),
                    deprecated => {since, "5.1.0"},
                    %% deprecated, use idle_timeout_ms instead
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"idle_timeout_ms",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(64),
                ?DESC(fields_mqtt_quic_listener_idle_timeout_ms)
            )},
        {"handshake_idle_timeout",
            sc(
                timeout_duration_ms(),
                #{
                    default => <<"10s">>,
                    desc => ?DESC(fields_mqtt_quic_listener_handshake_idle_timeout),
                    deprecated => {since, "5.1.0"},
                    %% use handshake_idle_timeout_ms
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"handshake_idle_timeout_ms",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(64),
                ?DESC(fields_mqtt_quic_listener_handshake_idle_timeout_ms)
            )},
        {"keep_alive_interval",
            sc(
                timeout_duration_ms(),
                #{
                    default => 0,
                    desc => ?DESC(fields_mqtt_quic_listener_keep_alive_interval),
                    %% TODO: deprecated => {since, "5.1.0"}
                    %% use keep_alive_interval_ms instead
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"keep_alive_interval_ms",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(32),
                ?DESC(fields_mqtt_quic_listener_keep_alive_interval_ms)
            )},
        {"peer_bidi_stream_count",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_peer_bidi_stream_count)
            )},
        {"peer_unidi_stream_count",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_peer_unidi_stream_count)
            )},
        {"retry_memory_limit",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_retry_memory_limit)
            )},
        {"load_balancing_mode",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_load_balancing_mode)
            )},
        {"max_operations_per_drain",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(8),
                ?DESC(fields_mqtt_quic_listener_max_operations_per_drain)
            )},
        {"send_buffering_enabled",
            quic_feature_toggle(
                ?DESC(fields_mqtt_quic_listener_send_buffering_enabled)
            )},
        {"pacing_enabled",
            quic_feature_toggle(
                ?DESC(fields_mqtt_quic_listener_pacing_enabled)
            )},
        {"migration_enabled",
            quic_feature_toggle(
                ?DESC(fields_mqtt_quic_listener_migration_enabled)
            )},
        {"datagram_receive_enabled",
            quic_feature_toggle(
                ?DESC(fields_mqtt_quic_listener_datagram_receive_enabled)
            )},
        {"server_resumption_level",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(8),
                ?DESC(fields_mqtt_quic_listener_server_resumption_level)
            )},
        {"minimum_mtu",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_minimum_mtu)
            )},
        {"maximum_mtu",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_maximum_mtu)
            )},
        {"mtu_discovery_search_complete_timeout_us",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(64),
                ?DESC(fields_mqtt_quic_listener_mtu_discovery_search_complete_timeout_us)
            )},
        {"mtu_discovery_missing_probe_count",
            quic_lowlevel_settings_uint(
                1,
                ?MAX_UINT(8),
                ?DESC(fields_mqtt_quic_listener_mtu_discovery_missing_probe_count)
            )},
        {"max_binding_stateless_operations",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_max_binding_stateless_operations)
            )},
        {"stateless_operation_expiration_ms",
            quic_lowlevel_settings_uint(
                0,
                ?MAX_UINT(16),
                ?DESC(fields_mqtt_quic_listener_stateless_operation_expiration_ms)
            )},
        {"sslkeylogfile",
            sc(
                string(),
                #{
                    desc => ?DESC(fields_mqtt_quic_listener_sslkeylogfile),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"ssl_options",
            sc(
                ref("listener_quic_ssl_opts"),
                #{
                    required => false,
                    desc => ?DESC(fields_mqtt_quic_listener_ssl_options)
                }
            )}
    ] ++ base_listener(14567);
fields("ws_opts") ->
    [
        {"mqtt_path",
            sc(
                string(),
                #{
                    default => <<"/mqtt">>,
                    desc => ?DESC(fields_ws_opts_mqtt_path)
                }
            )},
        {"mqtt_piggyback",
            sc(
                hoconsc:enum([single, multiple]),
                #{
                    default => multiple,
                    desc => ?DESC(fields_ws_opts_mqtt_piggyback)
                }
            )},
        {"compress",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_compress)
                }
            )},
        {"idle_timeout",
            sc(
                duration(),
                #{
                    default => <<"7200s">>,
                    desc => ?DESC(fields_ws_opts_idle_timeout)
                }
            )},
        {"max_frame_size",
            sc(
                hoconsc:union([infinity, pos_integer()]),
                #{
                    default => infinity,
                    desc => ?DESC(fields_ws_opts_max_frame_size)
                }
            )},
        {"fail_if_no_subprotocol",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_fail_if_no_subprotocol)
                }
            )},
        {"supported_subprotocols",
            sc(
                comma_separated_list(),
                #{
                    default => <<"mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5">>,
                    desc => ?DESC(fields_ws_opts_supported_subprotocols)
                }
            )},
        {"check_origin_enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_ws_opts_check_origin_enable)
                }
            )},
        {"allow_origin_absence",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_allow_origin_absence)
                }
            )},
        {"check_origins",
            sc(
                comma_separated_binary(),
                #{
                    default => <<"http://localhost:18083, http://127.0.0.1:18083">>,
                    desc => ?DESC(fields_ws_opts_check_origins)
                }
            )},
        {"proxy_address_header",
            sc(
                string(),
                #{
                    default => <<"x-forwarded-for">>,
                    desc => ?DESC(fields_ws_opts_proxy_address_header)
                }
            )},
        {"proxy_port_header",
            sc(
                string(),
                #{
                    default => <<"x-forwarded-port">>,
                    desc => ?DESC(fields_ws_opts_proxy_port_header)
                }
            )},
        {"deflate_opts",
            sc(
                ref("deflate_opts"),
                #{}
            )},
        {"validate_utf8",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_ws_opts_validate_utf8)
                }
            )}
    ];
fields("tcp_opts") ->
    [
        {"active_n",
            sc(
                non_neg_integer(),
                #{
                    default => 100,
                    desc => ?DESC(fields_tcp_opts_active_n)
                }
            )},
        {"backlog",
            sc(
                pos_integer(),
                #{
                    default => 1024,
                    desc => ?DESC(fields_tcp_opts_backlog)
                }
            )},
        {"send_timeout",
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(fields_tcp_opts_send_timeout)
                }
            )},
        {"send_timeout_close",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_send_timeout_close)
                }
            )},
        {"recbuf",
            sc(
                bytesize(),
                #{
                    example => <<"2KB">>,
                    desc => ?DESC(fields_tcp_opts_recbuf)
                }
            )},
        {"sndbuf",
            sc(
                bytesize(),
                #{
                    example => <<"4KB">>,
                    desc => ?DESC(fields_tcp_opts_sndbuf)
                }
            )},
        {"buffer",
            sc(
                bytesize(),
                #{
                    default => <<"4KB">>,
                    example => <<"4KB">>,
                    desc => ?DESC(fields_tcp_opts_buffer)
                }
            )},
        {"high_watermark",
            sc(
                bytesize(),
                #{
                    default => <<"1MB">>,
                    desc => ?DESC(fields_tcp_opts_high_watermark)
                }
            )},
        {"nodelay",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_nodelay)
                }
            )},
        {"nolinger",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(fields_tcp_opts_nolinger)
                }
            )},
        {"reuseaddr",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(fields_tcp_opts_reuseaddr)
                }
            )},
        {"keepalive",
            sc(
                string(),
                #{
                    default => <<"none">>,
                    desc => ?DESC(fields_tcp_opts_keepalive),
                    validator => fun validate_tcp_keepalive/1
                }
            )}
    ];
fields("listener_ssl_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        false
    );
fields("listener_wss_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        true
    );
fields("listener_quic_ssl_opts") ->
    %% Mark unsupported TLS options deprecated.
    Schema0 = server_ssl_opts_schema(#{}, false),
    Schema1 = lists:keydelete("ocsp", 1, Schema0),
    lists:map(
        fun({Name, Schema}) ->
            case is_quic_ssl_opts(Name) of
                true ->
                    {Name, Schema};
                false ->
                    {Name, Schema#{
                        deprecated => {since, "5.0.20"}, importance => ?IMPORTANCE_HIDDEN
                    }}
            end
        end,
        Schema1
    );
fields("ssl_client_opts") ->
    client_ssl_opts_schema(#{});
fields("ocsp") ->
    [
        {enable_ocsp_stapling,
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC("server_ssl_opts_schema_enable_ocsp_stapling")
                }
            )},
        {responder_url,
            sc(
                url(),
                #{
                    required => false,
                    desc => ?DESC("server_ssl_opts_schema_ocsp_responder_url")
                }
            )},
        {issuer_pem,
            sc(
                binary(),
                #{
                    required => false,
                    desc => ?DESC("server_ssl_opts_schema_ocsp_issuer_pem")
                }
            )},
        {refresh_interval,
            sc(
                duration(),
                #{
                    default => <<"5m">>,
                    desc => ?DESC("server_ssl_opts_schema_ocsp_refresh_interval")
                }
            )},
        {refresh_http_timeout,
            sc(
                duration(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("server_ssl_opts_schema_ocsp_refresh_http_timeout")
                }
            )}
    ];
fields("deflate_opts") ->
    [
        {"level",
            sc(
                hoconsc:enum([none, default, best_compression, best_speed]),
                #{desc => ?DESC(fields_deflate_opts_level)}
            )},
        {"mem_level",
            sc(
                range(1, 9),
                #{
                    default => 8,
                    desc => ?DESC(fields_deflate_opts_mem_level)
                }
            )},
        {"strategy",
            sc(
                hoconsc:enum([default, filtered, huffman_only, rle]),
                #{
                    default => default,
                    desc => ?DESC(fields_deflate_opts_strategy)
                }
            )},
        {"server_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    default => takeover,
                    desc => ?DESC(fields_deflate_opts_server_context_takeover)
                }
            )},
        {"client_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    default => takeover,
                    desc => ?DESC(fields_deflate_opts_client_context_takeover)
                }
            )},
        {"server_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => ?DESC(fields_deflate_opts_server_max_window_bits)
                }
            )},
        {"client_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => ?DESC(fields_deflate_opts_client_max_window_bits)
                }
            )}
    ];
fields("broker") ->
    [
        {enable_session_registry,
            sc(
                boolean(),
                #{
                    default => true,
                    importance => ?IMPORTANCE_HIGH,
                    desc => ?DESC(broker_enable_session_registry)
                }
            )},
        {session_history_retain,
            sc(
                duration_s(),
                #{
                    default => <<"0s">>,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC("broker_session_history_retain")
                }
            )},
        {session_locking_strategy,
            sc(
                hoconsc:enum([local, leader, quorum, all]),
                #{
                    default => quorum,
                    importance => ?IMPORTANCE_HIDDEN,
                    desc => ?DESC(broker_session_locking_strategy)
                }
            )},
        %% moved to under mqtt root
        {shared_subscription_strategy,
            sc(
                string(),
                #{
                    deprecated => {since, "5.1.0"},
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {shared_dispatch_ack_enabled,
            sc(
                boolean(),
                #{
                    deprecated => {since, "5.1.0"},
                    importance => ?IMPORTANCE_HIDDEN,
                    default => false,
                    desc => ?DESC(broker_shared_dispatch_ack_enabled)
                }
            )},
        {route_batch_clean,
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "This config is stale since 4.3",
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {perf,
            sc(
                ref("broker_perf"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {routing,
            sc(
                ref("broker_routing"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        %% FIXME: Need new design for shared subscription group
        {shared_subscription_group,
            sc(
                map(name, ref("shared_subscription_group")),
                #{
                    example => #{<<"example_group">> => #{<<"strategy">> => <<"random">>}},
                    desc => ?DESC(shared_subscription_group_strategy),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields("broker_routing") ->
    [
        {"storage_schema",
            sc(
                hoconsc:enum([v1, v2]),
                #{
                    default => v2,
                    'readOnly' => true,
                    desc => ?DESC(broker_routing_storage_schema)
                }
            )},
        {"batch_sync",
            sc(
                ref("broker_routing_batch_sync"),
                #{importance => ?IMPORTANCE_HIDDEN}
            )}
    ];
fields("broker_routing_batch_sync") ->
    [
        {"enable_on",
            sc(
                hoconsc:enum([none, core, replicant, all]),
                #{
                    %% TODO
                    %% Make `replicant` the default value after initial release.
                    default => none,
                    desc => ?DESC(broker_routing_batch_sync_enable_on)
                }
            )}
    ];
fields("shared_subscription_group") ->
    [
        {"strategy",
            sc(
                hoconsc:enum([
                    random,
                    round_robin,
                    round_robin_per_group,
                    sticky,
                    local,
                    hash_topic,
                    hash_clientid
                ]),
                #{
                    default => random,
                    desc => ?DESC(shared_subscription_strategy_enum)
                }
            )},
        {"initial_sticky_pick",
            sc(
                hoconsc:enum([
                    random,
                    local,
                    hash_topic,
                    hash_clientid
                ]),
                #{
                    default => random,
                    desc => ?DESC(shared_subscription_initial_sticky_pick_enum)
                }
            )}
    ];
fields("broker_perf") ->
    [
        {"route_lock_type",
            sc(
                hoconsc:enum([key, tab, global]),
                #{
                    default => key,
                    desc => ?DESC(broker_perf_route_lock_type)
                }
            )},
        {"trie_compaction",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(broker_perf_trie_compaction)
                }
            )}
    ];
fields("sys_topics") ->
    [
        {"sys_msg_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => <<"1m">>,
                    desc => ?DESC(sys_msg_interval)
                }
            )},
        {"sys_heartbeat_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(sys_heartbeat_interval)
                }
            )},
        {"sys_event_messages",
            sc(
                ref("event_names"),
                #{desc => ?DESC(sys_event_messages)}
            )}
    ];
fields("event_names") ->
    [
        {"client_connected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sys_event_client_connected)
                }
            )},
        {"client_disconnected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sys_event_client_disconnected)
                }
            )},
        {"client_subscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(sys_event_client_subscribed)
                }
            )},
        {"client_unsubscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(sys_event_client_unsubscribed)
                }
            )}
    ];
fields("sysmon") ->
    [
        {"vm",
            sc(
                ref("sysmon_vm"),
                #{}
            )},
        {"os",
            sc(
                ref("sysmon_os"),
                #{}
            )},
        {"top",
            sc(
                ref("sysmon_top"),
                %% Userful monitoring solution when benchmarking,
                %% but hardly common enough for regular users.
                #{importance => ?IMPORTANCE_HIDDEN}
            )},
        {"mnesia_tm_mailbox_size_alarm_threshold",
            sc(
                pos_integer(),
                #{
                    default => 500,
                    desc => ?DESC("sysmon_mnesia_tm_mailbox_size_alarm_threshold")
                }
            )},
        {"broker_pool_mailbox_size_alarm_threshold",
            sc(
                pos_integer(),
                #{
                    default => 500,
                    desc => ?DESC("sysmon_broker_pool_mailbox_size_alarm_threshold")
                }
            )}
    ];
fields("sysmon_vm") ->
    [
        {"process_check_interval",
            sc(
                duration(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(sysmon_vm_process_check_interval)
                }
            )},
        {"process_high_watermark",
            sc(
                percent(),
                #{
                    default => <<"80%">>,
                    desc => ?DESC(sysmon_vm_process_high_watermark)
                }
            )},
        {"process_low_watermark",
            sc(
                percent(),
                #{
                    default => <<"60%">>,
                    desc => ?DESC(sysmon_vm_process_low_watermark)
                }
            )},
        {"long_gc",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => disabled,
                    desc => ?DESC(sysmon_vm_long_gc)
                }
            )},
        {"long_schedule",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => <<"240ms">>,
                    desc => ?DESC(sysmon_vm_long_schedule)
                }
            )},
        {"large_heap",
            sc(
                hoconsc:union([disabled, bytesize()]),
                #{
                    default => <<"32MB">>,
                    desc => ?DESC(sysmon_vm_large_heap)
                }
            )},
        {"busy_dist_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sysmon_vm_busy_dist_port)
                }
            )},
        {"busy_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(sysmon_vm_busy_port)
                }
            )}
    ];
fields("sysmon_os") ->
    [
        {"cpu_check_interval",
            sc(
                duration(),
                #{
                    default => <<"60s">>,
                    desc => ?DESC(sysmon_os_cpu_check_interval)
                }
            )},
        {"cpu_high_watermark",
            sc(
                percent(),
                #{
                    default => <<"80%">>,
                    desc => ?DESC(sysmon_os_cpu_high_watermark)
                }
            )},
        {"cpu_low_watermark",
            sc(
                percent(),
                #{
                    default => <<"60%">>,
                    desc => ?DESC(sysmon_os_cpu_low_watermark)
                }
            )},
        {"mem_check_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => default_mem_check_interval(),
                    desc => ?DESC(sysmon_os_mem_check_interval)
                }
            )},
        {"sysmem_high_watermark",
            sc(
                percent(),
                #{
                    default => <<"70%">>,
                    desc => ?DESC(sysmon_os_sysmem_high_watermark)
                }
            )},
        {"procmem_high_watermark",
            sc(
                percent(),
                #{
                    default => <<"5%">>,
                    desc => ?DESC(sysmon_os_procmem_high_watermark)
                }
            )}
    ];
fields("sysmon_top") ->
    [
        {"num_items",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_num_items",
                    default => 10,
                    desc => ?DESC(sysmon_top_num_items)
                }
            )},
        {"sample_interval",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "system_monitor.top_sample_interval",
                    default => <<"2s">>,
                    desc => ?DESC(sysmon_top_sample_interval)
                }
            )},
        {"max_procs",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_max_procs",
                    default => 1_000_000,
                    desc => ?DESC(sysmon_top_max_procs)
                }
            )},
        {"db_hostname",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_hostname",
                    desc => ?DESC(sysmon_top_db_hostname),
                    default => <<>>
                }
            )},
        {"db_port",
            sc(
                integer(),
                #{
                    mapping => "system_monitor.db_port",
                    default => 5432,
                    desc => ?DESC(sysmon_top_db_port)
                }
            )},
        {"db_username",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_username",
                    default => <<"system_monitor">>,
                    desc => ?DESC(sysmon_top_db_username)
                }
            )},
        {"db_password",
            sc(
                binary(),
                #{
                    mapping => "system_monitor.db_password",
                    default => <<"system_monitor_password">>,
                    desc => ?DESC(sysmon_top_db_password),
                    converter => fun password_converter/2,
                    sensitive => true
                }
            )},
        {"db_name",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_name",
                    default => <<"postgres">>,
                    desc => ?DESC(sysmon_top_db_name)
                }
            )}
    ];
fields("alarm") ->
    [
        {"actions",
            sc(
                hoconsc:array(hoconsc:enum([log, publish])),
                #{
                    default => [log, publish],
                    example => [log, publish],
                    desc => ?DESC(alarm_actions)
                }
            )},
        {"size_limit",
            sc(
                range(1, 3000),
                #{
                    default => 1000,
                    example => 1000,
                    desc => ?DESC(alarm_size_limit)
                }
            )},
        {"validity_period",
            sc(
                duration(),
                #{
                    default => <<"24h">>,
                    example => "24h",
                    desc => ?DESC(alarm_validity_period)
                }
            )}
    ];
fields("trace") ->
    [
        {"payload_encode",
            sc(hoconsc:enum([hex, text, hidden]), #{
                default => text,
                deprecated => {since, "5.0.22"},
                importance => ?IMPORTANCE_HIDDEN,
                desc => ?DESC(fields_trace_payload_encode)
            })}
    ];
fields("durable_sessions") ->
    [
        {"enable",
            sc(
                boolean(), #{
                    desc => ?DESC(durable_sessions_enable),
                    %% importance => ?IMPORTANCE_NO_DOC,
                    default => false
                }
            )},
        {"batch_size",
            sc(
                pos_integer(),
                #{
                    default => 100,
                    desc => ?DESC(session_ds_batch_size),
                    importance => ?IMPORTANCE_MEDIUM,
                    %% Note: the same value is used for both sync
                    %% `next' request and async polls. Since poll
                    %% workers are global for the DS DB, this value is
                    %% global and it cannot be overridden per
                    %% listener:
                    mapping => "emqx_durable_session.poll_batch_size"
                }
            )},
        {"idle_poll_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"10s">>,
                    desc => ?DESC(session_ds_idle_poll_interval)
                }
            )},
        {"heartbeat_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"5000ms">>,
                    desc => ?DESC(session_ds_heartbeat_interval)
                }
            )},
        {"renew_streams_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"1s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"session_gc_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"10m">>,
                    desc => ?DESC(session_ds_session_gc_interval)
                }
            )},
        {"session_gc_batch_size",
            sc(
                pos_integer(),
                #{
                    default => 100,
                    importance => ?IMPORTANCE_LOW,
                    desc => ?DESC(session_ds_session_gc_batch_size)
                }
            )},
        {"subscription_count_refresh_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"5s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"disconnected_session_count_refresh_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"5s">>,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"message_retention_period",
            sc(
                timeout_duration(),
                #{
                    default => <<"1d">>,
                    desc => ?DESC(session_ds_message_retention_period)
                }
            )},
        {"force_persistence",
            sc(
                boolean(),
                #{
                    default => false,
                    %% Only for testing, shall remain hidden
                    importance => ?IMPORTANCE_HIDDEN
                }
            )}
    ];
fields(durable_storage) ->
    emqx_ds_schema:schema();
fields("client_attrs_init") ->
    [
        {expression,
            sc(
                typerefl:alias("string", any()),
                #{
                    desc => ?DESC("client_attrs_init_expression"),
                    converter => fun compile_variform/2
                }
            )},
        {set_as_attr,
            sc(binary(), #{
                desc => ?DESC("client_attrs_init_set_as_attr"),
                validator => fun restricted_string/1
            })}
    ];
fields("banned") ->
    [
        {bootstrap_file,
            sc(
                binary(),
                #{
                    desc => ?DESC("banned_bootstrap_file"),
                    require => false
                }
            )}
    ].

compile_variform_allow_disabled(disabled, _Opts) ->
    disabled;
compile_variform_allow_disabled(<<"disabled">>, _Opts) ->
    disabled;
compile_variform_allow_disabled(Expression, Opts) ->
    compile_variform(Expression, Opts).

compile_variform(undefined, _Opts) ->
    undefined;
compile_variform(Expression, #{make_serializable := true}) ->
    case is_binary(Expression) of
        true ->
            Expression;
        false ->
            emqx_variform:decompile(Expression)
    end;
compile_variform(Expression, _Opts) ->
    case emqx_variform:compile(Expression) of
        {ok, Compiled} ->
            Compiled;
        {error, Reason} ->
            throw(#{expression => Expression, reason => Reason})
    end.

restricted_string(Str) ->
    case emqx_utils:is_restricted_str(Str) of
        true -> ok;
        false -> {error, <<"Invalid string for attribute name">>}
    end.

mqtt_listener(Bind) ->
    base_listener(Bind) ++
        [
            {"access_rules",
                sc(
                    hoconsc:array(string()),
                    #{
                        desc => ?DESC(mqtt_listener_access_rules),
                        default => [<<"allow all">>],
                        converter => fun access_rules_converter/1,
                        validator => fun access_rules_validator/1
                    }
                )},
            {"proxy_protocol",
                sc(
                    boolean(),
                    #{
                        desc => ?DESC(mqtt_listener_proxy_protocol),
                        default => false
                    }
                )},
            {"proxy_protocol_timeout",
                sc(
                    duration(),
                    #{
                        desc => ?DESC(mqtt_listener_proxy_protocol_timeout),
                        default => <<"3s">>
                    }
                )}
        ] ++ emqx_schema_hooks:list_injection_point('mqtt.listener').

access_rules_converter(AccessRules) ->
    DeepRules =
        lists:foldr(
            fun(Rule, Acc) ->
                Rules0 = re:split(Rule, <<"\\s*,\\s*">>, [{return, binary}]),
                Rules1 = [string:trim(R) || R <- Rules0],
                [Rules1 | Acc]
            end,
            [],
            AccessRules
        ),
    [unicode:characters_to_list(RuleBin) || RuleBin <- lists:flatten(DeepRules)].

access_rules_validator(AccessRules) ->
    InvalidRules = [Rule || Rule <- AccessRules, is_invalid_rule(Rule)],
    case InvalidRules of
        [] ->
            ok;
        _ ->
            MsgStr = io_lib:format("invalid_rule(s): ~ts", [string:join(InvalidRules, ", ")]),
            MsgBin = unicode:characters_to_binary(MsgStr),
            {error, MsgBin}
    end.

is_invalid_rule(S) ->
    try
        [Action, CIDR] = string:tokens(S, " "),
        case Action of
            "allow" -> ok;
            "deny" -> ok
        end,
        case CIDR of
            "all" ->
                ok;
            _ ->
                %% should not crash
                _ = esockd_cidr:parse(CIDR, true),
                ok
        end,
        false
    catch
        _:_ -> true
    end.

base_listener(Bind) ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    aliases => [enabled],
                    importance => ?IMPORTANCE_NO_DOC,
                    desc => ?DESC(fields_listener_enabled)
                }
            )},
        {"bind",
            sc(
                ip_port(),
                #{
                    default => Bind,
                    required => true,
                    desc => ?DESC(base_listener_bind)
                }
            )},
        {"acceptors",
            sc(
                pos_integer(),
                #{
                    default => 16,
                    desc => ?DESC(base_listener_acceptors)
                }
            )},
        {"max_connections",
            sc(
                hoconsc:union([infinity, pos_integer()]),
                #{
                    default => emqx_listeners:default_max_conn(),
                    desc => ?DESC(base_listener_max_connections)
                }
            )},
        {"mountpoint",
            sc(
                binary(),
                #{
                    default => <<>>,
                    desc => ?DESC(base_listener_mountpoint)
                }
            )},
        {"zone",
            sc(
                atom(),
                #{
                    desc => ?DESC(base_listener_zone),
                    default => 'default',
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"limiter",
            sc(
                ?R_REF(
                    emqx_limiter_schema,
                    listener_fields
                ),
                #{
                    desc => ?DESC(base_listener_limiter),
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"enable_authn",
            sc(
                hoconsc:enum([true, false, quick_deny_anonymous]),
                #{
                    desc => ?DESC(base_listener_enable_authn),
                    default => true
                }
            )}
    ] ++ emqx_limiter_schema:short_paths_fields().

%% @hidden Starting from 5.7, listeners.{TYPE}.{NAME}.zone is no longer hidden
%% However, the root key 'zones' is still hidden because the fields' schema
%% just repeat other root field's schema, which makes the dumped schema doc
%% unnecessarily bloated.
%%
%% zone schema is documented here since 5.7:
%% https://docs.emqx.com/en/enterprise/latest/configuration/configuration.html
zones_field_schema() ->
    sc(
        map(name, ref("zone")),
        #{
            desc => ?DESC(zones),
            importance => ?IMPORTANCE_HIDDEN
        }
    ).

desc("persistent_table_mria_opts") ->
    "Tuning options for the mria table.";
desc("stats") ->
    "Enable/disable statistic data collection.\n"
    "Statistic data such as message receive/send count/rate etc. "
    "It provides insights of system performance and helps to diagnose issues. "
    "You can find statistic data from the dashboard, or from the '/stats' API.";
desc("authorization") ->
    "Settings for client authorization.";
desc("mqtt") ->
    "Global MQTT configuration.";
desc("authz_cache") ->
    "Settings for the authorization cache.";
desc("zone") ->
    "A `Zone` defines a set of configuration items (such as the maximum number of connections)"
    " that can be shared between multiple listeners.\n\n"
    "`Listener` can refer to a `Zone` through the configuration item"
    " <code>listener.\\<Protocol>.\\<Listener Name>.zone</code>.\n\n"
    "The configs defined in the zones will override the global configs with the same key.\n\n"
    "For example, given the following config:\n"
    "```\n"
    "a {\n"
    "    b: 1, c: 1\n"
    "}\n"
    "zone.my_zone {\n"
    "  a {\n"
    "    b:2\n"
    "  }\n"
    "}\n"
    "```\n\n"
    "The global config `a` is overridden by the configs `a` inside the zone `my_zone`.\n\n"
    "If there is a listener using the zone `my_zone`, the value of config `a` will be: "
    "`{b:2, c: 1}`.\n"
    "Note that although the default value of `a.c` is `0`, the global value is used,"
    " i.e. configs in the zone have no default values. To override `a.c` one must configure"
    " it explicitly in the zone.\n\n"
    "All the global configs that can be overridden in zones are:\n"
    " - `stats.*`\n"
    " - `mqtt.*`\n"
    " - `authorization.*`\n"
    " - `flapping_detect.*`\n"
    " - `force_shutdown.*`\n"
    " - `conn_congestion.*`\n"
    " - `force_gc.*`\n\n";
desc("flapping_detect") ->
    "This config controls the allowed maximum number of `CONNECT` packets received\n"
    "from the same clientid in a time frame defined by `window_time`.\n"
    "After the limit is reached, successive `CONNECT` requests are forbidden\n"
    "(banned) until the end of the time period defined by `ban_time`.";
desc("force_shutdown") ->
    "When the process message queue length, or the memory bytes\n"
    "reaches a certain value, the process is forced to close.\n\n"
    "Note: \"message queue\" here refers to the \"message mailbox\"\n"
    "of the Erlang process, not the `mqueue` of QoS 1 and QoS 2.";
desc("overload_protection") ->
    "Overload protection mechanism monitors the load of the system and temporarily\n"
    "disables some features (such as accepting new connections) when the load is high.";
desc("conn_congestion") ->
    "Settings for `conn_congestion` alarm.\n\n"
    "Sometimes the MQTT connection (usually an MQTT subscriber) may\n"
    "get \"congested\", because there are too many packets to be sent.\n"
    "The socket tries to buffer the packets until the buffer is\n"
    "full. If more packets arrive after that, the packets will be\n"
    "\"pending\" in the queue, and we consider the connection\n"
    "congested.\n\n"
    "Note: `sndbuf` can be set to larger value if the\n"
    "alarm is triggered too often.\n"
    "The name of the alarm is of format `conn_congestion/<ClientID>/<Username>`,\n"
    "where the `<ClientID>` is the client ID of the congested MQTT connection,\n"
    "and `<Username>` is the username or `unknown_user`.";
desc("force_gc") ->
    "Force garbage collection in MQTT connection process after\n"
    " they process certain number of messages or bytes of data.";
desc("listeners") ->
    "MQTT listeners identified by their protocol type and assigned names";
desc("mqtt_tcp_listener") ->
    "Settings for the MQTT over TCP listener.";
desc("mqtt_ssl_listener") ->
    "Settings for the MQTT over SSL listener.";
desc("mqtt_ws_listener") ->
    "Settings for the MQTT over WebSocket listener.";
desc("mqtt_wss_listener") ->
    "Settings for the MQTT over WebSocket/SSL listener.";
desc("mqtt_quic_listener") ->
    "Settings for the MQTT over QUIC listener.";
desc("ws_opts") ->
    "WebSocket listener options.";
desc("tcp_opts") ->
    "TCP listener options.";
desc("listener_ssl_opts") ->
    "Socket options for SSL connections.";
desc("listener_wss_opts") ->
    "Socket options for WebSocket/SSL connections.";
desc("fields_mqtt_quic_listener_certfile") ->
    "Path to the certificate file. Will be deprecated in 5.1, use '.ssl_options.certfile' instead.";
desc("fields_mqtt_quic_listener_keyfile") ->
    "Path to the secret key file. Will be deprecated in 5.1, use '.ssl_options.keyfile' instead.";
desc("listener_quic_ssl_opts") ->
    "TLS options for QUIC transport.";
desc("ssl_client_opts") ->
    "Socket options for SSL clients.";
desc("deflate_opts") ->
    "Compression options.";
desc("broker") ->
    "Message broker options.";
desc("broker_perf") ->
    "Broker performance tuning parameters.";
desc("sys_topics") ->
    "The EMQX Broker periodically publishes its own status, message statistics,\n"
    "client online and offline events to the system topic starting with `$SYS/`.\n\n"
    "The following options control the behavior of `$SYS` topics.";
desc("event_names") ->
    "Enable or disable client lifecycle event publishing.\n\n"
    "The following options affect MQTT clients as well as\n"
    "gateway clients. The types of the clients\n"
    "are distinguished by the topic prefix:\n\n"
    "- For the MQTT clients, the format is:\n"
    "`$SYS/broker/<node>/clients/<clientid>/<event>`\n"
    "- For the Gateway clients, it is\n"
    "`$SYS/broker/<node>/gateway/<gateway-name>/clients/<clientid>/<event>`\n";
desc("sysmon") ->
    "Features related to system monitoring and introspection.";
desc("sysmon_vm") ->
    "This part of the configuration is responsible for collecting\n"
    " BEAM VM events, such as long garbage collection, traffic congestion in the inter-broker\n"
    " communication, etc.";
desc("sysmon_os") ->
    "This part of the configuration is responsible for monitoring\n"
    " the host OS health, such as free memory, disk space, CPU load, etc.";
desc("sysmon_top") ->
    "This part of the configuration is responsible for monitoring\n"
    " the Erlang processes in the VM. This information can be sent to an external\n"
    " PostgreSQL database. This feature is inactive unless the PostgreSQL sink is configured.";
desc("alarm") ->
    "Settings for the alarms.";
desc("trace") ->
    "Real-time filtering logs for the ClientID or Topic or IP for debugging.";
desc("shared_subscription_group") ->
    "Per group dispatch strategy for shared subscription";
desc("ocsp") ->
    "Per listener OCSP Stapling configuration.";
desc("crl_cache") ->
    "Global CRL cache options.";
desc("durable_sessions") ->
    ?DESC(durable_sessions);
desc(durable_storage) ->
    ?DESC(durable_storage);
desc("client_attrs_init") ->
    ?DESC(client_attrs_init);
desc("banned") ->
    "Banned .";
desc(_) ->
    undefined.

%% utils
-spec conf_get(string() | [string()], hocon:config()) -> term().
conf_get(Key, Conf) ->
    ensure_list(hocon_maps:get(Key, Conf)).

conf_get(Key, Conf, Default) ->
    ensure_list(hocon_maps:get(Key, Conf, Default)).

ensure_list(V) ->
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private This function defines the SSL opts which are commonly used by
%% SSL listener and client.
-spec common_ssl_opts_schema(map(), server | client) -> hocon_schema:field_schema().
common_ssl_opts_schema(Defaults, Type) ->
    D = fun(Field) -> maps:get(Field, Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(Field, Defaults, Default) end,
    Collection = maps:get(versions, Defaults, tls_all_available),
    [
        {"cacertfile",
            sc(
                binary(),
                #{
                    default => cert_file("cacert.pem", Type),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_cacertfile)
                }
            )},
        {"cacerts",
            sc(
                boolean(),
                #{
                    default => false,
                    deprecated => {since, "5.1.4"}
                }
            )},
        {"certfile",
            sc(
                binary(),
                #{
                    default => cert_file("cert.pem", Type),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_certfile)
                }
            )},
        {"keyfile",
            sc(
                binary(),
                #{
                    default => cert_file("key.pem", Type),
                    required => false,
                    desc => ?DESC(common_ssl_opts_schema_keyfile)
                }
            )},
        {"verify",
            sc(
                hoconsc:enum([verify_peer, verify_none]),
                #{
                    default => Df(verify, verify_none),
                    desc => ?DESC(common_ssl_opts_schema_verify)
                }
            )},
        {"reuse_sessions",
            sc(
                boolean(),
                #{
                    default => Df(reuse_sessions, true),
                    desc => ?DESC(common_ssl_opts_schema_reuse_sessions)
                }
            )},
        {"depth",
            sc(
                non_neg_integer(),
                #{
                    default => Df(depth, 10),
                    desc => ?DESC(common_ssl_opts_schema_depth)
                }
            )},
        {"password",
            emqx_schema_secret:mk(
                #{
                    sensitive => true,
                    required => false,
                    example => <<"">>,
                    format => <<"password">>,
                    desc => ?DESC(common_ssl_opts_schema_password),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"versions", tls_versions_schema(Collection)},
        {"ciphers", ciphers_schema(D(ciphers))},
        {"secure_renegotiate",
            sc(
                boolean(),
                #{
                    default => Df(secure_renegotiate, true),
                    desc => ?DESC(common_ssl_opts_schema_secure_renegotiate)
                }
            )},
        {"log_level",
            sc(
                hoconsc:enum([
                    emergency, alert, critical, error, warning, notice, info, debug, none, all
                ]),
                #{
                    default => notice,
                    desc => ?DESC(common_ssl_opts_schema_log_level),
                    importance => ?IMPORTANCE_LOW
                }
            )},

        {"hibernate_after",
            sc(
                duration(),
                #{
                    default => Df(hibernate_after, <<"5s">>),
                    desc => ?DESC(common_ssl_opts_schema_hibernate_after)
                }
            )}
    ] ++ emqx_schema_hooks:list_injection_point('common_ssl_opts_schema').

%% @doc Make schema for SSL listener options.
-spec server_ssl_opts_schema(map(), boolean()) -> hocon_schema:field_schema().
server_ssl_opts_schema(Defaults, IsRanchListener) ->
    D = fun(Field) -> maps:get(Field, Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(Field, Defaults, Default) end,
    common_ssl_opts_schema(Defaults, server) ++
        [
            {"dhfile",
                sc(
                    string(),
                    #{
                        default => D(dhfile),
                        required => false,
                        desc => ?DESC(server_ssl_opts_schema_dhfile)
                    }
                )},
            {"fail_if_no_peer_cert",
                sc(
                    boolean(),
                    #{
                        default => Df(fail_if_no_peer_cert, false),
                        desc => ?DESC(server_ssl_opts_schema_fail_if_no_peer_cert)
                    }
                )},
            {"honor_cipher_order",
                sc(
                    boolean(),
                    #{
                        default => Df(honor_cipher_order, true),
                        desc => ?DESC(server_ssl_opts_schema_honor_cipher_order)
                    }
                )},
            {"client_renegotiation",
                sc(
                    boolean(),
                    #{
                        default => Df(client_renegotiation, true),
                        desc => ?DESC(server_ssl_opts_schema_client_renegotiation)
                    }
                )},
            {"handshake_timeout",
                sc(
                    duration(),
                    #{
                        default => Df(handshake_timeout, <<"15s">>),
                        desc => ?DESC(server_ssl_opts_schema_handshake_timeout)
                    }
                )},
            {"user_lookup_fun",
                sc(
                    typerefl:alias("string", any()),
                    #{
                        default => <<"emqx_tls_psk:lookup">>,
                        converter => fun ?MODULE:user_lookup_fun_tr/2,
                        importance => ?IMPORTANCE_HIDDEN,
                        desc => ?DESC(common_ssl_opts_schema_user_lookup_fun)
                    }
                )}
        ] ++
        [
            Field
         || not IsRanchListener,
            Field <- [
                {gc_after_handshake,
                    sc(boolean(), #{
                        default => false,
                        desc => ?DESC(server_ssl_opts_schema_gc_after_handshake)
                    })},
                {ocsp,
                    sc(
                        ref("ocsp"),
                        #{
                            required => false,
                            validator => fun ocsp_inner_validator/1
                        }
                    )},
                {enable_crl_check,
                    sc(
                        boolean(),
                        #{
                            default => false,
                            importance => ?IMPORTANCE_MEDIUM,
                            desc => ?DESC("server_ssl_opts_schema_enable_crl_check")
                        }
                    )}
            ]
        ].

validate_server_ssl_opts(#{<<"fail_if_no_peer_cert">> := true, <<"verify">> := Verify}) ->
    validate_verify(Verify);
validate_server_ssl_opts(#{fail_if_no_peer_cert := true, verify := Verify}) ->
    validate_verify(Verify);
validate_server_ssl_opts(_SSLOpts) ->
    ok.

validate_verify(verify_peer) ->
    ok;
validate_verify(_) ->
    {error, "verify must be verify_peer when fail_if_no_peer_cert is true"}.

mqtt_ssl_listener_ssl_options_validator(Conf) ->
    Checks = [
        fun validate_server_ssl_opts/1,
        fun ocsp_outer_validator/1,
        fun crl_outer_validator/1
    ],
    case emqx_utils:pipeline(Checks, Conf, not_used) of
        {ok, _, _} ->
            ok;
        {error, Reason, _NotUsed} ->
            {error, Reason}
    end.

ocsp_outer_validator(#{<<"ocsp">> := #{<<"enable_ocsp_stapling">> := true}} = Conf) ->
    %% outer mqtt listener ssl server config
    ServerCertPemPath = maps:get(<<"certfile">>, Conf, undefined),
    case ServerCertPemPath of
        undefined ->
            {error, "Server certificate must be defined when using OCSP stapling"};
        _ ->
            %% check if issuer pem is readable and/or valid?
            ok
    end;
ocsp_outer_validator(_Conf) ->
    ok.

ocsp_inner_validator(#{enable_ocsp_stapling := _} = Conf) ->
    ocsp_inner_validator(emqx_utils_maps:binary_key_map(Conf));
ocsp_inner_validator(#{<<"enable_ocsp_stapling">> := false} = _Conf) ->
    ok;
ocsp_inner_validator(#{<<"enable_ocsp_stapling">> := true} = Conf) ->
    assert_required_field(
        Conf, <<"responder_url">>, "The responder URL is required for OCSP stapling"
    ),
    assert_required_field(
        Conf, <<"issuer_pem">>, "The issuer PEM path is required for OCSP stapling"
    ),
    ok.

crl_outer_validator(
    #{<<"enable_crl_check">> := true} = SSLOpts
) ->
    case maps:get(<<"verify">>, SSLOpts) of
        verify_peer ->
            ok;
        _ ->
            {error, "verify must be verify_peer when CRL check is enabled"}
    end;
crl_outer_validator(_SSLOpts) ->
    ok.

%% @doc Make schema for SSL client.
-spec client_ssl_opts_schema(map()) -> hocon_schema:field_schema().
client_ssl_opts_schema(Defaults) ->
    common_ssl_opts_schema(Defaults, client) ++
        [
            {"enable",
                sc(
                    boolean(),
                    #{
                        default => false,
                        %% importance => ?IMPORTANCE_NO_DOC,
                        desc => ?DESC(client_ssl_opts_schema_enable)
                    }
                )},
            {"server_name_indication",
                sc(
                    hoconsc:union([disable, string()]),
                    #{
                        required => false,
                        example => disable,
                        validator => fun emqx_schema:non_empty_string/1,
                        desc => ?DESC(client_ssl_opts_schema_server_name_indication)
                    }
                )},
            {"user_lookup_fun",
                sc(
                    string(),
                    #{
                        deprecated => {since, "5.8.1"},
                        importance => ?IMPORTANCE_HIDDEN
                    }
                )}
        ].

available_tls_vsns(dtls_all_available) -> emqx_tls_lib:available_versions(dtls);
available_tls_vsns(tls_all_available) -> emqx_tls_lib:available_versions(tls).

outdated_tls_vsn(dtls_all_available) -> [dtlsv1];
outdated_tls_vsn(tls_all_available) -> ['tlsv1.1', tlsv1].

default_tls_vsns(Key) ->
    available_tls_vsns(Key) -- outdated_tls_vsn(Key).

-spec tls_versions_schema(tls_all_available | dtls_all_available) -> hocon_schema:field_schema().
tls_versions_schema(Collection) ->
    DefaultVersions = default_tls_vsns(Collection),
    sc(
        hoconsc:array(typerefl:atom()),
        #{
            default => DefaultVersions,
            desc => ?DESC(common_ssl_opts_schema_versions),
            importance => ?IMPORTANCE_HIGH,
            validator => fun(Input) -> validate_tls_versions(Collection, Input) end
        }
    ).

-spec ciphers_schema(quic | dtls_all_available | tls_all_available | undefined) ->
    hocon_schema:field_schema().
ciphers_schema(Default) ->
    Desc =
        case Default of
            quic -> ?DESC(ciphers_schema_quic);
            _ -> ?DESC(ciphers_schema_common)
        end,
    sc(
        hoconsc:array(string()),
        #{
            default => default_ciphers(Default),
            converter => fun converter_ciphers/2,
            validator =>
                case Default =:= quic of
                    %% quic has openssl statically linked
                    true -> undefined;
                    false -> fun validate_ciphers/1
                end,
            desc => Desc
        }
    ).

converter_ciphers(undefined, _Opts) ->
    [];
converter_ciphers(<<>>, _Opts) ->
    [];
converter_ciphers(Ciphers, _Opts) when is_list(Ciphers) -> Ciphers;
converter_ciphers(Ciphers, _Opts) when is_binary(Ciphers) ->
    {ok, List} = to_comma_separated_binary(binary_to_list(Ciphers)),
    List.

default_ciphers(Which) ->
    lists:map(
        fun unicode:characters_to_binary/1,
        do_default_ciphers(Which)
    ).

do_default_ciphers(quic) ->
    [
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",
        "TLS_CHACHA20_POLY1305_SHA256"
    ];
do_default_ciphers(_) ->
    %% otherwise resolve default ciphers list at runtime
    [].

password_converter(X, Opts) ->
    bin_str_converter(X, Opts).

bin_str_converter(undefined, _) ->
    undefined;
bin_str_converter(I, _) when is_integer(I) ->
    integer_to_binary(I);
bin_str_converter(X, _) ->
    try
        unicode:characters_to_binary(X)
    catch
        _:_ ->
            throw("must_quote")
    end.

authz_fields() ->
    [
        {"no_match",
            sc(
                hoconsc:enum([allow, deny]),
                #{
                    default => allow,
                    required => true,
                    desc => ?DESC(fields_authorization_no_match)
                }
            )},
        {"deny_action",
            sc(
                hoconsc:enum([ignore, disconnect]),
                #{
                    default => ignore,
                    required => true,
                    desc => ?DESC(fields_authorization_deny_action)
                }
            )},
        {"cache",
            sc(
                ref(?MODULE, "authz_cache"),
                #{}
            )}
    ].

%% @private return a list of keys in a parent field
-spec keys(string(), hocon:config()) -> [string()].
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

-spec ceiling(number()) -> integer().
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

ref(StructName) -> hoconsc:ref(?MODULE, StructName).

ref(Module, StructName) -> hoconsc:ref(Module, StructName).

mk_duration(Desc, OverrideMeta) ->
    DefaultMeta = #{
        desc => Desc ++
            " Time interval is a string that contains a number followed by time unit:<br/>"
            "- `ms` for milliseconds,\n"
            "- `s` for seconds,\n"
            "- `m` for minutes,\n"
            "- `h` for hours;\n<br/>"
            "or combination of whereof: `1h5m0s`"
    },
    hoconsc:mk(typerefl:alias("string", duration()), maps:merge(DefaultMeta, OverrideMeta)).

to_duration(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> to_integer(Str)
    end.

to_duration_s(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I / 1000)};
        _ -> to_integer(Str)
    end.

-spec to_duration_ms(Input) -> {ok, integer()} | {error, Input} when
    Input :: string() | binary().
to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I)};
        _ -> to_integer(Str)
    end.

-spec to_timeout_duration(Input) -> {ok, timeout_duration()} | {error, Input} when
    Input :: string() | binary().
to_timeout_duration(Str) ->
    do_to_timeout_duration(Str, fun to_duration/1, ?MAX_INT_TIMEOUT_MS, "ms").

-spec to_timeout_duration_ms(Input) -> {ok, timeout_duration_ms()} | {error, Input} when
    Input :: string() | binary().
to_timeout_duration_ms(Str) ->
    do_to_timeout_duration(Str, fun to_duration_ms/1, ?MAX_INT_TIMEOUT_MS, "ms").

-spec to_timeout_duration_s(Input) -> {ok, timeout_duration_s()} | {error, Input} when
    Input :: string() | binary().
to_timeout_duration_s(Str) ->
    do_to_timeout_duration(Str, fun to_duration_s/1, ?MAX_INT_TIMEOUT_S, "s").

do_to_timeout_duration(Str, Fn, Max, Unit) ->
    case Fn(Str) of
        {ok, I} ->
            case I =< Max of
                true ->
                    {ok, I};
                false ->
                    Msg = lists:flatten(
                        io_lib:format("timeout value too large (max: ~b ~s)", [Max, Unit])
                    ),
                    throw(#{
                        schema_module => ?MODULE,
                        message => Msg,
                        kind => validation_error
                    })
            end;
        Err ->
            Err
    end.

to_bytesize(Str) ->
    case hocon_postprocess:bytesize(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> to_integer(Str)
    end.

to_wordsize(Str) ->
    WordSize = erlang:system_info(wordsize),
    case to_bytesize(Str) of
        {ok, Bytes} -> {ok, Bytes div WordSize};
        Error -> Error
    end.

to_integer(Str) ->
    case string:to_integer(Str) of
        {Int, []} -> {ok, Int};
        {Int, <<>>} -> {ok, Int};
        _ -> {error, Str}
    end.

to_percent(Str) ->
    Percent = hocon_postprocess:percent(Str),
    case is_number(Percent) andalso Percent >= 0.0 andalso Percent =< 1.0 of
        true -> {ok, Percent};
        false -> {error, Str}
    end.

to_comma_separated_list(Str) ->
    {ok, string:tokens(Str, ", ")}.

to_comma_separated_binary(Str) ->
    {ok, lists:map(fun unicode:characters_to_binary/1, string:tokens(Str, ", "))}.

to_comma_separated_atoms(Str) ->
    {ok, lists:map(fun to_atom/1, string:tokens(Str, ", "))}.

to_url(Str) ->
    case emqx_http_lib:uri_parse(Str) of
        {ok, URIMap} ->
            URIString = emqx_http_lib:normalize(URIMap),
            {ok, unicode:characters_to_binary(URIString)};
        Error ->
            Error
    end.

to_json_binary(Str) ->
    case emqx_utils_json:safe_decode(Str) of
        {ok, _} ->
            {ok, unicode:characters_to_binary(Str)};
        Error ->
            Error
    end.

to_template(Str) ->
    {ok, unicode:characters_to_binary(Str, utf8)}.

to_template_str(Str) ->
    {ok, unicode:characters_to_list(Str, utf8)}.

%% @doc support the following format:
%%  - 127.0.0.1:1883
%%  - ::1:1883
%%  - [::1]:1883
%%  - :1883
%%  - :::1883
to_ip_port(Str) ->
    case split_ip_port(Str) of
        {"", Port} ->
            %% this is a local address
            {ok, parse_port(Port)};
        {MaybeIp, Port} ->
            PortVal = parse_port(Port),
            case inet:parse_address(MaybeIp) of
                {ok, IpTuple} ->
                    {ok, {IpTuple, PortVal}};
                _ ->
                    {error, bad_ip_port}
            end;
        _ ->
            {error, bad_ip_port}
    end.

split_ip_port(Str0) ->
    Str = re:replace(Str0, " ", "", [{return, list}, global]),
    case lists:split(string:rchr(Str, $:), Str) of
        %% no colon
        {[], Str} ->
            {"", Str};
        {IpPlusColon, PortString} ->
            IpStr0 = lists:droplast(IpPlusColon),
            case IpStr0 of
                %% drop head/tail brackets
                [$[ | S] ->
                    case lists:last(S) of
                        $] -> {lists:droplast(S), PortString};
                        _ -> error
                    end;
                _ ->
                    {IpStr0, PortString}
            end
    end.

to_erl_cipher_suite(Str) ->
    case ssl:str_to_suite(Str) of
        {error, Reason} -> error({invalid_cipher, Reason});
        Cipher -> Cipher
    end.

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

validate_heap_size(Siz) when is_integer(Siz) ->
    WordSize = erlang:system_info(wordsize),
    %% 128 GB
    MaxSiz = (128 * 1024 * 1024 * 1024) div WordSize,
    case Siz > MaxSiz of
        true ->
            {error, #{cause => max_heap_size_too_large, maximum => MaxSiz}};
        false ->
            ok
    end;
validate_heap_size(_SizStr) ->
    {error, invalid_heap_size}.

validate_max_packet_size(Siz) when is_integer(Siz) andalso Siz < 1 ->
    {error, #{cause => max_mqtt_packet_size_too_small, minimum => 1}};
validate_max_packet_size(Siz) when is_integer(Siz) andalso Siz > ?MAX_INT_MQTT_PACKET_SIZE ->
    {error, #{
        cause => max_mqtt_packet_size_too_large,
        maximum => ?MAX_INT_MQTT_PACKET_SIZE
    }};
validate_max_packet_size(Siz) when is_integer(Siz) ->
    ok;
validate_max_packet_size(_SizStr) ->
    {error, invalid_packet_size}.

%% This is for backward compatibility.
%% We used to allow setting 256MB, but in fact the limit is one byte less.
convert_max_packet_size(<<"256MB">>, _) ->
    iolist_to_binary([integer_to_list(?MAX_INT_MQTT_PACKET_SIZE), "B"]);
convert_max_packet_size(X, _) ->
    X.

validate_keepalive_multiplier(Multiplier) when
    is_number(Multiplier) andalso Multiplier >= 1.0 andalso Multiplier =< 65535.0
->
    ok;
validate_keepalive_multiplier(_Multiplier) ->
    {error, #{reason => keepalive_multiplier_out_of_range, min => 1, max => 65535}}.

validate_tcp_keepalive(Value) ->
    case unicode:characters_to_binary(Value) of
        <<"none">> ->
            ok;
        _ ->
            _ = parse_tcp_keepalive(Value),
            ok
    end.

%% @doc This function is used as value validator and also run-time parser.
parse_tcp_keepalive(Str) ->
    try
        {ok, [Idle, Interval, Probes]} = to_comma_separated_binary(Str),
        %% use 10 times the Linux defaults as range limit
        IdleInt = parse_ka_int(Idle, "Idle", 1, 7200_0),
        IntervalInt = parse_ka_int(Interval, "Interval", 1, 75_0),
        ProbesInt = parse_ka_int(Probes, "Probes", 1, 9_0),
        {IdleInt, IntervalInt, ProbesInt}
    catch
        error:_ ->
            throw(#{
                reason => "Not comma separated positive integers of 'Idle,Interval,Probes' format",
                value => Str
            })
    end.

parse_ka_int(Bin, Name, Min, Max) ->
    I = binary_to_integer(string:trim(Bin)),
    case I >= Min andalso I =< Max of
        true ->
            I;
        false ->
            Msg = io_lib:format("TCP-Keepalive '~s' value must be in the rage of [~p, ~p].", [
                Name, Min, Max
            ]),
            throw(#{reason => lists:flatten(Msg), value => I})
    end.

user_lookup_fun_tr(undefined, Opts) ->
    user_lookup_fun_tr(<<"emqx_tls_psk:lookup">>, Opts);
user_lookup_fun_tr(Lookup, #{make_serializable := true}) ->
    fmt_user_lookup_fun(Lookup);
user_lookup_fun_tr(Lookup, _) ->
    parse_user_lookup_fun(Lookup).

fmt_user_lookup_fun({Fun, _}) when is_function(Fun, 3) ->
    {module, Mod} = erlang:fun_info(Fun, module),
    {name, Name} = erlang:fun_info(Fun, name),
    atom_to_list(Mod) ++ ":" ++ atom_to_list(Name);
fmt_user_lookup_fun(Other) ->
    %% already serializable
    Other.

parse_user_lookup_fun({Fun, _} = Lookup) when is_function(Fun, 3) -> Lookup;
parse_user_lookup_fun(StrConf) ->
    [ModStr, FunStr] = string:tokens(str(StrConf), ": "),
    Mod = list_to_atom(ModStr),
    Fun = list_to_atom(FunStr),
    {fun Mod:Fun/3, undefined}.

validate_ciphers(Ciphers) ->
    Set = emqx_tls_lib:all_ciphers_set_cached(),
    case lists:filter(fun(Cipher) -> not sets:is_element(Cipher, Set) end, Ciphers) of
        [] -> ok;
        Bad -> {error, {bad_ciphers, Bad}}
    end.

validate_tls_versions(Collection, Versions) ->
    AvailableVersions = available_tls_vsns(Collection),
    case lists:filter(fun(V) -> not lists:member(V, AvailableVersions) end, Versions) of
        [] -> validate_tls_version_gap(Versions);
        Vs -> {error, {unsupported_tls_versions, Vs}}
    end.

%% See also `validate_version_gap/1` in OTP ssl.erl,
%% e.g: https://github.com/emqx/otp/blob/emqx-OTP-25.1.2/lib/ssl/src/ssl.erl#L2566.
%% Do not allow configuration of TLS 1.3 with a gap where TLS 1.2 is not supported
%% as that configuration can trigger the built in version downgrade protection
%% mechanism and the handshake can fail with an Illegal Parameter alert.
validate_tls_version_gap(Versions) ->
    case lists:member('tlsv1.3', Versions) of
        true when length(Versions) >= 2 ->
            case lists:member('tlsv1.2', Versions) of
                true ->
                    ok;
                false ->
                    {error,
                        "Using multiple versions that include tlsv1.3 but "
                        "exclude tlsv1.2 is not allowed"}
            end;
        _ ->
            ok
    end.

validations() ->
    [
        {check_process_watermark, fun check_process_watermark/1},
        {check_cpu_watermark, fun check_cpu_watermark/1}
    ].

%% validations from emqx_conf_schema, we must filter other *_schema by undefined.
check_process_watermark(Conf) ->
    check_watermark("sysmon.vm.process_low_watermark", "sysmon.vm.process_high_watermark", Conf).

check_cpu_watermark(Conf) ->
    check_watermark("sysmon.os.cpu_low_watermark", "sysmon.os.cpu_high_watermark", Conf).

check_watermark(LowKey, HighKey, Conf) ->
    case to_percent(hocon_maps:get(LowKey, Conf)) of
        {error, undefined} ->
            true;
        {ok, Low} ->
            case to_percent(hocon_maps:get(HighKey, Conf)) of
                {ok, High} when High > Low -> true;
                {ok, High} -> {bad_watermark, #{LowKey => Low, HighKey => High}};
                {error, HighVal} -> {bad_watermark, #{HighKey => HighVal}}
            end;
        {error, Low} ->
            {bad_watermark, #{LowKey => Low}}
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

-spec qos() -> typerefl:type().
qos() ->
    typerefl:alias("qos", typerefl:union([0, 1, 2])).

non_empty_string(<<>>) -> {error, empty_string_not_allowed};
non_empty_string("") -> {error, empty_string_not_allowed};
non_empty_string(S) when is_binary(S); is_list(S) -> ok;
non_empty_string(_) -> {error, invalid_string}.

non_empty_array([]) ->
    {error, empty_array_not_allowed};
non_empty_array(List) when is_list(List) ->
    ok;
non_empty_array(_) ->
    {error, invalid_data}.

%% @doc Make schema for 'server' or 'servers' field.
%% for each field, there are three passes:
%% 1. converter: Normalize the value.
%%               This normalized value is stored in EMQX's raw config.
%% 2. validator: Validate the normalized value.
%%               Besides checkin if the value can be empty or undefined
%%               it also calls the 3rd pass to see if the provided
%%               hosts can be successfully parsed.
%% 3. parsing: Done at runtime in each module which uses this config
servers_sc(Meta0, ParseOpts) ->
    %% if this field has a default value
    %% then it is not NOT required
    %% NOTE: maps:is_key is not the solution because #{default => undefined} is legit
    HasDefault = (maps:get(default, Meta0, undefined) =/= undefined),
    Required = maps:get(required, Meta0, not HasDefault),
    Meta = #{
        required => Required,
        converter => fun convert_servers/2,
        validator => servers_validator(ParseOpts, Required)
    },
    sc(string(), maps:merge(Meta, Meta0)).

%% @hidden Convert a deep map to host:port pairs.
%% This is due to the fact that a host:port string
%% often can be parsed as a HOCON struct.
%% e.g. when a string from environment variable is `host.domain.name:80'
%% without escaped quotes, it's parsed as
%% `#{<<"host">> => #{<<"domain">> => #{<<"name">> => 80}}}'
%% and when it is a comma-separated list of host:port pairs
%% like `h1.foo:80, h2.bar:81' then it is parsed as
%% `#{<<"h1">> => #{<<"foo">> => 80}, <<"h2">> => #{<<"bar">> => 81}}'
%% This function is to format the map back to host:port (pairs)
%% This function also tries to remove spaces around commas in comma-separated,
%% `host:port' list, and format string array to comma-separated.
convert_servers(HoconValue, _HoconOpts) ->
    convert_servers(HoconValue).

convert_servers(undefined) ->
    %% should not format 'undefined' as string
    %% not to throw exception either
    %% (leave it to the 'required => true | false' check)
    undefined;
convert_servers(Map) when is_map(Map) ->
    try
        List = convert_hocon_map_host_port(Map),
        unicode:characters_to_binary(string:join(List, ","))
    catch
        _:_ ->
            throw("bad_host_port")
    end;
convert_servers([H | _] = Array) when is_binary(H) orelse is_list(H) ->
    %% if the old config was a string array
    %% we want to make sure it's converted to a comma-separated
    unicode:characters_to_binary([[I, ","] || I <- Array]);
convert_servers(Str) ->
    normalize_host_port_str(Str).

%% remove spaces around comma (,)
normalize_host_port_str(Str) ->
    unicode:characters_to_binary(re:replace(Str, "(\s)*,(\s)*", ",")).

%% @doc Shared validation function for both 'server' and 'servers' string.
%% NOTE: Validator is called after converter.
servers_validator(Opts, Required) ->
    fun(Str0) ->
        case str(Str0) of
            "" ->
                %% Empty string is not allowed even if the field is not required
                %% we should remove field from config if it's empty
                throw("cannot_be_empty");
            "undefined" when Required ->
                %% when the field is not set in config file
                %% NOTE: assuming nobody is going to name their server "undefined"
                throw("cannot_be_empty");
            "undefined" ->
                ok;
            Str ->
                %% it's valid as long as it can be parsed
                _ = parse_servers(Str, Opts),
                ok
        end
    end.

%% @doc Parse `host[:port]' endpoint to a `{Host, Port}' tuple or just `Host' string.
%% `Opt' is a `map()' with below options supported:
%%
%% `default_port': a port number, so users are not forced to configure
%%                 port number.
%% `no_port': by default it's `false', when set to `true',
%%            a `throw' exception is raised if the port is found.
-spec parse_server(undefined | string() | binary(), server_parse_option()) ->
    undefined | parsed_server().
parse_server(Str, Opts) ->
    case parse_servers(Str, Opts) of
        undefined ->
            undefined;
        [L] ->
            L;
        [_ | _] = L ->
            throw("expecting_one_host_but_got: " ++ integer_to_list(length(L)))
    end.

%% @doc Parse comma separated `host[:port][,host[:port]]' endpoints
%% into a list of `{Host, Port}' tuples or just `Host' string.
-spec parse_servers(undefined | string() | binary(), server_parse_option()) ->
    undefined | [parsed_server()].
parse_servers(undefined, _Opts) ->
    %% should not parse 'undefined' as string,
    %% not to throw exception either,
    %% leave it to the 'required => true | false' check
    undefined;
parse_servers(Str, Opts) ->
    case do_parse_servers(Str, Opts) of
        [] ->
            %% treat empty as 'undefined'
            undefined;
        [_ | _] = L ->
            L
    end.

do_parse_servers([H | _] = Array, Opts) when is_binary(H) orelse is_list(H) ->
    %% the old schema allowed providing a list of strings
    %% e.g. ["server1:80", "server2:80"]
    lists:map(
        fun(HostPort) ->
            do_parse_server(str(HostPort), Opts)
        end,
        Array
    );
do_parse_servers(Str, Opts) when is_binary(Str) orelse is_list(Str) ->
    lists:map(
        fun(HostPort) ->
            do_parse_server(HostPort, Opts)
        end,
        split_host_port(Str)
    ).

split_host_port(Str) ->
    lists:filtermap(
        fun(S) ->
            case string:strip(S) of
                "" -> false;
                X -> {true, X}
            end
        end,
        string:tokens(str(Str), ",")
    ).

do_parse_server(Str, Opts) ->
    DefaultPort = maps:get(default_port, Opts, undefined),
    NotExpectingPort = maps:get(no_port, Opts, false),
    DefaultScheme = maps:get(default_scheme, Opts, undefined),
    SupportedSchemes = maps:get(supported_schemes, Opts, []),
    NotExpectingScheme = (not is_list(DefaultScheme)) andalso length(SupportedSchemes) =:= 0,
    case is_integer(DefaultPort) andalso NotExpectingPort of
        true ->
            %% either provide a default port from schema,
            %% or do not allow user to set port number
            error("bad_schema");
        false ->
            ok
    end,
    case is_list(DefaultScheme) andalso (not lists:member(DefaultScheme, SupportedSchemes)) of
        true ->
            %% inconsistent schema
            error("bad_schema");
        false ->
            ok
    end,
    %% do not split with space, there should be no space allowed between host and port
    Tokens = string:tokens(Str, ":"),
    Context = #{
        not_expecting_port => NotExpectingPort,
        not_expecting_scheme => NotExpectingScheme,
        default_port => DefaultPort,
        default_scheme => DefaultScheme,
        opts => Opts
    },
    check_server_parts(Tokens, Context).

check_server_parts([Scheme, "//" ++ Hostname, Port], Context) ->
    #{
        not_expecting_scheme := NotExpectingScheme,
        not_expecting_port := NotExpectingPort,
        opts := Opts
    } = Context,
    NotExpectingPort andalso throw("not_expecting_port_number"),
    NotExpectingScheme andalso throw("not_expecting_scheme"),
    #{
        scheme => check_scheme(Scheme, Opts),
        hostname => check_hostname(Hostname),
        port => parse_port(Port)
    };
check_server_parts([Scheme, "//" ++ Hostname], Context) ->
    #{
        not_expecting_scheme := NotExpectingScheme,
        not_expecting_port := NotExpectingPort,
        default_port := DefaultPort,
        opts := Opts
    } = Context,
    NotExpectingScheme andalso throw("not_expecting_scheme"),
    case is_integer(DefaultPort) of
        true ->
            #{
                scheme => check_scheme(Scheme, Opts),
                hostname => check_hostname(Hostname),
                port => DefaultPort
            };
        false when NotExpectingPort ->
            #{
                scheme => check_scheme(Scheme, Opts),
                hostname => check_hostname(Hostname)
            };
        false ->
            throw("missing_port_number")
    end;
check_server_parts([Hostname, Port], Context) ->
    #{
        not_expecting_port := NotExpectingPort,
        default_scheme := DefaultScheme
    } = Context,
    NotExpectingPort andalso throw("not_expecting_port_number"),
    case is_list(DefaultScheme) of
        false ->
            #{
                hostname => check_hostname(Hostname),
                port => parse_port(Port)
            };
        true ->
            #{
                scheme => DefaultScheme,
                hostname => check_hostname(Hostname),
                port => parse_port(Port)
            }
    end;
check_server_parts([Hostname], Context) ->
    #{
        not_expecting_scheme := NotExpectingScheme,
        not_expecting_port := NotExpectingPort,
        default_port := DefaultPort,
        default_scheme := DefaultScheme
    } = Context,
    case is_integer(DefaultPort) orelse NotExpectingPort of
        true ->
            ok;
        false ->
            throw("missing_port_number")
    end,
    case is_list(DefaultScheme) orelse NotExpectingScheme of
        true ->
            ok;
        false ->
            throw("missing_scheme")
    end,
    case {is_integer(DefaultPort), is_list(DefaultScheme)} of
        {true, true} ->
            #{
                scheme => DefaultScheme,
                hostname => check_hostname(Hostname),
                port => DefaultPort
            };
        {true, false} ->
            #{
                hostname => check_hostname(Hostname),
                port => DefaultPort
            };
        {false, true} ->
            #{
                scheme => DefaultScheme,
                hostname => check_hostname(Hostname)
            };
        {false, false} ->
            #{hostname => check_hostname(Hostname)}
    end;
check_server_parts(_Tokens, _Context) ->
    throw("bad_host_port").

check_scheme(Str, Opts) ->
    SupportedSchemes = maps:get(supported_schemes, Opts, []),
    IsSupported = lists:member(Str, SupportedSchemes),
    case IsSupported of
        true ->
            Str;
        false ->
            throw("unsupported_scheme")
    end.

check_hostname(Str) ->
    %% not intended to use inet_parse:domain here
    %% only checking space because it interferes the parsing
    case string:tokens(Str, " ") of
        [H] ->
            case is_port_number(H) of
                true ->
                    throw("expecting_hostname_but_got_a_number");
                false ->
                    H
            end;
        _ ->
            throw("hostname_has_space")
    end.

convert_hocon_map_host_port(Map) ->
    lists:map(
        fun({Host, Port}) ->
            %% Only when Host:Port string is a valid HOCON object
            %% is it possible for the converter to reach here.
            %%
            %% For example EMQX_FOO__SERVER='1.2.3.4:1234' is parsed as
            %% a HOCON string value "1.2.3.4:1234" but not a map because
            %% 1 is not a valid HOCON field.
            %%
            %% EMQX_FOO__SERVER="local.domain.host" (without ':port')
            %% is also not a valid HOCON object (because it has no value),
            %% hence parsed as string.
            true = (Port > 0),
            str(Host) ++ ":" ++ integer_to_list(Port)
        end,
        hocon_maps:flatten(Map, #{})
    ).

is_port_number(Port) ->
    try
        _ = parse_port(Port),
        true
    catch
        _:_ ->
            false
    end.

parse_port(Port) ->
    case string:to_integer(string:strip(Port)) of
        {P, ""} when P < 0 -> throw("port_number_must_be_positive");
        {P, ""} when P > 65535 -> throw("port_number_too_large");
        {P, ""} -> P;
        _ -> throw("bad_port_number")
    end.

quic_feature_toggle(Desc) ->
    sc(
        %% true, false are for user facing
        %% 0, 1 are for internal representation
        typerefl:alias("boolean", typerefl:union([true, false, 0, 1])),
        #{
            desc => Desc,
            importance => ?IMPORTANCE_HIDDEN,
            required => false,
            converter => fun
                (Val, #{make_serializable := true}) -> Val;
                (true, _Opts) -> 1;
                (false, _Opts) -> 0;
                (Other, _Opts) -> Other
            end
        }
    ).

quic_lowlevel_settings_uint(Low, High, Desc) ->
    sc(
        range(Low, High),
        #{
            required => false,
            importance => ?IMPORTANCE_HIDDEN,
            desc => Desc
        }
    ).

-spec is_quic_ssl_opts(string()) -> boolean().
is_quic_ssl_opts(Name) ->
    lists:member(Name, [
        "cacertfile",
        "certfile",
        "keyfile",
        "verify",
        "password",
        "hibernate_after"
        %% Followings are planned
        %% , "fail_if_no_peer_cert"
        %% , "handshake_timeout"
        %% , "gc_after_handshake"
    ]).

assert_required_field(Conf, Key, ErrorMessage) ->
    case maps:get(Key, Conf, undefined) of
        undefined ->
            throw(ErrorMessage);
        _ ->
            ok
    end.

default_listener(tcp) ->
    #{
        <<"bind">> => <<"0.0.0.0:1883">>
    };
default_listener(ws) ->
    #{
        <<"bind">> => <<"0.0.0.0:8083">>,
        <<"websocket">> => #{<<"mqtt_path">> => <<"/mqtt">>}
    };
default_listener(SSLListener) ->
    %% The env variable is resolved in emqx_tls_lib by calling naive_env_interpolate
    SslOptions = #{
        <<"cacertfile">> => cert_file(<<"cacert.pem">>, server),
        <<"certfile">> => cert_file(<<"cert.pem">>, server),
        <<"keyfile">> => cert_file(<<"key.pem">>, server)
    },
    case SSLListener of
        ssl ->
            #{
                <<"bind">> => <<"0.0.0.0:8883">>,
                <<"ssl_options">> => SslOptions
            };
        wss ->
            #{
                <<"bind">> => <<"0.0.0.0:8084">>,
                <<"ssl_options">> => SslOptions,
                <<"websocket">> => #{<<"mqtt_path">> => <<"/mqtt">>}
            }
    end.

%% @doc This function helps to perform a naive string interpolation which
%% only looks at the first segment of the string and tries to replace it.
%% For example
%%  "$MY_FILE_PATH"
%%  "${MY_FILE_PATH}"
%%  "$ENV_VARIABLE/sub/path"
%%  "${ENV_VARIABLE}/sub/path"
%%  "${ENV_VARIABLE}\sub\path" # windows
%% This function returns undefined if the input is undefined
%% otherwise always return string.
naive_env_interpolation(undefined) ->
    undefined;
naive_env_interpolation(Bin) when is_binary(Bin) ->
    naive_env_interpolation(unicode:characters_to_list(Bin, utf8));
naive_env_interpolation("$" ++ Maybe = Original) ->
    {Env, Tail} = split_path(Maybe),
    case resolve_env(Env) of
        {ok, Path} ->
            filename:join([Path, Tail]);
        error ->
            ?SLOG(warning, #{
                msg => "cannot_resolve_env_variable",
                env => Env,
                original => Original
            }),
            Original
    end;
naive_env_interpolation(Other) ->
    Other.

split_path(Path) ->
    {Name0, Tail} = split_path(Path, []),
    {string:trim(Name0, both, "{}"), Tail}.

split_path([], Acc) ->
    {lists:reverse(Acc), []};
split_path([Char | Rest], Acc) when Char =:= $/ orelse Char =:= $\\ ->
    {lists:reverse(Acc), string:trim(Rest, leading, "/\\")};
split_path([Char | Rest], Acc) ->
    split_path(Rest, [Char | Acc]).

resolve_env(Name) ->
    Value = os:getenv(Name),
    case Value =/= false andalso Value =/= "" of
        true ->
            {ok, Value};
        false ->
            special_env(Name)
    end.

-ifdef(TEST).
%% when running tests, we need to mock the env variables
special_env("EMQX_ETC_DIR") ->
    {ok, filename:join([code:lib_dir(emqx), etc])};
special_env("EMQX_LOG_DIR") ->
    {ok, "log"};
special_env(_Name) ->
    %% only in tests
    error.
-else.
special_env(_Name) -> error.
-endif.

%% The tombstone atom.
tombstone() ->
    ?TOMBSTONE_TYPE.

%% Make a map type, the value of which is allowed to be 'marked_for_deletion'
%% 'marked_for_delition' is a special value which means the key is deleted.
%% This is used to support the 'delete' operation in configs,
%% since deleting the key would result in default value being used.
tombstone_map(Name, Type) ->
    %% marked_for_deletion must be the last member of the union
    %% because we need to first union member to populate the default values
    map(
        Name,
        hoconsc:union(
            fun
                (all_union_members) ->
                    [Type, ?TOMBSTONE_TYPE];
                ({value, V}) when is_map(V) ->
                    [Type];
                ({value, _}) ->
                    [?TOMBSTONE_TYPE]
            end
        )
    ).

%% inverse of mark_del_map
get_tombstone_map_value_type(Schema) ->
    %% TODO: violation of abstraction, expose an API in hoconsc
    %% hoconsc:map_value_type(Schema)
    ?MAP(_Name, Union) = hocon_schema:field_schema(Schema, type),
    %% TODO: violation of abstraction, fix hoconsc:union_members/1
    ?UNION(Members, _) = Union,
    Tombstone = tombstone(),
    [Type, Tombstone] = hoconsc:union_members(Members),
    Type.

%% Keep the 'default' tombstone, but delete others.
keep_default_tombstone(Map, _Opts) when is_map(Map) ->
    maps:filter(
        fun(Key, Value) ->
            Key =:= <<"default">> orelse Value =/= ?TOMBSTONE_VALUE
        end,
        Map
    );
keep_default_tombstone(Value, _Opts) ->
    Value.

ensure_default_listener(undefined, ListenerType) ->
    %% let the schema's default value do its job
    #{<<"default">> => default_listener(ListenerType)};
ensure_default_listener(#{<<"default">> := _} = Map, _ListenerType) ->
    keep_default_tombstone(Map, #{});
ensure_default_listener(Map, ListenerType) ->
    NewMap = Map#{<<"default">> => default_listener(ListenerType)},
    keep_default_tombstone(NewMap, #{}).

cert_file(_File, client) ->
    undefined;
cert_file(File, server) ->
    unicode:characters_to_binary(filename:join(["${EMQX_ETC_DIR}", "certs", File])).

mqtt_converter(#{<<"keepalive_multiplier">> := Multi} = Mqtt, _Opts) ->
    case round(Multi * 100) =:= round(?DEFAULT_MULTIPLIER * 100) of
        false ->
            %% Multiplier is provided, and it's not default value
            Mqtt;
        true ->
            %% Multiplier is default value, fallback to use Backoff value
            %% Backoff default value was half of Multiplier default value
            %% so there is no need to compare Backoff with its default.
            Backoff = maps:get(<<"keepalive_backoff">>, Mqtt, ?DEFAULT_BACKOFF),
            Mqtt#{<<"keepalive_multiplier">> => Backoff * 2}
    end;
mqtt_converter(#{<<"keepalive_backoff">> := Backoff} = Mqtt, _Opts) ->
    Mqtt#{<<"keepalive_multiplier">> => Backoff * 2};
mqtt_converter(Mqtt, _Opts) ->
    Mqtt.

%% For backward compatibility with window_time is disable
flapping_detect_converter(Conf = #{<<"window_time">> := <<"disable">>}, _Opts) ->
    Conf#{<<"window_time">> => ?DEFAULT_WINDOW_TIME, <<"enable">> => false};
flapping_detect_converter(Conf, _Opts) ->
    Conf.

mqtt_general() ->
    [
        {"idle_timeout",
            sc(
                hoconsc:union([infinity, duration()]),
                #{
                    default => <<"15s">>,
                    desc => ?DESC(mqtt_idle_timeout)
                }
            )},
        {"max_packet_size",
            sc(
                bytesize(),
                #{
                    default => <<"1MB">>,
                    validator => fun ?MODULE:validate_max_packet_size/1,
                    converter => fun ?MODULE:convert_max_packet_size/2,
                    desc => ?DESC(mqtt_max_packet_size)
                }
            )},
        {"max_clientid_len",
            sc(
                %% MQTT-v3.1.1-[MQTT-3.1.3-5], MQTT-v5.0-[MQTT-3.1.3-5]
                range(23, 65535),
                #{
                    default => 65535,
                    desc => ?DESC(mqtt_max_clientid_len)
                }
            )},
        {"max_topic_levels",
            sc(
                range(1, 65535),
                #{
                    default => 128,
                    desc => ?DESC(mqtt_max_topic_levels)
                }
            )},
        {"max_topic_alias",
            sc(
                range(0, 65535),
                #{
                    default => 65535,
                    desc => ?DESC(mqtt_max_topic_alias)
                }
            )},
        {"retain_available",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_retain_available)
                }
            )},
        {"wildcard_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_wildcard_subscription)
                }
            )},
        {"shared_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_shared_subscription)
                }
            )},
        {"shared_subscription_strategy",
            sc(
                hoconsc:enum([
                    random,
                    round_robin,
                    round_robin_per_group,
                    sticky,
                    local,
                    hash_topic,
                    hash_clientid
                ]),
                #{
                    default => round_robin,
                    desc => ?DESC(mqtt_shared_subscription_strategy)
                }
            )},
        {"shared_subscription_initial_sticky_pick",
            sc(
                hoconsc:enum([
                    random,
                    local,
                    hash_topic,
                    hash_clientid
                ]),
                #{
                    default => random,
                    desc => ?DESC(mqtt_shared_subscription_initial_sticky_pick)
                }
            )},
        {"exclusive_subscription",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_exclusive_subscription)
                }
            )},
        {"ignore_loop_deliver",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_ignore_loop_deliver)
                }
            )},
        {"strict_mode",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_strict_mode)
                }
            )},
        {"response_information",
            sc(
                string(),
                #{
                    default => <<"">>,
                    desc => ?DESC(mqtt_response_information)
                }
            )},
        {"server_keepalive",
            sc(
                hoconsc:union([pos_integer(), disabled]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_server_keepalive)
                }
            )},
        {"keepalive_backoff",
            sc(
                number(),
                #{
                    default => ?DEFAULT_BACKOFF,
                    %% Must add required => false, zone schema has no default.
                    required => false,
                    importance => ?IMPORTANCE_HIDDEN
                }
            )},
        {"keepalive_multiplier",
            sc(
                number(),
                #{
                    default => ?DEFAULT_MULTIPLIER,
                    validator => fun ?MODULE:validate_keepalive_multiplier/1,
                    desc => ?DESC(mqtt_keepalive_multiplier)
                }
            )},
        {"keepalive_check_interval",
            sc(
                timeout_duration(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC(mqtt_keepalive_check_interval)
                }
            )},
        {"retry_interval",
            sc(
                hoconsc:union([infinity, timeout_duration()]),
                #{
                    default => infinity,
                    desc => ?DESC(mqtt_retry_interval)
                }
            )},
        {"use_username_as_clientid",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_use_username_as_clientid)
                }
            )},
        {"peer_cert_as_username",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_peer_cert_as_username)
                }
            )},
        {"peer_cert_as_clientid",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_peer_cert_as_clientid)
                }
            )},
        {"client_attrs_init",
            sc(
                hoconsc:array(ref("client_attrs_init")),
                #{
                    default => [],
                    desc => ?DESC("client_attrs_init")
                }
            )},
        {"clientid_override",
            sc(
                hoconsc:union([disabled, typerefl:alias("string", any())]),
                #{
                    default => disabled,
                    desc => ?DESC("clientid_override"),
                    converter => fun compile_variform_allow_disabled/2
                }
            )}
    ].
%% All session's importance should be lower than general part to organize document.
mqtt_session() ->
    [
        {"session_expiry_interval",
            sc(
                duration(),
                #{
                    default => <<"2h">>,
                    desc => ?DESC(mqtt_session_expiry_interval),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"message_expiry_interval",
            sc(
                hoconsc:union([duration(), infinity]),
                #{
                    default => infinity,
                    desc => ?DESC(mqtt_message_expiry_interval),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"max_awaiting_rel",
            sc(
                hoconsc:union([non_neg_integer(), infinity]),
                #{
                    default => 100,
                    desc => ?DESC(mqtt_max_awaiting_rel),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"max_qos_allowed",
            sc(
                qos(),
                #{
                    default => 2,
                    desc => ?DESC(mqtt_max_qos_allowed),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"mqueue_priorities",
            sc(
                hoconsc:union([disabled, map()]),
                #{
                    default => disabled,
                    desc => ?DESC(mqtt_mqueue_priorities),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"mqueue_default_priority",
            sc(
                hoconsc:enum([highest, lowest]),
                #{
                    default => lowest,
                    desc => ?DESC(mqtt_mqueue_default_priority),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"mqueue_store_qos0",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC(mqtt_mqueue_store_qos0),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"max_mqueue_len",
            sc(
                hoconsc:union([non_neg_integer(), infinity]),
                #{
                    default => 1000,
                    desc => ?DESC(mqtt_max_mqueue_len),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"max_inflight",
            sc(
                range(1, 65535),
                #{
                    default => 32,
                    desc => ?DESC(mqtt_max_inflight),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"max_subscriptions",
            sc(
                hoconsc:union([range(1, inf), infinity]),
                #{
                    default => infinity,
                    desc => ?DESC(mqtt_max_subscriptions),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"upgrade_qos",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => ?DESC(mqtt_upgrade_qos),
                    importance => ?IMPORTANCE_LOW
                }
            )},
        {"await_rel_timeout",
            sc(
                duration(),
                #{
                    default => <<"300s">>,
                    desc => ?DESC(mqtt_await_rel_timeout),
                    importance => ?IMPORTANCE_LOW
                }
            )}
    ].

default_mem_check_interval() ->
    case emqx_os_mon:is_os_check_supported() of
        true -> <<"60s">>;
        false -> disabled
    end.

description_schema() ->
    sc(
        binary(),
        #{
            default => <<"">>,
            desc => ?DESC(description),
            required => false,
            importance => ?IMPORTANCE_LOW
        }
    ).

tags_schema() ->
    sc(
        hoconsc:array(binary()),
        #{
            desc => ?DESC(resource_tags),
            required => false,
            importance => ?IMPORTANCE_LOW
        }
    ).

ensure_unicode_path(undefined, _) ->
    undefined;
ensure_unicode_path(Path, #{make_serializable := true}) ->
    %% format back to serializable string
    unicode:characters_to_binary(Path, utf8);
ensure_unicode_path(Path, Opts) when is_binary(Path) ->
    case unicode:characters_to_list(Path, utf8) of
        {R, _, _} when R =:= error orelse R =:= incomplete ->
            throw({"bad_file_path_string", Path});
        PathStr ->
            ensure_unicode_path(PathStr, Opts)
    end;
ensure_unicode_path(Path, _) when is_list(Path) ->
    Path;
ensure_unicode_path(Path, _) ->
    throw({"not_string", Path}).

listeners() ->
    [
        {"tcp",
            sc(
                tombstone_map(name, ref("mqtt_tcp_listener")),
                #{
                    desc => ?DESC(fields_listeners_tcp),
                    converter => fun(X, _) ->
                        ensure_default_listener(X, tcp)
                    end,
                    required => {false, recursively}
                }
            )},
        {"ssl",
            sc(
                tombstone_map(name, ref("mqtt_ssl_listener")),
                #{
                    desc => ?DESC(fields_listeners_ssl),
                    converter => fun(X, _) -> ensure_default_listener(X, ssl) end,
                    required => {false, recursively}
                }
            )},
        {"ws",
            sc(
                tombstone_map(name, ref("mqtt_ws_listener")),
                #{
                    desc => ?DESC(fields_listeners_ws),
                    converter => fun(X, _) -> ensure_default_listener(X, ws) end,
                    required => {false, recursively}
                }
            )},
        {"wss",
            sc(
                tombstone_map(name, ref("mqtt_wss_listener")),
                #{
                    desc => ?DESC(fields_listeners_wss),
                    converter => fun(X, _) -> ensure_default_listener(X, wss) end,
                    required => {false, recursively}
                }
            )},
        {"quic",
            sc(
                tombstone_map(name, ref("mqtt_quic_listener")),
                #{
                    desc => ?DESC(fields_listeners_quic),
                    converter => fun keep_default_tombstone/2,
                    required => {false, recursively}
                }
            )}
    ].

mkunion(Field, Schemas) ->
    mkunion(Field, Schemas, none).

mkunion(Field, Schemas, Default) ->
    hoconsc:union(fun(Arg) -> scunion(Field, Schemas, Default, Arg) end).

scunion(_Field, Schemas, _Default, all_union_members) ->
    maps:values(Schemas);
scunion(Field, Schemas, Default, {value, Value}) ->
    Selector =
        case maps:get(emqx_utils_conv:bin(Field), Value, undefined) of
            undefined ->
                Default;
            X ->
                emqx_utils_conv:bin(X)
        end,
    case maps:find(Selector, Schemas) of
        {ok, Schema} ->
            [Schema];
        _Error ->
            throw(#{field_name => Field, expected => maps:keys(Schemas)})
    end.
