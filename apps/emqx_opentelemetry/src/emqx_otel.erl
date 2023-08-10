%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_otel).
-include_lib("emqx/include/logger.hrl").

-export([start_link/1]).
-export([get_cluster_gauge/1, get_stats_gauge/1, get_vm_gauge/1, get_metric_counter/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link(Conf) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Conf, []).

init(Conf) ->
    erlang:process_flag(trap_exit, true),
    {ok, #{}, {continue, {setup, Conf}}}.

handle_continue({setup, Conf}, State) ->
    setup(Conf),
    {noreply, State, hibernate}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    cleanup(),
    ok.

setup(Conf = #{enable := true}) ->
    ensure_apps(Conf),
    create_metric_views();
setup(_Conf) ->
    cleanup(),
    ok.

ensure_apps(Conf) ->
    #{exporter := #{interval := ExporterInterval}} = Conf,
    {ok, _} = application:ensure_all_started(opentelemetry_exporter),
    _ = application:stop(opentelemetry_experimental),
    ok = application:set_env(
        opentelemetry_experimental,
        readers,
        [
            #{
                module => otel_metric_reader,
                config => #{
                    exporter => {opentelemetry_exporter, #{}},
                    export_interval_ms => ExporterInterval
                }
            }
        ]
    ),
    {ok, _} = application:ensure_all_started(opentelemetry_experimental),
    {ok, _} = application:ensure_all_started(opentelemetry_api_experimental),
    ok.

cleanup() ->
    _ = application:stop(opentelemetry_experimental),
    _ = application:stop(opentelemetry_experimental_api),
    _ = application:stop(opentelemetry_exporter),
    ok.

create_metric_views() ->
    Meter = opentelemetry_experimental:get_meter(),
    StatsGauge = emqx_stats:getstats(),
    create_gauge(Meter, StatsGauge, fun ?MODULE:get_stats_gauge/1),
    VmGauge = lists:map(fun({K, V}) -> {normalize_name(K), V} end, emqx_mgmt:vm_stats()),
    create_gauge(Meter, VmGauge, fun ?MODULE:get_vm_gauge/1),
    ClusterGauge = [{'node.running', 0}, {'node.stopped', 0}],
    create_gauge(Meter, ClusterGauge, fun ?MODULE:get_cluster_gauge/1),
    Metrics = lists:map(fun({K, V}) -> {K, V, unit(K)} end, emqx_metrics:all()),
    create_counter(Meter, Metrics, fun ?MODULE:get_metric_counter/1),
    ok.

unit(K) ->
    case lists:member(K, bytes_metrics()) of
        true -> kb;
        false -> '1'
    end.

bytes_metrics() ->
    [
        'bytes.received',
        'bytes.sent',
        'packets.received',
        'packets.sent',
        'packets.connect',
        'packets.connack.sent',
        'packets.connack.error',
        'packets.connack.auth_error',
        'packets.publish.received',
        'packets.publish.sent',
        'packets.publish.inuse',
        'packets.publish.error',
        'packets.publish.auth_error',
        'packets.publish.dropped',
        'packets.puback.received',
        'packets.puback.sent',
        'packets.puback.inuse',
        'packets.puback.missed',
        'packets.pubrec.received',
        'packets.pubrec.sent',
        'packets.pubrec.inuse',
        'packets.pubrec.missed',
        'packets.pubrel.received',
        'packets.pubrel.sent',
        'packets.pubrel.missed',
        'packets.pubcomp.received',
        'packets.pubcomp.sent',
        'packets.pubcomp.inuse',
        'packets.pubcomp.missed',
        'packets.subscribe.received',
        'packets.subscribe.error',
        'packets.subscribe.auth_error',
        'packets.suback.sent',
        'packets.unsubscribe.received',
        'packets.unsubscribe.error',
        'packets.unsuback.sent',
        'packets.pingreq.received',
        'packets.pingresp.sent',
        'packets.disconnect.received',
        'packets.disconnect.sent',
        'packets.auth.received',
        'packets.auth.sent'
    ].

get_stats_gauge(Name) ->
    [{emqx_stats:getstat(Name), #{}}].

get_vm_gauge(Name) ->
    [{emqx_mgmt:vm_stats(Name), #{}}].

get_cluster_gauge('node.running') ->
    length(emqx:cluster_nodes(running));
get_cluster_gauge('node.stopped') ->
    length(emqx:cluster_nodes(stopped)).

get_metric_counter(Name) ->
    [{emqx_metrics:val(Name), #{}}].

create_gauge(Meter, Names, CallBack) ->
    lists:foreach(
        fun({Name, _}) ->
            true = otel_meter_server:add_view(
                #{instrument_name => Name},
                #{aggregation_module => otel_aggregation_last_value}
            ),
            otel_meter:create_observable_gauge(
                Meter,
                Name,
                CallBack,
                Name,
                #{
                    description => iolist_to_binary([
                        <<"observable ">>, atom_to_binary(Name), <<" gauge">>
                    ]),
                    unit => '1'
                }
            )
        end,
        Names
    ).

create_counter(Meter, Counters, CallBack) ->
    lists:foreach(
        fun({Name, _, Unit}) ->
            true = otel_meter_server:add_view(
                #{instrument_name => Name},
                #{aggregation_module => otel_aggregation_sum}
            ),
            otel_meter:create_observable_counter(
                Meter,
                Name,
                CallBack,
                Name,
                #{
                    description => iolist_to_binary([
                        <<"observable ">>, atom_to_binary(Name), <<" counter">>
                    ]),
                    unit => Unit
                }
            )
        end,
        Counters
    ).

normalize_name(Name) ->
    list_to_existing_atom(lists:flatten(string:replace(atom_to_list(Name), "_", ".", all))).
