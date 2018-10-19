%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-define(TOPICS, [<<"TopicA">>, <<"TopicA/B">>, <<"Topic/C">>, <<"TopicA/C">>,
                 <<"/TopicA">>]).

-define(CLIENT2, ?CONNECT_PACKET(#mqtt_packet_connect{
                                    username  = <<"admin">>,
                                    clean_start = false,
                                    password  = <<"public">>})).

all() ->
    [
     {group, mqttv4},
     {group, mqttv5}].

groups() ->
    [{mqttv4,
      [sequence],
      [
       connect_v4,
       subscribe_v4
      ]},
     {mqttv5,
      [sequence],
      [
       connect_v5,
       subscribe_v5
      ]
     }].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

with_connection(DoFun) ->
    {ok, Sock} = emqx_client_sock:connect({127, 0, 0, 1}, 1883,
                                          [binary, {packet, raw},
                                           {active, false}], 3000),
    try
        DoFun(Sock)
    after
        emqx_client_sock:close(Sock)
    end.

connect_v4(_) ->
    with_connection(fun(Sock) ->
                            emqx_client_sock:send(Sock, raw_send_serialize(?PACKET(?PUBLISH))),
                            {error, closed} =gen_tcp:recv(Sock, 0)
                    end),
    with_connection(fun(Sock) ->
                            ConnectPacket = raw_send_serialize(?CONNECT_PACKET
                                                                   (#mqtt_packet_connect{
                                                                       client_id = <<"mqttv4_client">>,
                                                                       username = <<"admin">>,
                                                                       password = <<"public">>,
                                                                       proto_ver = ?MQTT_PROTO_V4
                                                                      })),
                            emqx_client_sock:send(Sock, ConnectPacket),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_parse(Data, ?MQTT_PROTO_V4),

                            emqx_client_sock:send(Sock, ConnectPacket),
                            {error, closed} = gen_tcp:recv(Sock, 0)
                    end),
    ok.


connect_v5(_) ->
    with_connection(fun(Sock) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(#mqtt_packet_connect{
                                                                       proto_ver  = ?MQTT_PROTO_V5,
                                                                       properties =
                                                                           #{'Request-Response-Information' => -1}}))),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), _} = raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),

    with_connection(fun(Sock) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(
                                                       #mqtt_packet_connect{
                                                          proto_ver  = ?MQTT_PROTO_V5,
                                                          properties =
                                                               #{'Request-Problem-Information' => 2}}))),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR), _} = raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),

    with_connection(fun(Sock) ->
                            emqx_client_sock:send(Sock,
                                                  raw_send_serialize(
                                                    ?CONNECT_PACKET(
                                                       #mqtt_packet_connect{
                                                          proto_ver  = ?MQTT_PROTO_V5,
                                                          properties =
                                                              #{'Request-Response-Information' => 1}})
                                                  )),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?CONNACK_PACKET(?RC_SUCCESS, 0,
                                                 #{'Response-Information' := _RespInfo}), _} =
                                raw_recv_parse(Data, ?MQTT_PROTO_V5)
                    end),
    ok.

do_connect(Sock, ProtoVer) ->
    emqx_client_sock:send(Sock, raw_send_serialize(
                                  ?CONNECT_PACKET(
                                     #mqtt_packet_connect{
                                        client_id  = <<"mqtt_client">>,
                                        proto_ver  = ProtoVer
                                       }))),
    {ok, Data} = gen_tcp:recv(Sock, 0),
    {ok, ?CONNACK_PACKET(?CONNACK_ACCEPT), _} = raw_recv_parse(Data, ProtoVer).

subscribe_v4(_) ->
    with_connection(fun(Sock) ->
                            do_connect(Sock, ?MQTT_PROTO_V4),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(15,
                                                            [{<<"topic">>, #{rh => 1,
                                                                             qos => ?QOS_2,
                                                                             rap => 0,
                                                                             nl => 0,
                                                                             rc => 0}}])),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, Data} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(15, _), _} = raw_recv_parse(Data, ?MQTT_PROTO_V4)
                    end),
    ok.

subscribe_v5(_) ->
    with_connection(fun(Sock) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(?SUBSCRIBE_PACKET(15, #{'Subscription-Identifier' => -1},[]),
                                                            #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_TOPIC_FILTER_INVALID), _} =
                                raw_recv_parse(DisConnData, ?MQTT_PROTO_V5)
                    end),
    with_connection(fun(Sock) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(0, #{}, [{<<"TopicQos0">>,
                                                                      #{rh => 1, qos => ?QOS_2,
                                                                        rap => 0, nl => 0,
                                                                        rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_MALFORMED_PACKET), _} =
                                raw_recv_parse(DisConnData, ?MQTT_PROTO_V5)
                    end),
    with_connection(fun(Sock) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => 0},
                                                            [{<<"TopicQos0">>,
                                                              #{rh => 1, qos => ?QOS_2,
                                                                rap => 0, nl => 0,
                                                                rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, DisConnData} = gen_tcp:recv(Sock, 0),
                            {ok, ?DISCONNECT_PACKET(?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED), _} =
                                raw_recv_parse(DisConnData, ?MQTT_PROTO_V5)
                    end),
    with_connection(fun(Sock) ->
                            do_connect(Sock, ?MQTT_PROTO_V5),
                            SubPacket = raw_send_serialize(
                                          ?SUBSCRIBE_PACKET(1, #{'Subscription-Identifier' => 1},
                                                            [{<<"TopicQos0">>,
                                                              #{rh => 1, qos => ?QOS_2,
                                                                rap => 0, nl => 0,
                                                                rc => 0}}]),
                                          #{version => ?MQTT_PROTO_V5}),
                            emqx_client_sock:send(Sock, SubPacket),
                            {ok, SubData} = gen_tcp:recv(Sock, 0),
                            {ok, ?SUBACK_PACKET(1, #{}, [2]), _} =
                                raw_recv_parse(SubData, ?MQTT_PROTO_V5)
                    end),
    ok.

publish_v4(_) ->
    ok.

publish_v5(_) ->
    ok.

raw_send_serialize(Packet) ->
    emqx_frame:serialize(Packet).

raw_send_serialize(Packet, Opts) ->
    emqx_frame:serialize(Packet, Opts).

raw_recv_parse(P, ProtoVersion) ->
    emqx_frame:parse(P, {none, #{max_packet_size => ?MAX_PACKET_SIZE,
                                 version         => ProtoVersion}}).
