%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_channel,
        [ handle_in/2
        , handle_out/2
        ]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_CONNINFO,
        #{peername => {{127,0,0,1}, 3456},
          sockname => {{127,0,0,1}, 1883},
          conn_mod => emqx_connection,
          proto_name => <<"MQTT">>,
          proto_ver => ?MQTT_PROTO_V5,
          clean_start => true,
          keepalive => 30,
          clientid => <<"clientid">>,
          username => <<"username">>,
          conn_props => #{},
          receive_maximum => 100,
          expiry_interval => 0
         }).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules([router, broker]),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Test cases for handle_in
%%--------------------------------------------------------------------

t_handle_connect(_) ->
    ConnPkt = #mqtt_packet_connect{
                 proto_name  = <<"MQTT">>,
                 proto_ver   = ?MQTT_PROTO_V4,
                 is_bridge   = false,
                 clean_start = true,
                 keepalive   = 30,
                 properties  = undefined,
                 clientid    = <<"clientid">>,
                 username    = <<"username">>,
                 password    = <<"passwd">>
                },
    with_channel(
      fun(Channel) ->
              ConnAck = ?CONNACK_PACKET(?RC_SUCCESS, 0, #{}),
              ExpectedOutput = [{enter, connected},{outgoing, ConnAck}],
              {ok, Output, Channel1} = handle_in(?CONNECT_PACKET(ConnPkt), Channel),
              ?assertEqual(ExpectedOutput, Output),
              #{clientid := ClientId, username := Username} = emqx_channel:info(clientinfo, Channel1),
              ?assertEqual(<<"clientid">>, ClientId),
              ?assertEqual(<<"username">>, Username)
      end).

t_handle_in_publish_qos0(_) ->
    with_channel(
      fun(Channel) ->
              Publish = ?PUBLISH_PACKET(?QOS_0, <<"topic">>, undefined, <<"payload">>),
              {ok, Channel1} = handle_in(Publish, Channel),
              ?assertEqual(#{publish_in => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_in_publish_qos1(_) ->
    with_channel(
      fun(Channel) ->
              Publish = ?PUBLISH_PACKET(?QOS_1, <<"topic">>, 1, <<"payload">>),
              {ok, ?PUBACK_PACKET(1, RC), _} = handle_in(Publish, Channel),
              ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS))
      end).

t_handle_publish_qos2(_) ->
    with_channel(
      fun(Channel) ->
              Publish1 = ?PUBLISH_PACKET(?QOS_2, <<"topic">>, 1, <<"payload">>),
              {ok, ?PUBREC_PACKET(1, RC), Channel1} = handle_in(Publish1, Channel),
              Publish2 = ?PUBLISH_PACKET(?QOS_2, <<"topic">>, 2, <<"payload">>),
              {ok, ?PUBREC_PACKET(2, RC), Channel2} = handle_in(Publish2, Channel1),
              ?assert((RC == ?RC_SUCCESS) orelse (RC == ?RC_NO_MATCHING_SUBSCRIBERS)),
              #{awaiting_rel := AwaitingRel} = emqx_channel:info(session, Channel2),
              ?assertEqual(2, AwaitingRel)
      end).

t_handle_in_puback(_) ->
    with_channel(
      fun(Channel) ->
              {ok, Channel1} = handle_in(?PUBACK_PACKET(1, ?RC_SUCCESS), Channel),
              ?assertEqual(#{puback_in => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_in_pubrec(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?PUBREL_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), Channel1}
                = handle_in(?PUBREC_PACKET(1, ?RC_SUCCESS), Channel),
              ?assertEqual(#{pubrec_in => 1, pubrel_out => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_in_pubrel(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?PUBCOMP_PACKET(1, ?RC_PACKET_IDENTIFIER_NOT_FOUND), Channel1}
                = handle_in(?PUBREL_PACKET(1, ?RC_SUCCESS), Channel),
              ?assertEqual(#{pubrel_in => 1, pubcomp_out => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_in_pubcomp(_) ->
    with_channel(
      fun(Channel) ->
              {ok, Channel1} = handle_in(?PUBCOMP_PACKET(1, ?RC_SUCCESS), Channel),
              ?assertEqual(#{pubcomp_in => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_subscribe(_) ->
    with_channel(
      fun(Channel) ->
              TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS}],
              {ok, ?SUBACK_PACKET(10, [?QOS_0]), Channel1}
                = handle_in(?SUBSCRIBE_PACKET(10, #{}, TopicFilters), Channel),
              #{subscriptions := Subscriptions}
                = emqx_channel:info(session, Channel1),
              ?assertEqual(maps:from_list(TopicFilters), Subscriptions)
      end).

t_handle_unsubscribe(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?UNSUBACK_PACKET(11), Channel}
                = handle_in(?UNSUBSCRIBE_PACKET(11, #{}, [<<"+">>]), Channel)
      end).

t_handle_pingreq(_) ->
    with_channel(
      fun(Channel) ->
          {ok, ?PACKET(?PINGRESP), Channel} = handle_in(?PACKET(?PINGREQ), Channel)
      end).

t_handle_disconnect(_) ->
    with_channel(
      fun(Channel) ->
              {stop, {shutdown, normal}, Channel1} = handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), Channel),
              ?assertEqual(undefined, emqx_channel:info(will_msg, Channel1))
      end).

t_handle_in_auth(_) ->
    with_channel(
      fun(Channel) ->
              Packet = ?DISCONNECT_PACKET(?RC_IMPLEMENTATION_SPECIFIC_ERROR),
              {stop, {shutdown, implementation_specific_error}, Packet, Channel} = handle_in(?AUTH_PACKET(), Channel)
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_deliver
%%--------------------------------------------------------------------

t_handle_deliver(_) ->
    with_connected_channel(
      fun(Channel) ->
              TopicFilters = [{<<"+">>, ?DEFAULT_SUBOPTS#{qos => ?QOS_2}}],
              {ok, ?SUBACK_PACKET(1, [?QOS_2]), Channel1}
                = handle_in(?SUBSCRIBE_PACKET(1, #{}, TopicFilters), Channel),
              Msg0 = emqx_message:make(<<"clientx">>, ?QOS_0, <<"t0">>, <<"qos0">>),
              Msg1 = emqx_message:make(<<"clientx">>, ?QOS_1, <<"t1">>, <<"qos1">>),
              Delivers = [{deliver, <<"+">>, Msg0}, {deliver, <<"+">>, Msg1}],
              {ok, {outgoing, Packets}, _Ch} = emqx_channel:handle_out(Delivers, Channel1),
              ?assertEqual([?QOS_0, ?QOS_1], [emqx_packet:qos(Pkt)|| Pkt <- Packets])
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_out
%%--------------------------------------------------------------------

t_handle_out_connack(_) ->
    ConnPkt = #mqtt_packet_connect{
                 proto_name  = <<"MQTT">>,
                 proto_ver   = ?MQTT_PROTO_V4,
                 clean_start = true,
                 properties  = #{},
                 clientid    = <<"clientid">>
                },
    with_channel(
      fun(Channel) ->
              {ok, [{enter, connected},{outgoing, ?CONNACK_PACKET(?RC_SUCCESS, SP, _)}], _Chan}
                = handle_out({connack, ?RC_SUCCESS, 0, ConnPkt}, Channel),
              {stop, {shutdown, not_authorized}, ?CONNACK_PACKET(?RC_NOT_AUTHORIZED), _}
                = handle_out({connack, ?RC_NOT_AUTHORIZED, ConnPkt}, Channel)
      end).

t_handle_out_publish(_) ->
    with_channel(
      fun(Channel) ->
              Pub0 = {publish, undefined, emqx_message:make(<<"t">>, <<"qos0">>)},
              Pub1 = {publish, 1, emqx_message:make(<<"c">>, ?QOS_1, <<"t">>, <<"qos1">>)},
              {ok, ?PUBLISH_PACKET(?QOS_0), Channel} = handle_out(Pub0, Channel),
              {ok, ?PUBLISH_PACKET(?QOS_1), Channel} = handle_out(Pub1, Channel),
              {ok, {outgoing, Packets}, Channel1} = handle_out({publish, [Pub0, Pub1]}, Channel),
              ?assertEqual(2, length(Packets)),
              ?assertEqual(#{publish_out => 2}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_out_puback(_) ->
    with_channel(
      fun(Channel) ->
              {ok, Channel} = handle_out({puberr, ?RC_NOT_AUTHORIZED}, Channel),
              {ok, ?PUBACK_PACKET(1, ?RC_SUCCESS), Channel1}
                = handle_out({puback, 1, ?RC_SUCCESS}, Channel),
              ?assertEqual(#{puback_out => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_out_pubrec(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?PUBREC_PACKET(4, ?RC_SUCCESS), Channel1}
                = handle_out({pubrec, 4, ?RC_SUCCESS}, Channel),
              ?assertEqual(#{pubrec_out => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_out_pubrel(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?PUBREL_PACKET(2), Channel1}
                = handle_out({pubrel, 2, ?RC_SUCCESS}, Channel),
              {ok, ?PUBREL_PACKET(3, ?RC_SUCCESS), Channel2}
                = handle_out({pubrel, 3, ?RC_SUCCESS}, Channel1),
              ?assertEqual(#{pubrel_out => 2}, emqx_channel:info(pub_stats, Channel2))
      end).

t_handle_out_pubcomp(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?PUBCOMP_PACKET(5, ?RC_SUCCESS), Channel1}
                = handle_out({pubcomp, 5, ?RC_SUCCESS}, Channel),
              ?assertEqual(#{pubcomp_out => 1}, emqx_channel:info(pub_stats, Channel1))
      end).

t_handle_out_suback(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?SUBACK_PACKET(1, [?QOS_2]), Channel}
                 = handle_out({suback, 1, [?QOS_2]}, Channel)
      end).

t_handle_out_unsuback(_) ->
    with_channel(
      fun(Channel) ->
              {ok, ?UNSUBACK_PACKET(1), Channel}
                = handle_out({unsuback, 1, [?RC_SUCCESS]}, Channel)
      end).

t_handle_out_disconnect(_) ->
    with_channel(
      fun(Channel) ->
              handle_out({disconnect, ?RC_SUCCESS}, Channel)
      end).

%%--------------------------------------------------------------------
%% Test cases for handle_timeout
%%--------------------------------------------------------------------

t_handle_timeout(_) ->
    with_channel(
      fun(Channel) ->
        'TODO'
      end).

%%--------------------------------------------------------------------
%% Test cases for terminate
%%--------------------------------------------------------------------

t_terminate(_) ->
    with_channel(
      fun(Channel) ->
        'TODO'
      end).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

with_connected_channel(TestFun) ->
    with_channel(
      fun(Channel) ->
          TestFun(emqx_channel:set_field(conn_state, connected, Channel))
      end).

with_channel(TestFun) ->
    with_channel(#{}, TestFun).

with_channel(ConnInfo, TestFun) ->
    ConnInfo1 = maps:merge(?DEFAULT_CONNINFO, ConnInfo),
    ClientInfo = #{zone => <<"external">>,
                   protocol => mqtt,
                   peerhost => {127,0,0,1},
                   clientid => <<"clientid">>,
                   username => <<"username">>,
                   peercert => undefined,
                   is_bridge => false,
                   is_superuser => false,
                   mountpoint => undefined
                  },
    Channel = emqx_channel:init(ConnInfo1, [{zone, testing}]),
    Session = emqx_session:init(ClientInfo, ConnInfo1),
    Channel1 = emqx_channel:set_field(clientinfo, ClientInfo, Channel),
    TestFun(emqx_channel:set_field(session, Session, Channel1)).

