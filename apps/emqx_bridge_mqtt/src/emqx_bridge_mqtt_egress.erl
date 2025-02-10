%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_egress).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    config/1,
    send/4,
    send_async/5
]).

-type message() :: emqx_types:message() | map().
-type callback() :: {function(), [_Arg]} | {module(), atom(), [_Arg]}.
-type remote_message() :: #mqtt_msg{}.

-type egress() :: #{
    local => #{
        topic => emqx_types:topic()
    },
    remote := emqx_bridge_mqtt_msg:msgvars()
}.

-spec config(map()) ->
    egress().
config(#{remote := RC = #{}} = Conf) ->
    Conf#{remote => emqx_bridge_mqtt_msg:parse(RC)}.

-spec send(pid(), emqx_trace:rendered_action_template_ctx(), message(), egress()) ->
    ok | {error, {unrecoverable_error, term()}}.
send(Pid, TraceRenderedCTX, MsgIn, Egress) ->
    ?tp("mqtt_action_about_to_publish", #{}),
    try
        emqtt:publish(Pid, export_msg(MsgIn, Egress, TraceRenderedCTX))
    catch
        error:{unrecoverable_error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

-spec send_async(pid(), emqx_trace:rendered_action_template_ctx(), message(), callback(), egress()) ->
    {ok, pid()} | {error, {unrecoverable_error, term()}}.
send_async(Pid, TraceRenderedCTX, MsgIn, Callback, Egress) ->
    ?tp("mqtt_action_about_to_publish", #{}),
    try
        ok = emqtt:publish_async(
            Pid, export_msg(MsgIn, Egress, TraceRenderedCTX), _Timeout = infinity, Callback
        ),
        {ok, Pid}
    catch
        error:{unrecoverable_error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

export_msg(Msg, #{remote := Remote}, TraceRenderedCTX) ->
    to_remote_msg(Msg, Remote, TraceRenderedCTX).

-spec to_remote_msg(
    message(), emqx_bridge_mqtt_msg:msgvars(), emqx_trace:rendered_action_template_ctx()
) ->
    remote_message().
to_remote_msg(#message{flags = Flags} = Msg, Vars, TraceRenderedCTX) ->
    {EventMsg, _} = emqx_rule_events:eventmsg_publish(Msg),
    to_remote_msg(EventMsg#{retain => maps:get(retain, Flags, false)}, Vars, TraceRenderedCTX);
to_remote_msg(Msg = #{}, Remote, TraceRenderedCTX) ->
    #{
        topic := Topic,
        payload := Payload,
        qos := QoS,
        retain := Retain
    } = emqx_bridge_mqtt_msg:render(Msg, Remote),
    PubProps = maps:get(pub_props, Msg, #{}),
    emqx_trace:rendered_action_template_with_ctx(TraceRenderedCTX, #{
        qos => QoS,
        retain => Retain,
        topic => Topic,
        props => PubProps,
        payload => Payload
    }),
    #mqtt_msg{
        qos = QoS,
        retain = Retain,
        topic = Topic,
        props = emqx_utils:pub_props_to_packet(PubProps),
        payload = Payload
    }.
