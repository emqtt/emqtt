%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_management_proto_v6).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,

    node_info/1,
    broker_info/1,
    list_subscriptions/1,

    list_listeners/1,
    subscribe/3,
    subscribe/4,
    unsubscribe/3,
    unsubscribe/4,
    unsubscribe_batch/3,
    unsubscribe_batch/4,

    call_client/3,
    call_client/4,

    get_full_config/1,

    kickout_clients/2,
    kickout_clients/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.8.0".

-spec unsubscribe_batch(node(), emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe_batch(Node, ClientId, Topics) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe_batch, [ClientId, Topics]).

-spec unsubscribe_batch(node(), emqx_types:mtns(), emqx_types:clientid(), [emqx_types:topic()]) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe_batch(Node, Mtns, ClientId, Topics) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe_batch, [Mtns, ClientId, Topics]).

-spec node_info([node()]) -> emqx_rpc:erpc_multicall(map()).
node_info(Nodes) ->
    erpc:multicall(Nodes, emqx_mgmt, node_info, [], 30000).

-spec broker_info([node()]) -> emqx_rpc:erpc_multicall(map()).
broker_info(Nodes) ->
    erpc:multicall(Nodes, emqx_mgmt, broker_info, [], 30000).

-spec list_subscriptions(node()) -> [map()] | {badrpc, _}.
list_subscriptions(Node) ->
    rpc:call(Node, emqx_mgmt, do_list_subscriptions, []).

-spec list_listeners(node()) -> map() | {badrpc, _}.
list_listeners(Node) ->
    rpc:call(Node, emqx_mgmt_api_listeners, do_list_listeners, []).

-spec subscribe(node(), emqx_types:clientid(), emqx_types:topic_filters()) ->
    {subscribe, _} | {error, atom()} | {badrpc, _}.
subscribe(Node, ClientId, TopicTables) ->
    rpc:call(Node, emqx_mgmt, do_subscribe, [ClientId, TopicTables]).

-spec subscribe(node(), emqx_types:mtns(), emqx_types:clientid(), emqx_types:topic_filters()) ->
    {subscribe, _} | {error, atom()} | {badrpc, _}.
subscribe(Node, Mtns, ClientId, TopicTables) ->
    rpc:call(Node, emqx_mgmt, do_subscribe, [Mtns, ClientId, TopicTables]).

-spec unsubscribe(node(), emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe(Node, ClientId, Topic) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe, [ClientId, Topic]).

-spec unsubscribe(node(), emqx_types:mtns(), emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, _} | {badrpc, _}.
unsubscribe(Node, Mtns, ClientId, Topic) ->
    rpc:call(Node, emqx_mgmt, do_unsubscribe, [Mtns, ClientId, Topic]).

-spec call_client([node()], emqx_types:clientid(), term()) -> emqx_rpc:erpc_multicall(term()).
call_client(Nodes, ClientId, Req) ->
    erpc:multicall(Nodes, emqx_mgmt, do_call_client, [ClientId, Req], 30000).

-spec call_client([node()], emqx_types:mtns(), emqx_types:clientid(), term()) ->
    emqx_rpc:erpc_multicall(term()).
call_client(Nodes, Mtns, ClientId, Req) ->
    erpc:multicall(Nodes, emqx_mgmt, do_call_client, [Mtns, ClientId, Req], 30000).

-spec get_full_config(node()) -> map() | list() | {badrpc, _}.
get_full_config(Node) ->
    rpc:call(Node, emqx_mgmt_api_configs, get_full_config, []).

-spec kickout_clients(node(), [emqx_types:clientid()]) -> ok | {badrpc, _}.
kickout_clients(Node, ClientIds) ->
    rpc:call(Node, emqx_mgmt, do_kickout_clients, [ClientIds]).

-spec kickout_clients(node(), emqx_types:mtns(), [emqx_types:clientid()]) -> ok | {badrpc, _}.
kickout_clients(Node, Mtns, ClientIds) ->
    rpc:call(Node, emqx_mgmt, do_kickout_clients, [Mtns, ClientIds]).
