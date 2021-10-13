%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The MQTT-SN Gateway Implement interface
-module(emqx_sn_impl).

-behavior(emqx_gateway_impl).

%% APIs
-export([ reg/0
        , unreg/0
        ]).

-export([ on_gateway_load/2
        , on_gateway_update/3
        , on_gateway_unload/2
        ]).

-include_lib("emqx/include/logger.hrl").

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

reg() ->
    RegistryOptions = [ {cbkmod, ?MODULE}
                      ],
    emqx_gateway_registry:reg(mqttsn, RegistryOptions).

unreg() ->
    emqx_gateway_registry:unreg(mqttsn).

%%--------------------------------------------------------------------
%% emqx_gateway_registry callbacks
%%--------------------------------------------------------------------

on_gateway_load(_Gateway = #{ name := GwName,
                              config := Config
                            }, Ctx) ->

    %% We Also need to start `emqx_sn_broadcast` &
    %% `emqx_sn_registry` process
    case maps:get(broadcast, Config, false) of
        false ->
            ok;
        true ->
            %% FIXME:
            Port = 1884,
            SnGwId = maps:get(gateway_id, Config, undefined),
            _ = emqx_sn_broadcast:start_link(SnGwId, Port), ok
    end,

    PredefTopics = maps:get(predefined, Config, []),
    {ok, RegistrySvr} = emqx_sn_registry:start_link(GwName, PredefTopics),

    NConfig = maps:without(
                 [broadcast, predefined],
                 Config#{registry => emqx_sn_registry:lookup_name(RegistrySvr)}
                ),
    Listeners = emqx_gateway_utils:normalize_config(NConfig),

    ListenerPids = lists:map(fun(Lis) ->
                     start_listener(GwName, Ctx, Lis)
                   end, Listeners),
    {ok, ListenerPids, _InstaState = #{ctx => Ctx}}.

on_gateway_update(Config, Gateway, GwState = #{ctx := Ctx}) ->
    GwName = maps:get(name, Gateway),
    try
        %% XXX: 1. How hot-upgrade the changes ???
        %% XXX: 2. Check the New confs first before destroy old instance ???
        on_gateway_unload(Gateway, GwState),
        on_gateway_load(Gateway#{config => Config}, Ctx)
    catch
        Class : Reason : Stk ->
            logger:error("Failed to update ~ts; "
                         "reason: {~0p, ~0p} stacktrace: ~0p",
                         [GwName, Class, Reason, Stk]),
            {error, {Class, Reason}}
    end.

on_gateway_unload(_Gateway = #{ name := GwName,
                                config := Config
                              }, _GwState) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:foreach(fun(Lis) ->
        stop_listener(GwName, Lis)
    end, Listeners).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

start_listener(GwName, Ctx, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) of
        {ok, Pid} ->
            ?ULOG("Gateway ~ts:~ts:~ts on ~ts started.~n",
                  [GwName, Type, LisName, ListenOnStr]),
            Pid;
        {error, Reason} ->
            ?ELOG("Failed to start gateway ~ts:~ts:~ts on ~ts: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason]),
            throw({badconf, Reason})
    end.

start_listener(GwName, Ctx, Type, LisName, ListenOn, SocketOpts, Cfg) ->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    NCfg = Cfg#{
             ctx => Ctx,
             listene => {GwName, Type, LisName},
             frame_mod => emqx_sn_frame,
             chann_mod => emqx_sn_channel
            },
    esockd:open_udp(Name, ListenOn, merge_default(SocketOpts),
                    {emqx_gateway_conn, start_link, [NCfg]}).

merge_default(Options) ->
    Default = emqx_gateway_utils:default_udp_options(),
    case lists:keytake(udp_options, 1, Options) of
        {value, {udp_options, TcpOpts}, Options1} ->
            [{udp_options, emqx_misc:merge_opts(Default, TcpOpts)}
             | Options1];
        false ->
            [{udp_options, Default} | Options]
    end.

stop_listener(GwName, {Type, LisName, ListenOn, SocketOpts, Cfg}) ->
    StopRet = stop_listener(GwName, Type, LisName, ListenOn, SocketOpts, Cfg),
    ListenOnStr = emqx_gateway_utils:format_listenon(ListenOn),
    case StopRet of
        ok -> ?ULOG("Gateway ~ts:~ts:~ts on ~ts stopped.~n",
                    [GwName, Type, LisName, ListenOnStr]);
        {error, Reason} ->
            ?ELOG("Failed to stop gateway ~ts:~ts:~ts on ~ts: ~0p~n",
                  [GwName, Type, LisName, ListenOnStr, Reason])
    end,
    StopRet.

stop_listener(GwName, Type, LisName, ListenOn, _SocketOpts, _Cfg) ->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    esockd:close(Name, ListenOn).
