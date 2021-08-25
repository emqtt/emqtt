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

%% @doc Gateway Interface Module for HTTP-APIs
-module(emqx_gateway_intr).

-export([ gateways/1
        ]).

-type gateway_summary() ::
        #{ name := binary()
         , status := running | stopped | unloaded
         , started_at => binary()
         , max_connection => integer()
         , current_connect => integer()
         , listeners => []
         }.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec gateways(Status :: all | running | stopped | unloaded)
    -> [gateway_summary()].
gateways(Status) ->
    Gateways = lists:map(fun({GwName, _}) ->
        case emqx_gateway:lookup(GwName) of
            undefined -> #{name => GwName, status => unloaded};
            GwInfo = #{config := Config} ->
                GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                            [created_at, started_at, stopped_at],
                            GwInfo),
                GwInfo1 = maps:with([name,
                                     status,
                                     created_at,
                                     started_at,
                                     stopped_at], GwInfo0),
                GwInfo1#{listeners => get_listeners_status(GwName, Config)}

        end
    end, emqx_gateway_registry:list()),
    case Status of
        all -> Gateways;
        _ ->
            [Gw || Gw = #{status := S} <- Gateways, S == Status]
    end.

%% @private
get_listeners_status(GwName, Config) ->
    Listeners = emqx_gateway_utils:normalize_config(Config),
    lists:map(fun({Type, LisName, ListenOn, _, _}) ->
        Name0 = listener_name(GwName, Type, LisName),
        Name = {Name0, ListenOn},
        case catch esockd:listener(Name) of
            _Pid when is_pid(_Pid) ->
                #{Name0 => <<"activing">>};
            _ ->
                #{Name0 => <<"inactived">>}

        end
    end, Listeners).

%% @private
listener_name(GwName, Type, LisName) ->
    list_to_atom(lists:concat([GwName, ":", Type, ":", LisName])).
