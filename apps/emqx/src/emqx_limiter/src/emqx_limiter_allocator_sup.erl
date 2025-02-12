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

-module(emqx_limiter_allocator_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%%  API functions
%%--------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%%  Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },
    {ok, {SupFlags, child_specs()}}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

child_spec(Zone) ->
    #{
        id => Zone,
        start => {emqx_limiter_allocator, start_link, [Zone]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [emqx_limiter_allocator]
    }.

child_specs() ->
    Zones = maps:keys(emqx_config:get([zones])),
    lists:foldl(
        fun(Zone, Acc) ->
            case has_limiter(Zone) of
                true ->
                    [child_spec(Zone) | Acc];
                _ ->
                    Acc
            end
        end,
        [child_spec(emqx_limiter:default_allocator())],
        Zones
    ).

has_limiter(Zone) ->
    case emqx_config:get_zone_conf(Zone, [mqtt, limiter], undefined) of
        undefined ->
            false;
        Cfg ->
            has_any_limiters_configured(Cfg)
    end.

has_any_limiters_configured(Cfg) ->
    Names = emqx_limiter_schema:mqtt_limiter_names(),
    lists:any(
        fun(Name) ->
            emqx_limiter:get_config(Name, Cfg) =/= undefined
        end,
        Names
    ).
