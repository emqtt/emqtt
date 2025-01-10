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
-export([start_link/0, start/1, stop/1]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%%  API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()}
    | {error, {already_started, Pid :: pid()}}
    | {error, {shutdown, term()}}
    | {error, term()}
    | ignore.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start(emqx_limiter:zone()) -> _.
start(Zone) ->
    case has_limiter(Zone) of
        true ->
            Spec = make_child(Zone),
            supervisor:start_child(?MODULE, Spec);
        _ ->
            {error, <<"No Limiter">>}
    end.

stop(Zone) ->
    _ = supervisor:terminate_child(?MODULE, Zone),
    supervisor:delete_child(?MODULE, Zone).

%%--------------------------------------------------------------------
%%  Supervisor callbacks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart intensity, and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}
    | ignore.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },
    {ok, {SupFlags, childs()}}.

%%--==================================================================
%%  Internal functions
%%--==================================================================
make_child(Zone) ->
    #{
        id => Zone,
        start => {emqx_limiter_allocator, start_link, [Zone]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [emqx_limiter_allocator]
    }.

childs() ->
    Zones = maps:keys(emqx_config:get([zones])),
    lists:foldl(
        fun(Zone, Acc) ->
            case has_limiter(Zone) of
                true ->
                    [make_child(Zone) | Acc];
                _ ->
                    Acc
            end
        end,
        [make_child(emqx_limiter:internal_allocator())],
        Zones
    ).

has_limiter(Zone) ->
    case emqx_config:get_zone_conf(Zone, [mqtt, limiter], undefined) of
        undefined ->
            false;
        Cfg ->
            has_any_rate(Cfg)
    end.

has_any_rate(Cfg) ->
    Names = emqx_limiter_schema:mqtt_limiter_names(),
    lists:any(
        fun(Name) ->
            {ok, RateKey} = emqx_limiter:to_rate_key(Name),
            maps:get(RateKey, Cfg, infinity) =/= infinity
        end,
        Names
    ).
