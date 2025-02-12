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

-module(emqx_limiter_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%%  API functions
%%--------------------------------------------------------------------

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

    Childs = [
        child_spec(emqx_limiter_bucket_registry, worker),
        child_spec(emqx_limiter_allocator_sup, supervisor)
    ],

    {ok, {SupFlags, Childs}}.

child_spec(Mod, Type) ->
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => transient,
        type => Type,
        modules => [Mod]
    }.
