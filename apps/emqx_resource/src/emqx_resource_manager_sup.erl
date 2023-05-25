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
-module(emqx_resource_manager_sup).

-behaviour(supervisor).

-include("emqx_resource.hrl").

-export([ensure_child/5, delete_child/1]).

-export([start_link/0]).

-export([init/1]).

ensure_child(ResId, Group, ResourceType, Config, Opts) ->
    _ = supervisor:start_child(?MODULE, [ResId, Group, ResourceType, Config, Opts]),
    ok.

delete_child(Pid) ->
    _ = supervisor:terminate_child(?MODULE, Pid),
    _ = supervisor:delete_child(?MODULE, Pid),
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Maps resource_id() to one or more allocated resources.
    emqx_utils_ets:new(?RESOURCE_ALLOCATION_TAB, [
        bag,
        public,
        {read_concurrency, true}
    ]),
    ChildSpecs = [
        #{
            id => emqx_resource_manager,
            start => {emqx_resource_manager, start_link, []},
            restart => transient,
            %% never force kill a resource manager.
            %% becasue otherwise it may lead to release leak,
            %% resource_manager's terminate callback calls resource on_stop
            shutdown => infinity,
            type => worker,
            modules => [emqx_resource_manager]
        }
    ],
    SupFlags = #{strategy => simple_one_for_one, intensity => 10, period => 10},
    {ok, {SupFlags, ChildSpecs}}.
