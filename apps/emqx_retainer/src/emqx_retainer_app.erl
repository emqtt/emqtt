%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_app).

-behaviour(application).

-include("emqx_retainer.hrl").

-export([
    start/2,
    stop/1
]).

%% For testing
-export([
    init_buckets/0,
    delete_buckets/0
]).

start(_Type, _Args) ->
    ok = emqx_retainer_cli:load(),
    init_buckets(),
    emqx_retainer_sup:start_link().

stop(_State) ->
    ok = emqx_retainer_cli:unload(),
    delete_buckets(),
    ok.

init_buckets() ->
    case emqx:get_config([retainer, flow_control, batch_deliver_limiter], 0) of
        0 ->
            ok;
        undefined ->
            ok;
        Rate ->
            Cfg = #{rate => Rate, burst => 0},
            emqx_limiter_allocator:add_bucket(?DISPATCHER_LIMITER_ID, Cfg)
    end.

delete_buckets() ->
    emqx_limiter_allocator:delete_bucket(?DISPATCHER_LIMITER_ID).
