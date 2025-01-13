%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_esockd_limiter).

-behaviour(esockd_generic_limiter).

-define(LIMITER_CONNECTION, max_conn).

%% API
-export([new_create_options/1, create/1, delete/1, consume/2]).

-type create_options() :: #{
    module := ?MODULE,
    limiter := emqx_limiter_container:container()
}.

-define(PAUSE_INTERVAL, 100).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec new_create_options(emqx_limiter_container:container()) -> create_options().
new_create_options(Container) ->
    #{module => ?MODULE, limiter => Container}.

-spec create(create_options()) -> esockd_generic_limiter:limiter().
create(#{module := ?MODULE, limiter := _Container} = Opts) ->
    Opts.

delete(_GLimiter) ->
    ok.

consume(Token, #{limiter := Container} = GLimiter) ->
    case emqx_limiter_container:check([{?LIMITER_CONNECTION, Token}], Container) of
        {true, Container2} ->
            {ok, GLimiter#{limiter := Container2}};
        {false, Container2} ->
            {pause, ?PAUSE_INTERVAL, GLimiter#{limiter := Container2}}
    end.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
