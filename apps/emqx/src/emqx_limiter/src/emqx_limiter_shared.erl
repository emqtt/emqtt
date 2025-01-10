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

-module(emqx_limiter_shared).

-behaviour(emqx_limiter).

%% API
-export([create/1, check/2, restore/2]).
-export_type([limiter/0]).

-type bucket() :: emqx_limiter_bucket_ref:bucket_ref().

-type limiter() :: #{
    module := ?MODULE,
    bucket := bucket(),
    %% allow to add other keys
    atom => any()
}.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create(bucket()) -> limiter().
create(Bucket) ->
    #{
        module => ?MODULE,
        bucket => Bucket
    }.

check(Need, #{bucket := Bucket} = Limiter) ->
    {emqx_limiter_bucket_ref:check(Need, Bucket), Limiter}.

restore(Consumed, #{bucket := Bucket} = Limiter) ->
    emqx_limiter_bucket_ref:restore(Consumed, Bucket),
    Limiter.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
