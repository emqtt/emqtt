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

-module(emqx_limiter_bucket_ref).

%% @doc Limiter bucket reference
%%
%% Term `bucket' referes to the Token Bucket algorithm.
%% Limiter buckets are created/removed and refilled
%% by the allocator server `emqx_limiter_allocator`
%%
%% This module exposes
%% * a "handle" structure to reference a particular bucket
%% * functions to consume and restore tokens from/to the bucket by this handle

%% API
-export([
    new/2,
    check/2,
    restore/2,
    available/1
]).

-export_type([bucket_ref/0]).

-type bucket_ref() :: #{
    counter := counters:counters_ref(),
    index := emqx_limiter_allocator:index()
}.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec new(counters:counters_ref(), emqx_limiter_allocator:index()) -> bucket_ref().
new(Counter, Index) ->
    #{
        counter => Counter,
        index => Index
    }.

%% @doc check tokens
-spec check(pos_integer(), bucket_ref()) -> boolean().
check(
    Need,
    #{
        counter := Counter,
        index := Index
    }
) ->
    case counters:get(Counter, Index) of
        AvailableTokens when AvailableTokens >= Need ->
            counters:sub(Counter, Index, Need),
            true;
        _ ->
            false
    end.

%% @doc try to restore token when consume failed
-spec restore(non_neg_integer(), bucket_ref()) -> ok.
restore(0, _) ->
    ok;
restore(Inc, #{counter := Counter, index := Index}) ->
    counters:add(Counter, Index, Inc).

%% @doc get the number of tokens currently available
-spec available(bucket_ref()) -> integer().
available(#{counter := Counter, index := Index}) ->
    counters:get(Counter, Index).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
