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

%% @doc This module implements the private limiter.
%%
%% Private limiter is a limiter that is not shared between different processes.
%% I.e. different processes do not consume tokens concurrently.
%%
%% This is a more simple version of a limiter because its own toket bucket
%% is refilled algorithmically, without the help of the external emqx_limiter_allocator.

-module(emqx_limiter_private).

-behaviour(emqx_limiter).

%% API
-export([create/1, check/2, restore/2]).
-export_type([limiter/0]).

-type millisecond() :: non_neg_integer().

-type limiter() :: #{
    module := ?MODULE,
    tokens := number(),
    rate := number(),
    burst := number(),
    capacity := number(),
    lasttime := millisecond(),
    atom => any()
}.

-define(NOW, erlang:monotonic_time(millisecond)).
-define(MINIMUM_INTERVAL, 10).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create(emqx_config:config()) -> limiter().
create(#{rate := Rate, burst := Burst}) ->
    Capacity = emqx_limiter:calc_capacity(Rate),
    #{
        module => ?MODULE,
        tokens => Capacity,
        rate => Rate,
        burst => Burst,
        capacity => Capacity,
        lasttime => ?NOW
    }.

check(Need, Limiter) ->
    do_check(Need, Limiter).

restore(Consumed, #{tokens := Tokens, capacity := Capacity} = Limiter) ->
    New = erlang:min(Tokens + Consumed, Capacity),
    Limiter#{tokens := New}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
do_check(Need, #{tokens := Tokens} = Limiter) when Need =< Tokens ->
    {true, Limiter#{tokens := Tokens - Need}};
do_check(
    Need,
    #{
        tokens := Tokens,
        rate := Rate,
        lasttime := LastTime,
        capacity := Capacity
    } = Limiter
) ->
    Now = ?NOW,
    case Now >= LastTime + ?MINIMUM_INTERVAL of
        false ->
            {false, Limiter};
        _ ->
            Inc = apply_elapsed_time(Rate, Now - LastTime),
            Tokens2 = erlang:min(Capacity, Tokens + Inc),
            Limiter2 = Limiter#{tokens := Tokens2, lasttime := Now},
            case Tokens2 >= Need of
                true ->
                    {true, Limiter2#{tokens := Tokens2 - Need}};
                _ ->
                    {false, Limiter2}
            end
    end.

%% @doc apply the elapsed time to the limiter
apply_elapsed_time(_Rate, 0) ->
    0;
apply_elapsed_time(Rate, Elapsed) ->
    Rate * Elapsed.
