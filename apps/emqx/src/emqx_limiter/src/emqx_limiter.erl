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

-module(emqx_limiter).

%% API
-export([
    check/2,
    restore/2
]).

-export([
    get_config/2,
    filter_limiter_fields/2
]).

-export([
    internal_allocator/0,
    default_alloc_interval/0,
    calc_capacity/1,
    calc_capacity/2
]).

-export_type([type/0, limiter/0, zone/0, limiter_name/0]).

-type type() :: private | shared | infinity.
-type limiter() :: #{
    type := type(),
    module := module(),
    atom() => term()
}.

-type zone() :: atom().
-type limiter_name() :: max_conn | messages | bytes.

-define(DEFAULT_ALLOC_INTERVAL, 100).

%%--------------------------------------------------------------------
%%  call backs
%%--------------------------------------------------------------------

-callback check(non_neg_integer(), limiter()) -> {boolean(), limiter()}.
-callback restore(non_neg_integer(), limiter()) -> limiter().

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
%% @doc checks if the limiter has enough tokens, and consumes them if so
check(_Need, undefined) ->
    {true, undefined};
check(Need, #{module := Mod} = Limiter) ->
    Mod:check(Need, Limiter).

%% @doc restore the token when necessary.
%% For example, if a limiter container has two limiters `a` and `b`,
%% and `a `succeeds but `b` fails, then we should return the tokens to `a`.
restore(Consumed, #{module := Mod} = Limiter) ->
    Mod:restore(Consumed, Limiter).

%% @doc get the config of a limiter from a config map of different parameters.
%%
%% The convention is as follows:
%% Limiter with name `x` is configured with `x_rate` and `x_burst` keys in a config map.
%%
%% Having a config like `Config = #{foo => bar,  x_rate => 10, x_burst => 100}`
%% means that the limiter `x` has a rate of 10 tokens per interval and a burst of 100 tokens.
%%
%% The `get_config(x, Config)` function will return
%% limiter config `#{rate => 10, burst => 100}` for the limiter `x`.
%%
%% If the limiter `x` is not configured, the function will return `undefined`.
get_config(Name, Cfg) ->
    {ok, RateKey} = to_rate_key(Name),
    case maps:get(RateKey, Cfg, infinity) of
        infinity ->
            undefined;
        Rate ->
            {ok, BurstKey} = to_burst_key(Name),
            Burst = maps:get(BurstKey, Cfg, 0),
            #{rate => Rate, burst => Burst}
    end.

%% @doc filter limiter-related fields from a config map.
%%
%% E.g. `filter_limiter_fields([x, y], #{x_rate => 10, y_burst => 20, a => 1, b => 2})`
%% will return `#{x_rate => 10, y_burst => 20}`.
filter_limiter_fields(Names, Cfg) ->
    Keys = lists:foldl(
        fun(Name, Acc) ->
            {ok, RateKey} = to_rate_key(Name),
            {ok, BurstKey} = to_burst_key(Name),
            [RateKey, BurstKey | Acc]
        end,
        [],
        Names
    ),
    maps:with(Keys, Cfg).

internal_allocator() ->
    <<"internal_allocator">>.

default_alloc_interval() ->
    ?DEFAULT_ALLOC_INTERVAL.

%% Capacity = rate * interval
%% but if interval is less than 1 second, use 1 second instead of interval,
%% so we can ensure that capacity is at least greater than 1
calc_capacity(Rate) ->
    calc_capacity(Rate, default_alloc_interval()).

calc_capacity(Rate, Interval) ->
    erlang:ceil(Rate * erlang:max(Interval, 1000)).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

to_rate_key(Name) ->
    NameStr = emqx_utils_conv:str(Name),
    emqx_utils:safe_to_existing_atom(NameStr ++ "_rate").

to_burst_key(Name) ->
    NameStr = emqx_utils_conv:str(Name),
    emqx_utils:safe_to_existing_atom(NameStr ++ "_burst").
