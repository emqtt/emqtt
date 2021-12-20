%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_schema).

-include_lib("typerefl/include/types.hrl").

-export([ roots/0, fields/1, to_rate/1, to_capacity/1
        , minimum_period/0, to_burst_rate/1, to_initial/1]).

-define(KILOBYTE, 1024).

-type limiter_type() :: bytes_in
                      | message_in
                      | connection
                      | message_routing.

-type bucket_name() :: atom().
-type zone_name() :: atom().
-type rate() :: infinity | float().
-type burst_rate() :: 0 | float().
-type capacity() :: infinity | number().    %% the capacity of the token bucket
-type initial() :: non_neg_integer().       %% initial capacity of the token bucket

%% the processing strategy after the failure of the token request
-type failure_strategy() :: force           %% Forced to pass
                          | drop            %% discard the current request
                          | throw.          %% throw an exception

-typerefl_from_string({rate/0, ?MODULE, to_rate}).
-typerefl_from_string({burst_rate/0, ?MODULE, to_burst_rate}).
-typerefl_from_string({capacity/0, ?MODULE, to_capacity}).
-typerefl_from_string({initial/0, ?MODULE, to_initial}).

-reflect_type([ rate/0
              , burst_rate/0
              , capacity/0
              , initial/0
              , failure_strategy/0
              ]).

-export_type([limiter_type/0, bucket_name/0, zone_name/0]).

-import(emqx_schema, [sc/2, map/2]).

roots() -> [emqx_limiter].

fields(emqx_limiter) ->
    [ {bytes_in, sc(ref(limiter), #{})}
    , {message_in, sc(ref(limiter), #{})}
    , {connection, sc(ref(limiter), #{})}
    , {message_routing, sc(ref(limiter), #{})}
    ];

fields(limiter) ->
    [ {global, sc(ref(rate_burst), #{})}
    , {zone, sc(map("zone name", ref(rate_burst)), #{})}
    , {bucket, sc(map("bucket id", ref(bucket)),
                  #{desc => "token bucket"})}
    ];

fields(rate_burst) ->
    [ {rate, sc(rate(), #{})}
    , {burst, sc(burst_rate(), #{default => "0/0s"})}
    ];

fields(bucket) ->
    [ {zone, sc(atom(), #{desc => "the zone which the bucket in"})}
    , {aggregated, sc(ref(bucket_aggregated), #{})}
    , {per_client, sc(ref(client_bucket), #{})}
    ];

fields(bucket_aggregated) ->
    [ {rate, sc(rate(), #{})}
    , {initial, sc(initial(), #{default => "0"})}
    , {capacity, sc(capacity(), #{})}
    ];

fields(client_bucket) ->
    [ {rate, sc(rate(), #{})}
    , {initial, sc(initial(), #{default => "0"})}
      %% low_water_mark add for emqx_channel and emqx_session
      %% both modules consume first and then check
      %% so we need to use this value to prevent excessive consumption (e.g, consumption from an empty bucket)
    , {low_water_mark, sc(initial(),
                          #{desc => "if the remaining tokens are lower than this value,
the check/consume will succeed, but it will be forced to hang for a short period of time",
                            default => "0"})}
    , {capacity, sc(capacity(), #{desc => "the capacity of the token bucket"})}
    , {divisible, sc(boolean(),
                     #{desc => "is it possible to split the number of tokens requested",
                       default => false})}
    , {max_retry_time, sc(emqx_schema:duration(),
                          #{ desc => "the maximum retry time when acquire failed"
                           , default => "5s"})}
    , {failure_strategy, sc(failure_strategy(),
                            #{ desc => "the strategy when all retry failed"
                             , default => force})}
    ].

%% minimum period is 100ms
minimum_period() ->
    100.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
ref(Field) -> hoconsc:ref(?MODULE, Field).

to_rate(Str) ->
    to_rate(Str, true, false).

to_burst_rate(Str) ->
    to_rate(Str, false, true).

to_rate(Str, CanInfinity, CanZero) ->
    Tokens = [string:trim(T) || T <- string:tokens(Str, "/")],
    case Tokens of
        ["infinity"] when CanInfinity ->
            {ok, infinity};
        ["0", _] when CanZero ->
            {ok, 0}; %% for burst
        [Quota, Interval] ->
            {ok, Val} = to_capacity(Quota),
            case emqx_schema:to_duration_ms(Interval) of
                {ok, Ms} when Ms > 0 ->
                    {ok, Val * minimum_period() / Ms};
                _ ->
                    {error, Str}
            end;
        _ ->
            {error, Str}
    end.

to_capacity(Str) ->
    Regex = "^\s*(?:(?:([1-9][0-9]*)([a-zA-z]*))|infinity)\s*$",
    to_quota(Str, Regex).

to_initial(Str) ->
    Regex = "^\s*([0-9]+)([a-zA-z]*)\s*$",
    to_quota(Str, Regex).

to_quota(Str, Regex) ->
    {ok, MP} = re:compile(Regex),
    Result = re:run(Str, MP, [{capture, all_but_first, list}]),
    case Result of
        {match, [Quota, Unit]} ->
            Val = erlang:list_to_integer(Quota),
            Unit2 = string:to_lower(Unit),
            {ok, apply_unit(Unit2, Val)};
        {match, [Quota]} ->
            {ok, erlang:list_to_integer(Quota)};
        {match, []} ->
            {ok, infinity};
        _ ->
            {error, Str}
    end.

apply_unit("", Val) -> Val;
apply_unit("kb", Val) -> Val * ?KILOBYTE;
apply_unit("mb", Val) -> Val * ?KILOBYTE * ?KILOBYTE;
apply_unit("gb", Val) -> Val * ?KILOBYTE * ?KILOBYTE * ?KILOBYTE;
apply_unit(Unit, _) -> throw("invalid unit:" ++ Unit).
