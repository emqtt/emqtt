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

-module(emqx_limiter_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-export([
    to_rate/1,
    to_capacity/1,
    to_burst/1,
    to_burst_rate/1,
    to_initial/1,
    rate_type/0
]).

-export([
    mqtt_limiter_names/0
]).

-define(KILOBYTE, 1024).

-type rate() :: infinity | number().
-type burst_rate() :: number().
%% this is a compatible type for the deprecated field and type `capacity`.
-type burst() :: burst_rate().

%% the processing strategy after the failure of the token request
-typerefl_from_string({rate/0, ?MODULE, to_rate}).
-typerefl_from_string({burst_rate/0, ?MODULE, to_burst_rate}).
-typerefl_from_string({burst/0, ?MODULE, to_burst}).

-reflect_type([
    rate/0,
    burst_rate/0,
    burst/0
]).

%%--------------------------------------------------------------------
%% schema
%%--------------------------------------------------------------------

namespace() -> limiter.

roots() ->
    [].

fields(mqtt_with_interval) ->
    fields(mqtt) ++
        [
            {alloc_interval,
                ?HOCON(emqx_schema:duration_ms(), #{
                    desc => ?DESC(alloc_interval),
                    default => emqx_limiter:default_alloc_interval(),
                    importance => ?IMPORTANCE_LOW
                })}
        ];
fields(mqtt) ->
    lists:foldl(fun make_mqtt_limiters_schema/2, [], mqtt_limiter_names()).

desc(mqtt_with_interval) ->
    ?DESC(mqtt);
desc(mqtt) ->
    ?DESC(mqtt);
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
mqtt_limiter_names() ->
    [
        max_conn,
        messages,
        bytes
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
to_rate(Str) ->
    to_rate(Str, true, false).

to_burst_rate(Str) ->
    to_rate(Str, false, true).

%% The default value of `capacity` is `infinity`,
%% but we have changed `capacity` to `burst` which should not be `infinity`
%% and its default value is 0, so we should convert `infinity` to 0
to_burst(Str) ->
    case to_rate(Str, true, true) of
        {ok, infinity} ->
            {ok, 0};
        Any ->
            Any
    end.

%% rate can be: 10 10MB 10MB/s 10MB/2s infinity
%% e.g. the bytes_in regex tree is:
%%
%%        __ infinity
%%        |                 - xMB
%%  rate -|                 |
%%        __ ?Size(/?Time) -|            - xMB/s
%%                          |            |
%%                          - xMB/?Time -|
%%                                       - xMB/ys
to_rate(Str, CanInfinity, CanZero) ->
    Regex = "^\s*(?:([0-9]+[a-zA-Z]*)(?:/([0-9]*)([m s h d M S H D]{1,2}))?\s*$)|infinity\s*$",
    {ok, MP} = re:compile(Regex),
    case re:run(Str, MP, [{capture, all_but_first, list}]) of
        {match, []} when CanInfinity ->
            {ok, infinity};
        %% if time unit is 1s, it can be omitted
        {match, [QuotaStr]} ->
            Fun = fun(Quota) ->
                {ok, erlang:float(Quota) / 1000}
            end,
            to_capacity(QuotaStr, Str, CanZero, Fun);
        {match, [QuotaStr, TimeVal, TimeUnit]} ->
            Interval =
                case TimeVal of
                    %% for xM/s
                    [] -> "1" ++ TimeUnit;
                    %% for xM/ys
                    _ -> TimeVal ++ TimeUnit
                end,
            Fun = fun(Quota) ->
                try
                    case emqx_schema:to_duration_ms(Interval) of
                        {ok, Ms} when Ms > 0 ->
                            {ok, erlang:float(Quota) / Ms};
                        {ok, 0} when CanZero ->
                            {ok, 0};
                        _ ->
                            {error, Str}
                    end
                catch
                    _:_ ->
                        {error, Str}
                end
            end,
            to_capacity(QuotaStr, Str, CanZero, Fun);
        _ ->
            {error, Str}
    end.

to_capacity(QuotaStr, Str, CanZero, Fun) ->
    case to_capacity(QuotaStr) of
        {ok, Val} -> check_capacity(Str, Val, CanZero, Fun);
        {error, _Error} -> {error, Str}
    end.

check_capacity(_Str, 0, true, Cont) ->
    %% must check the interval part or maybe will get incorrect config, e.g. "0/0sHello"
    Cont(0);
check_capacity(Str, 0, false, _Cont) ->
    {error, Str};
check_capacity(_Str, Quota, _CanZero, Cont) ->
    Cont(Quota).

to_capacity(Str) ->
    Regex = "^\s*(?:([0-9]+)([a-zA-Z]*))|infinity\s*$",
    to_quota(Str, Regex).

to_initial(Str) ->
    Regex = "^\s*([0-9]+)([a-zA-Z]*)\s*$",
    to_quota(Str, Regex).

to_quota(Str, Regex) ->
    {ok, MP} = re:compile(Regex),
    try
        Result = re:run(Str, MP, [{capture, all_but_first, list}]),
        case Result of
            {match, [Quota, Unit]} ->
                Val = erlang:list_to_integer(Quota),
                Unit2 = string:to_lower(Unit),
                apply_unit(Unit2, Val);
            {match, [Quota, ""]} ->
                {ok, erlang:list_to_integer(Quota)};
            {match, ""} ->
                {ok, infinity};
            _ ->
                {error, Str}
        end
    catch
        _:Error ->
            {error, Error}
    end.

apply_unit("", Val) -> {ok, Val};
apply_unit("kb", Val) -> {ok, Val * ?KILOBYTE};
apply_unit("mb", Val) -> {ok, Val * ?KILOBYTE * ?KILOBYTE};
apply_unit("gb", Val) -> {ok, Val * ?KILOBYTE * ?KILOBYTE * ?KILOBYTE};
apply_unit(Unit, _) -> {error, "invalid unit:" ++ Unit}.

make_mqtt_limiters_schema(Name, Schemas) ->
    NameStr = erlang:atom_to_list(Name),
    Rate = erlang:list_to_atom(NameStr ++ "_rate"),
    Burst = erlang:list_to_atom(NameStr ++ "_burst"),
    [
        {Rate,
            ?HOCON(rate_type(), #{
                desc => ?DESC(Rate),
                required => false
            })},
        {Burst,
            ?HOCON(burst_rate_type(), #{
                desc => ?DESC(burst),
                required => false
            })}
        | Schemas
    ].

rate_type() ->
    typerefl:alias("string", rate()).

burst_rate_type() ->
    typerefl:alias("string", burst_rate()).
