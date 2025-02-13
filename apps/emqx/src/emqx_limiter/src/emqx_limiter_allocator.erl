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

%% Handle limiters for a zone.
%% Currently, each allocator corresponds to one zone,
%% if there are thousands of limiters, we can change to using allocator pools

-module(emqx_limiter_allocator).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    add_bucket/2, add_bucket/3,
    delete_bucket/1, delete_bucket/2
]).

-export([start_link/1]).

-type bucket() :: #{
    name := bucket_name(),
    rate := rate(),
    burst => rate(),
    capacity := capacity(),
    counter := counters:counters_ref(),
    last_used_index := index(),
    correction := float()
}.

-type allocator_name() :: emqx_limiter:zone() | binary().

-type state() :: #{
    name := allocator_name(),
    buckets := buckets(),
    counter := counters:counters_ref(),
    last_used_index := index(),
    alloc_interval := millisecond(),
    last_alloc_time := millisecond(),
    free_indices := [index()]
}.

-type buckets() :: #{bucket_name() => bucket()}.
-type bucket_name() :: atom().
-type rate() :: number().
-type millisecond() :: non_neg_integer().
-type capacity() :: number().
-type index() :: pos_integer().

-define(COUNTER_SIZE, 8).
-define(VIA_GPROC(Id), {via, gproc, {n, l, Id}}).

-export_type([allocator_name/0, state/0, index/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(allocator_name()) -> _.
start_link(Name) ->
    gen_server:start_link(?VIA_GPROC(Name), ?MODULE, [Name], []).

add_bucket(Name, Cfg) ->
    add_bucket(emqx_limiter:default_allocator(), Name, Cfg).

add_bucket(AllocatorName, Name, Cfg) ->
    gen_server:call(?VIA_GPROC(AllocatorName), {?FUNCTION_NAME, Name, Cfg}).

delete_bucket(Name) ->
    delete_bucket(emqx_limiter:default_allocator(), Name).

delete_bucket(AllocatorName, Name) ->
    gen_server:call(?VIA_GPROC(AllocatorName), {?FUNCTION_NAME, Name}).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------

-spec init([allocator_name()]) -> {ok, State :: state()}.
init([Name]) ->
    State = init_state(Name),
    tick_alloc_event(State),
    {ok, State}.

handle_call(
    {add_bucket, Name, #{rate := Rate, burst := Burst}},
    _From,
    #{
        name := AllocatorName,
        counter := Counter,
        last_used_index := Index,
        alloc_interval := Interval,
        buckets := Buckets,
        free_indices := Free
    } = State
) ->
    case Free of
        [ToUse | Free2] ->
            NewIndex = Index;
        [] ->
            ToUse = Index + 1,
            NewIndex = ToUse,
            Free2 = Free
    end,

    case ToUse > ?COUNTER_SIZE of
        false ->
            Bucket = do_create_bucket(
                Name, Rate, Burst, AllocatorName, Counter, Interval, ToUse
            ),

            {reply, ok, State#{
                buckets := Buckets#{Name => Bucket},
                last_used_index := NewIndex,
                free_indices := Free2
            }};
        _ ->
            {reply, {error, exceeded_max_index}, State}
    end;
handle_call(
    {delete_bucket, Name},
    _From,
    #{name := AllocatorName, buckets := Buckets, free_indices := Free} = State
) ->
    case maps:take(Name, Buckets) of
        error ->
            {reply, ok, State};
        {#{index := Index}, Buckets2} ->
            emqx_limiter_bucket_registry:delete_bucket(AllocatorName, Name),
            {reply, ok, State#{buckets := Buckets2, free_indices := [Index | Free]}}
    end;
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(tick_alloc_event, State) ->
    {noreply, do_alloc(State)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "emqx_limiter_allocator_unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{name := Name, buckets := Buckets} = _State) ->
    maps:foreach(
        fun(LimiterName, _) ->
            emqx_limiter_bucket_registry:delete_bucket(Name, LimiterName)
        end,
        Buckets
    ),
    ok.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
tick_alloc_event(#{alloc_interval := Interval}) ->
    erlang:send_after(Interval, self(), ?FUNCTION_NAME).

%% an allocator server with a zone name is used for this zone, or it is a dynamic server
init_state(Name) when is_binary(Name) ->
    Counter = counters:new(?COUNTER_SIZE, [write_concurrency]),
    #{
        name => Name,
        counter => Counter,
        buckets => #{},
        last_used_index => 0,
        alloc_interval => emqx_limiter:default_alloc_interval(),
        last_alloc_time => now_ms(),
        free_indices => []
    };
init_state(Zone) when is_atom(Zone) ->
    Cfg = emqx_config:get_zone_conf(Zone, [mqtt, limiter]),
    init_state(Zone, Cfg).

init_state(Zone, #{alloc_interval := Interval} = Cfg) ->
    Counter = counters:new(?COUNTER_SIZE, [write_concurrency]),
    Names = emqx_limiter_schema:mqtt_limiter_names(),
    Buckets = init_buckets(Names, Zone, Counter, Cfg, #{}),
    #{
        name => Zone,
        counter => Counter,
        buckets => Buckets,
        last_used_index => maps:size(Buckets),
        alloc_interval => Interval,
        last_alloc_time => now_ms(),
        free_indices => []
    }.

init_buckets([Name | Names], Zone, Counter, #{alloc_interval := Interval} = Cfg, Buckets) ->
    case emqx_limiter:get_config(Name, Cfg) of
        undefined ->
            init_buckets(Names, Zone, Counter, Cfg, Buckets);
        #{rate := Rate, burst := Burst} ->
            Bucket = do_create_bucket(
                Name, Rate, Burst, Zone, Counter, Interval, maps:size(Buckets) + 1
            ),
            init_buckets(Names, Zone, Counter, Cfg, Buckets#{Name => Bucket})
    end;
init_buckets([], _Zone, _Counter, _, Buckets) ->
    Buckets.

%% @doc generate tokens, and then spread to leaf nodes
-spec do_alloc(state()) -> state().
do_alloc(
    #{
        last_alloc_time := LastTime,
        buckets := Buckets
    } = State
) ->
    tick_alloc_event(State),
    Now = now_ms(),
    Elapsed = Now - LastTime,
    Buckets2 = do_buckets_alloc(Buckets, Elapsed),
    State#{
        last_alloc_time := Now,
        buckets := Buckets2
    }.

do_buckets_alloc(Buckets, Elapsed) ->
    maps:map(
        fun(_, Bucket) ->
            do_bucket_alloc(Bucket, Elapsed)
        end,
        Buckets
    ).

do_bucket_alloc(
    #{
        rate := Rate,
        correction := Correction,
        counter := Counter,
        index := Index,
        capacity := Capacity
    } = Bucket,
    Elapsed
) ->
    Val = counters:get(Counter, Index),
    case Val >= Capacity of
        true ->
            Bucket;
        _ ->
            Inc = Rate * Elapsed + Correction,
            Inc2 = erlang:floor(Inc),
            Correction2 = Inc - Inc2,
            add_tokens(Bucket, Inc2),
            Bucket#{correction := Correction2}
    end.

set_tokens(Counter, Ix, Tokens) ->
    counters:put(Counter, Ix, erlang:floor(Tokens)).

add_tokens(_, 0) ->
    ok;
add_tokens(#{counter := Counter, index := Index, capacity := Capacity}, Tokens) ->
    Val = counters:get(Counter, Index),
    case erlang:min(Capacity, Val + Tokens) - Val of
        Inc when Inc > 0 ->
            counters:put(Counter, Index, Inc + Val);
        _ ->
            ok
    end.

do_create_bucket(
    LimiterName,
    Rate,
    Burst,
    AllocatorName,
    Counter,
    Interval,
    Index
) ->
    Capacity = emqx_limiter:calc_capacity(Rate, Interval),

    set_tokens(Counter, Index, Capacity),
    Ref = emqx_limiter_bucket_ref:new(Counter, Index),
    emqx_limiter_bucket_registry:insert_bucket(AllocatorName, LimiterName, Ref),
    #{
        name => LimiterName,
        rate => Rate,
        burst => Burst,
        capacity => Capacity,
        counter => Counter,
        index => Index,
        correction => 0
    }.

now_ms() ->
    erlang:system_time(millisecond).
