%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc
%% This module limits the rate of retained messages publishing
%% (deletion is a special case of publishing) using a simple token bucket algorithm.
%%
%% Each second at most `max_publish_rate` tokens are refilled. Each publish
%% consumes one token. If there are no tokens available, the publish is dropped.

-module(emqx_retainer_publisher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,
    store_retained/1,
    delete_message/1,
    refresh_limits/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(REFILL_INTERVAL, 1000).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%--------------------------------------------------------------------
%% Constants
%%--------------------------------------------------------------------

-define(COUNTER_PT_KEY, {?MODULE, counter}).
-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).
-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(refill, {}).
-record(refresh_limits, {
    max_publish_rate :: non_neg_integer() | infinity
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_retained(emqx_types:message()) -> ok.
store_retained(#message{topic = Topic, payload = Payload} = Msg) ->
    Size = iolist_size(Payload),
    case payload_size_limit() of
        PayloadSizeLimit when PayloadSizeLimit > 0 andalso PayloadSizeLimit < Size ->
            ?SLOG_THROTTLE(warning, #{
                msg => retain_failed_for_payload_size_exceeded_limit,
                topic => Topic,
                config => "retainer.max_payload_size",
                size => Size,
                limit => PayloadSizeLimit
            }),
            ok;
        _ ->
            WithinLimits = with_limiter(fun() ->
                _ = emqx_retainer:with_backend(
                    fun(Mod, State) -> Mod:store_retained(State, Msg) end
                )
            end),
            case WithinLimits of
                true ->
                    ?tp(retain_within_limit, #{topic => Topic});
                false ->
                    ?tp(retain_failed_for_rate_exceeded_limit, #{topic => Topic}),
                    ?SLOG_THROTTLE(warning, #{
                        msg => retain_failed_for_rate_exceeded_limit,
                        topic => Topic,
                        config => "retainer.max_publish_rate"
                    })
            end
    end.

-spec delete_message(binary()) -> ok.
delete_message(Topic) ->
    WithinLimits = with_limiter(fun() ->
        _ = emqx_retainer:with_backend(
            fun(Mod, State) -> Mod:delete_message(State, Topic) end
        )
    end),
    case WithinLimits of
        true ->
            ok;
        false ->
            ?tp(retained_delete_failed_for_rate_exceeded_limit, #{topic => Topic}),
            ?SLOG_THROTTLE(info, #{
                msg => retained_delete_failed_for_rate_exceeded_limit,
                topic => Topic,
                config => "retainer.max_publish_rate"
            })
    end.

-spec refresh_limits(map()) -> ok.
refresh_limits(Config) ->
    gen_server:cast(?SERVER, #refresh_limits{max_publish_rate = max_publish_rate(Config)}).

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    Counter = counters:new(1, []),
    ok = persistent_term:put(?COUNTER_PT_KEY, Counter),
    MaxPublishRate = max_publish_rate(),
    State0 = #{
        counter => Counter,
        timer => undefined,
        max_publish_rate => MaxPublishRate
    },
    {ok, refresh_limits(State0, MaxPublishRate)}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_call, call => Req}),
    {reply, ignored, State}.

handle_cast(#refresh_limits{max_publish_rate = NewMaxPublishRate}, State) ->
    {noreply, refresh_limits(State, NewMaxPublishRate)};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_cast, cast => Msg}),
    {noreply, State}.

handle_info(#refill{}, State) ->
    {noreply, refill(State)};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => retainer_publisher_unexpected_info, info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    persistent_term:erase(?COUNTER_PT_KEY),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

refill(#{counter := Counter, max_publish_rate := MaxPublishRate} = State) ->
    case MaxPublishRate of
        infinity ->
            State#{timer => undefined};
        _ ->
            RefillCount = min(MaxPublishRate, MaxPublishRate - get_tokens()),
            _ = counters:add(Counter, 1, RefillCount),
            set_refill_timer(State)
    end.

refresh_limits(#{counter := Counter} = State, NewMaxPublishRate) ->
    case NewMaxPublishRate of
        infinity ->
            State#{timer => undefined, max_publish_rate => NewMaxPublishRate};
        _ ->
            ok = counters:put(Counter, 1, NewMaxPublishRate),
            set_refill_timer(State#{max_publish_rate => NewMaxPublishRate})
    end.

set_refill_timer(#{timer := Timer0} = State) ->
    ok = emqx_utils:cancel_timer(Timer0),
    Timer1 = erlang:send_after(?REFILL_INTERVAL, self(), #refill{}),
    State#{timer => Timer1}.

max_publish_rate() ->
    max_publish_rate(emqx_config:get([retainer])).

max_publish_rate(#{max_publish_rate := MaxPublishRate}) ->
    case MaxPublishRate of
        infinity ->
            infinity;
        Rate ->
            round(Rate * ?REFILL_INTERVAL)
    end.

with_limiter(Fun) ->
    case max_publish_rate() of
        infinity ->
            _ = Fun(),
            true;
        _ ->
            with_enabled_limiter(Fun)
    end.

with_enabled_limiter(Fun) ->
    case get_tokens() of
        Tokens when Tokens > 0 ->
            try
                _ = Fun(),
                true
            after
                consume_tokens(1)
            end;
        _Tokens ->
            false
    end.

get_tokens() ->
    try
        counters:get(persistent_term:get(?COUNTER_PT_KEY), 1)
    catch
        error:badarg ->
            0
    end.

consume_tokens(N) ->
    try
        counters:sub(persistent_term:get(?COUNTER_PT_KEY), 1, N)
    catch
        error:badarg ->
            ok
    end.

payload_size_limit() ->
    emqx_conf:get([retainer, max_payload_size], ?DEF_MAX_PAYLOAD_SIZE).
