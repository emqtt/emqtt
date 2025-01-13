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

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/2,
    dispatch/1,
    refresh_limiter/0,
    wait_dispatch_complete/1,
    worker/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type limiter() :: emqx_limiter:limiter().
-type context() :: emqx_retainer:context().
-type topic() :: emqx_types:topic().

-define(POOL, ?DISPATCHER_POOL).

%% This module is `emqx_retainer` companion
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%%===================================================================
%%% API
%%%===================================================================

-spec dispatch(topic()) -> ok.
dispatch(Topic) ->
    dispatch(Topic, self()).

-spec dispatch(topic(), pid()) -> ok.
dispatch(Topic, Pid) ->
    cast({dispatch, Pid, Topic}).

-spec refresh_limiter() -> ok.
refresh_limiter() ->
    Conf = emqx:get_config([retainer]),
    refresh_limiter(Conf).

-spec refresh_limiter(hocon:config()) -> ok.
refresh_limiter(Conf) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            gen_server:cast(Pid, ?FUNCTION_NAME)
        end,
        Workers
    ).

-spec wait_dispatch_complete(timeout()) -> ok.
wait_dispatch_complete(Timeout) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            ok = gen_server:call(Pid, ?FUNCTION_NAME, Timeout)
        end,
        Workers
    ).

-spec worker() -> pid().
worker() ->
    gproc_pool:pick_worker(?POOL, self()).

-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    BucketCfg = emqx:get_config([retainer, flow_control, batch_deliver_limiter], undefined),
    {ok, Limiter} = emqx_limiter_server:connect(?DISPATCHER_LIMITER_ID, internal, BucketCfg),
    {ok, #{pool => Pool, id => Id, limiter => Limiter}}.

handle_call(wait_dispatch_complete, _From, State) ->
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({dispatch, Pid, Topic}, #{limiter := Limiter} = State) ->
    {ok, Limiter2} = dispatch(Pid, Topic, Limiter),
    {noreply, State#{limiter := Limiter2}};
handle_cast({refresh_limiter, Conf}, State) ->
    BucketCfg = emqx_utils_maps:deep_get([flow_control, batch_deliver_limiter], Conf, undefined),
    {ok, Limiter} = emqx_limiter_server:connect(?DISPATCHER_LIMITER_ID, internal, BucketCfg),
    {noreply, State#{limiter := Limiter}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cast(Msg) ->
    gen_server:cast(worker(), Msg).

-spec dispatch(pid(), topic(), limiter()) -> {ok, limiter()}.
dispatch(Pid, Topic, Limiter) ->
    Context = emqx_retainer:context(),
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    case emqx_topic:wildcard(Topic) of
        true ->
            {ok, Messages, Cursor} = Mod:match_messages(State, Topic, undefined),
            dispatch_with_cursor(Context, Messages, Cursor, Pid, Topic, Limiter);
        false ->
            {ok, Messages} = Mod:read_message(State, Topic),
            dispatch_at_once(Messages, Pid, Topic, Limiter)
    end.

dispatch_at_once(Messages, Pid, Topic, Limiter0) ->
    case deliver(Messages, Pid, Topic, Limiter0) of
        {_Result, Limiter1} ->
            {ok, Limiter1};
        no_receiver ->
            ?tp(debug, retainer_dispatcher_no_receiver, #{topic => Topic}),
            {ok, Limiter0}
    end.

dispatch_with_cursor(Context, [], Cursor, _Pid, _Topic, Limiter) ->
    ok = delete_cursor(Context, Cursor),
    {ok, Limiter};
dispatch_with_cursor(Context, Messages0, Cursor0, Pid, Topic, Limiter0) ->
    case deliver(Messages0, Pid, Topic, Limiter0) of
        {true, Limiter1} ->
            {ok, Messages1, Cursor1} = match_next(Context, Topic, Cursor0),
            dispatch_with_cursor(Context, Messages1, Cursor1, Pid, Topic, Limiter1);
        {false, Limiter1} ->
            ok = delete_cursor(Context, Cursor0),
            {ok, Limiter1};
        no_receiver ->
            ?tp(debug, retainer_dispatcher_no_receiver, #{topic => Topic}),
            ok = delete_cursor(Context, Cursor0),
            {ok, Limiter0}
    end.

match_next(_Context, _Topic, undefined) ->
    {ok, [], undefined};
match_next(Context, Topic, Cursor) ->
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:match_messages(State, Topic, Cursor).

delete_cursor(_Context, undefined) ->
    ok;
delete_cursor(Context, Cursor) ->
    Mod = emqx_retainer:backend_module(Context),
    State = emqx_retainer:backend_state(Context),
    Mod:delete_cursor(State, Cursor).

-spec deliver([emqx_types:message()], pid(), topic(), limiter()) ->
    {boolean(), limiter()} | no_receiver.
deliver(Messages, Pid, Topic, Limiter) ->
    case erlang:is_process_alive(Pid) of
        false ->
            no_receiver;
        _ ->
            BatchSize = emqx_conf:get([retainer, flow_control, batch_deliver_number], 0),
            NMessages = filter_delivery(Messages, Topic),
            case BatchSize of
                0 ->
                    deliver_to_client(NMessages, Pid, Topic),
                    {true, Limiter};
                _ ->
                    deliver_in_batches(NMessages, BatchSize, Pid, Topic, Limiter)
            end
    end.

deliver_in_batches([], _BatchSize, _Pid, _Topic, Limiter) ->
    {true, Limiter};
deliver_in_batches(Msgs, BatchSize, Pid, Topic, Limiter0) ->
    {BatchActualSize, Batch, RestMsgs} = take(BatchSize, Msgs),
    case emqx_limiter:check(BatchActualSize, Limiter0) of
        {true, Limiter1} ->
            ok = deliver_to_client(Batch, Pid, Topic),
            deliver_in_batches(RestMsgs, BatchSize, Pid, Topic, Limiter1);
        {false, _Limiter1} = Drop ->
            ?SLOG(debug, #{
                msg => "retained_message_dropped",
                reason => "reached_ratelimit",
                dropped_count => BatchActualSize
            }),
            Drop
    end.

deliver_to_client([Msg | Rest], Pid, Topic) ->
    Pid ! {deliver, Topic, Msg},
    deliver_to_client(Rest, Pid, Topic);
deliver_to_client([], _, _) ->
    ok.

filter_delivery(Messages, _Topic) ->
    lists:filter(fun check_clientid_banned/1, Messages).

check_clientid_banned(Msg) ->
    case emqx_banned:check_clientid(Msg#message.from) of
        false ->
            true;
        true ->
            ?tp(debug, ignore_retained_message_due_to_banned, #{
                reason => publisher_client_banned,
                clientid => Msg#message.from
            }),
            false
    end.

take(N, List) ->
    take(N, List, 0, []).

take(0, List, Count, Acc) ->
    {Count, lists:reverse(Acc), List};
take(_N, [], Count, Acc) ->
    {Count, lists:reverse(Acc), []};
take(N, [H | T], Count, Acc) ->
    take(N - 1, T, Count + 1, [H | Acc]).

get_limiter() ->
    case emqx_limiter_manager:find_bucket(emqx_limiter:internal_allocator(), retainer) of
        undefined ->
            undefined;
        {ok, Ref} ->
            emqx_limiter_shared:create(Ref)
    end.
