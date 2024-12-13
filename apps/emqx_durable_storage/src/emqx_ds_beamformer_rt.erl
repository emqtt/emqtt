%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_beamformer_rt).

-feature(maybe_expr, enable).

-behaviour(gen_server).

%% API:
-export([pool/1, start_link/4, enqueue/3, shard_event/2]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([]).

-export_type([]).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-include("emqx_ds_beamformer.hrl").
-include("emqx_ds.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-record(s, {
    module :: module(),
    metrics_id,
    shard,
    parent_sub_tab :: ets:tid(),
    name,
    high_watermark :: ets:tid(),
    queue :: emqx_ds_beamformer_waitq:t(),
    batch_size :: non_neg_integer()
}).

-define(housekeeping_loop, housekeeping_loop).

-record(shard_event, {event}).

%%================================================================================
%% API functions
%%================================================================================

pool(DBShard) ->
    {emqx_ds_beamformer_rt, DBShard}.

-spec enqueue(_Shard, emqx_d_beamformer:poll_req(), timeout()) -> ok.
enqueue(Shard, Req, Timeout) ->
    ?tp(debug, beamformer_enqueue, #{req_id => Req#sub_state.req_id, queue => rt}),
    emqx_ds_beamformer:enqueue(pool(Shard), Req, Timeout).

-spec start_link(module(), _Shard, integer(), emqx_ds_beamformer:opts()) -> {ok, pid()}.
start_link(Mod, ShardId, Name, Opts) ->
    gen_server:start_link(?MODULE, [Mod, ShardId, Name, Opts], []).

shard_event(Shard, Events) ->
    Pool = pool(Shard),
    lists:foreach(
        fun(Event = Stream) ->
            Worker = gproc_pool:pick_worker(Pool, Stream),
            gen_server:cast(Worker, #shard_event{event = Event})
        end,
        Events
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

init([CBM, ShardId, Name, _Opts]) ->
    process_flag(trap_exit, true),
    Pool = pool(ShardId),
    gproc_pool:add_worker(Pool, Name),
    gproc_pool:connect_worker(Pool, Name),
    S = #s{
        module = CBM,
        shard = ShardId,
        parent_sub_tab = emqx_ds_beamformer:subtab(ShardId),
        metrics_id = emqx_ds_beamformer:shard_metrics_id(ShardId),
        name = Name,
        high_watermark = ets:new(high_watermark, [ordered_set, private]),
        queue = emqx_ds_beamformer_waitq:new(),
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    self() ! ?housekeeping_loop,
    {ok, S}.

handle_call(Req = #sub_state{}, _From, S0) ->
    {Reply, S} = do_enqueue(Req, S0),
    {reply, Reply, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(Req = #sub_state{}, S0) ->
    {_Reply, S} = do_enqueue(Req, S0),
    {noreply, S};
handle_cast(#shard_event{event = Event}, S = #s{shard = Shard, queue = Queue}) ->
    ?tp(info, beamformer_rt_event, #{event => Event, shard => Shard}),
    %% FIXME: make a proper stream wrapper
    Stream = Event,
    case emqx_ds_beamformer_waitq:has_candidates(Stream, Queue) of
        true ->
            process_stream_event(Stream, S);
        false ->
            %% Even if we don't have any poll requests for the stream,
            %% it's still necessary to update the high watermark to
            %% avoid full scans of very old data:
            update_high_watermark(Stream, S)
    end,
    {noreply, S};
handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(
    ?housekeeping_loop, S0 = #s{metrics_id = Metrics, queue = Queue, parent_sub_tab = SubTab}
) ->
    %% Reload configuration according from environment variables:
    S = S0#s{
        batch_size = emqx_ds_beamformer:cfg_batch_size()
    },
    %% Drop expired poll requests:
    emqx_ds_beamformer:cleanup_expired(rt, SubTab, Metrics, Queue),
    %% Continue the loop:
    erlang:send_after(emqx_ds_beamformer:cfg_housekeeping_interval(), self(), ?housekeeping_loop),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{shard = ShardId, name = Name}) ->
    Pool = pool(ShardId),
    gproc_pool:disconnect_worker(Pool, Name),
    gproc_pool:remove_worker(Pool, Name),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

do_enqueue(
    Req = #sub_state{stream = Stream, topic_filter = TF, req_id = ReqId, start_key = Key, it = It0},
    S = #s{shard = Shard, queue = Queue, metrics_id = Metrics, module = CBM}
) ->
    case high_watermark(Stream, S) of
        {ok, HighWatermark} ->
            case emqx_ds_beamformer:fast_forward(CBM, Shard, It0, HighWatermark) of
                {ok, It} ->
                    ?tp(beamformer_push_rt, #{
                        req_id => ReqId, stream => Stream, key => Key, it => It
                    }),
                    PQLen = emqx_ds_beamformer_waitq:size(Queue),
                    emqx_ds_builtin_metrics:set_waitq_len(Metrics, PQLen),
                    emqx_ds_beamformer_waitq:insert(Stream, TF, ReqId, Queue);
                {error, unrecoverable, old_key} ->
                    %% FIXME: Race condition: catchup queue managed to
                    %% read the data faster than we got the
                    %% notification, and it managed to advance the
                    %% iterator beyond the current high watermark. We
                    %% cannot enque this request now, since it would
                    %% lead to message duplication.
                    PQLen = emqx_ds_beamformer_waitq:size(Queue),
                    emqx_ds_builtin_metrics:set_waitq_len(Metrics, PQLen),
                    emqx_ds_beamformer_waitq:insert(Stream, TF, ReqId, Queue);
                {error, unrecoverable, has_data} ->
                    ?tp(info, beamformer_push_rt_downgrade, #{
                        req_id => ReqId, stream => Stream, key => Key
                    }),
                    emqx_ds_beamformer_catchup:enqueue(Shard, Req, 0);
                Other ->
                    ?tp(
                        warning,
                        beamformer_rt_cannot_fast_forward,
                        #{
                            it => It0,
                            reason => Other
                        }
                    )
            end;
        undefined ->
            ?tp(
                warning,
                beamformer_push_rt_unknown_stream,
                #{poll_req => Req}
            )
    end,
    {ok, S}.

process_stream_event(
    Stream, S = #s{shard = ShardId, module = CBM, batch_size = BatchSize, queue = Queue}
) ->
    {ok, StartKey} = high_watermark(Stream, S),
    case emqx_ds_beamformer:scan_stream(CBM, ShardId, Stream, ['#'], StartKey, BatchSize) of
        {ok, LastKey, []} ->
            ?tp(beamformer_rt_batch, #{
                shard => ShardId, from => StartKey, to => LastKey, stream => Stream
            }),
            set_high_watermark(Stream, LastKey, S);
        {ok, LastKey, Batch} ->
            ?tp(beamformer_rt_batch, #{
                shard => ShardId, from => StartKey, to => LastKey, stream => Stream
            }),
            Beams = emqx_ds_beamformer:beams_init(
                CBM,
                ShardId,
                fun(SubS) -> queue_drop(Queue, SubS) end,
                fun(_OldSubState, _SubState) -> ok end
            ),
            process_batch(Stream, LastKey, Batch, S, Beams),
            set_high_watermark(Stream, LastKey, S),
            process_stream_event(Stream, S)
    end.

process_batch(
    _Stream,
    EndKey,
    [],
    #s{metrics_id = Metrics, shard = ShardId},
    Beams
) ->
    %% Report metrics:
    NFulfilled = emqx_ds_beamformer:beams_n_matched(Beams),
    NFulfilled > 0 andalso
        begin
            emqx_ds_builtin_metrics:inc_poll_requests_fulfilled(Metrics, NFulfilled),
            emqx_ds_builtin_metrics:observe_sharing(Metrics, NFulfilled)
        end,
    %% Send data:
    emqx_ds_beamformer:beams_conclude(ShardId, EndKey, Beams);
process_batch(Stream, EndKey, [{Key, Msg} | Rest], S, Beams0) ->
    Candidates = queue_search(S, Stream, Key, Msg),
    Beams = emqx_ds_beamformer:beams_add(Key, Msg, Candidates, Beams0),
    process_batch(Stream, EndKey, Rest, S, Beams).

queue_search(#s{queue = Queue, parent_sub_tab = SubTab}, Stream, MsgKey, Msg) ->
    Topic = emqx_topic:tokens(Msg#message.topic),
    Candidates = emqx_ds_beamformer_waitq:matching_keys(Stream, Topic, Queue),
    lists:filtermap(
        fun(WaitqKey) ->
            case ets:lookup(SubTab, emqx_trie_search:get_id(WaitqKey)) of
                [Req] ->
                    #sub_state{msg_matcher = Matcher} = Req,
                    case Matcher(MsgKey, Msg) of
                        true -> {true, Req};
                        false -> false
                    end;
                [] ->
                    %% FIXME: Unsubscribed.
                    false
            end
        end,
        Candidates
    ).

queue_drop(Queue, #sub_state{stream = Stream, topic_filter = TF, req_id = ID}) ->
    emqx_ds_beamformer_waitq:delete(Stream, TF, ID, Queue).

%% queue_update(Queue) ->
%%     fun(_NextKey, Req = #sub_state{stream = Stream, topic_filter = TF, req_id = ReqId}) ->
%%         emqx_ds_beamformer_waitq:insert(Stream, TF, ReqId, Req, Queue)
%%     end.

high_watermark(Stream, S = #s{high_watermark = Tab}) ->
    case ets:lookup(Tab, Stream) of
        [{_, HWM}] ->
            {ok, HWM};
        [] ->
            update_high_watermark(Stream, S)
    end.

update_high_watermark(Stream, S = #s{module = CBM, shard = Shard}) ->
    maybe
        {ok, HWM} ?= emqx_ds_beamformer:high_watermark(CBM, Shard, Stream),
        set_high_watermark(Stream, HWM, S),
        {ok, HWM}
    else
        Other ->
            ?tp(
                warning,
                beamformer_rt_failed_to_update_high_watermark,
                #{shard => Shard, stream => Stream, reason => Other}
            ),
            undefined
    end.

set_high_watermark(Stream, LastSeenKey, #s{high_watermark = Tab}) ->
    ?tp(debug, beamformer_rt_set_high_watermark, #{stream => Stream, key => LastSeenKey}),
    ets:insert(Tab, {Stream, LastSeenKey}).
