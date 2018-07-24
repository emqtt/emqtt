%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/2]).
-export([subscribe/1, subscribe/2, subscribe/3, subscribe/4]).
-export([publish/1, publish/2, safe_publish/1]).
-export([unsubscribe/1, unsubscribe/2]).
-export([dispatch/2, dispatch/3]).
-export([subscriptions/1, subscribers/1, subscribed/2]).
-export([get_subopts/2, set_subopts/3]).
-export([topics/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {pool, id, submon}).

-define(BROKER, ?MODULE).
-define(TIMEOUT, 120000).

%% ETS tables
-define(SUBOPTION,    emqx_suboption).
-define(SUBSCRIBER,   emqx_subscriber).
-define(SUBSCRIPTION, emqx_subscription).

-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], [{hibernate_after, 2000}]).

%%------------------------------------------------------------------------------
%% Subscribe/Unsubscribe
%%------------------------------------------------------------------------------

-spec(subscribe(topic()) -> ok | {error, term()}).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, self()).

-spec(subscribe(topic(), subscriber()) -> ok | {error, term()}).
subscribe(Topic, Subscriber) when is_binary(Topic) ->
    subscribe(Topic, Subscriber, []).

-spec(subscribe(topic(), subscriber(), [suboption()]) -> ok | {error, term()}).
subscribe(Topic, Subscriber, Options) when is_binary(Topic) ->
    subscribe(Topic, Subscriber, Options, ?TIMEOUT).

-spec(subscribe(topic(), subscriber(), [suboption()], timeout())
      -> ok | {error, term()}).
subscribe(Topic, Subscriber, Options, Timeout) ->
    {Topic1, Options1} = emqx_topic:parse(Topic, Options),
    SubReq = {subscribe, Topic1, with_subpid(Subscriber), Options1},
    async_call(pick(Subscriber), SubReq, Timeout).

-spec(unsubscribe(topic()) -> ok | {error, term()}).
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe(Topic, self()).

-spec(unsubscribe(topic(), subscriber()) -> ok | {error, term()}).
unsubscribe(Topic, Subscriber) when is_binary(Topic) ->
    unsubscribe(Topic, Subscriber, ?TIMEOUT).

-spec(unsubscribe(topic(), subscriber(), timeout()) -> ok | {error, term()}).
unsubscribe(Topic, Subscriber, Timeout) ->
    {Topic1, _} = emqx_topic:parse(Topic),
    UnsubReq = {unsubscribe, Topic1, with_subpid(Subscriber)},
    async_call(pick(Subscriber), UnsubReq, Timeout).

%%------------------------------------------------------------------------------
%% Publish
%%------------------------------------------------------------------------------

-spec(publish(topic(), payload()) -> delivery() | stopped).
publish(Topic, Payload) when is_binary(Topic), is_binary(Payload) ->
    publish(emqx_message:make(Topic, Payload)).

-spec(publish(message()) -> {ok, delivery()} | {error, stopped}).
publish(Msg = #message{from = From}) ->
    %% Hook to trace?
    _ = trace(publish, From, Msg),
    case emqx_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #message{topic = Topic}} ->
            {ok, route(aggre(emqx_router:match_routes(Topic)), delivery(Msg1))};
        {stop, Msg1} ->
            emqx_logger:warning("Stop publishing: ~s", [emqx_message:format(Msg1)]),
            {error, stopped}
    end.

%% called internally
safe_publish(Msg) ->
    try
        publish(Msg)
    catch
        _:Error:Stacktrace ->
            emqx_logger:error("[Broker] publish error: ~p~n~p~n~p", [Error, Msg, Stacktrace])
    end.

%%------------------------------------------------------------------------------
%% Trace
%%------------------------------------------------------------------------------

trace(publish, From, _Msg) when is_atom(From) ->
    %% Dont' trace '$SYS' publish
    ignore;
trace(public, #client{id = ClientId, username = Username},
      #message{topic = Topic, payload = Payload}) ->
    emqx_logger:info([{client, ClientId}, {topic, Topic}],
                     "~s/~s PUBLISH to ~s: ~p", [Username, ClientId, Topic, Payload]);
trace(public, From, #message{topic = Topic, payload = Payload})
    when is_binary(From); is_list(From) ->
    emqx_logger:info([{client, From}, {topic, Topic}],
                     "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

%%------------------------------------------------------------------------------
%% Route
%%------------------------------------------------------------------------------

route([], Delivery = #delivery{message = Msg}) ->
    emqx_hooks:run('message.dropped', [undefined, Msg]),
    dropped(Msg#message.topic), Delivery;

route([{Topic, Node}], Delivery) when Node =:= node() ->
    dispatch(Topic, Delivery);

route([{Topic, Node}], Delivery = #delivery{flows = Flows}) when is_atom(Node) ->
    forward(Node, Topic, Delivery#delivery{flows = [#route{topic = Topic, dest = Node} |Flows]});

route([{Topic, Shared}], Delivery) when is_tuple(Shared); is_binary(Shared) ->
    emqx_shared_sub:dispatch(Shared, Topic, Delivery);

route(Routes, Delivery) ->
    lists:foldl(fun(Route, Acc) -> route([Route], Acc) end, Delivery, Routes).

aggre([]) ->
    [];
aggre([#route{topic = To, dest = Dest}]) ->
    [{To, Dest}];
aggre(Routes) ->
    lists:foldl(
      fun(#route{topic = To, dest = Node}, Acc) when is_atom(Node) ->
          [{To, Node} | Acc];
        (#route{topic = To, dest = {Group, _Node}}, Acc) ->
          lists:usort([{To, Group} | Acc])
      end, [], Routes).

%% @doc Forward message to another node.
forward(Node, To, Delivery) ->
    %% rpc:call to ensure the delivery, but the latency:(
    case emqx_rpc:call(Node, ?BROKER, dispatch, [To, Delivery]) of
        {badrpc, Reason} ->
            emqx_logger:error("[Broker] Failed to forward msg to ~s: ~p", [Node, Reason]),
            Delivery;
        Delivery1 -> Delivery1
    end.

-spec(dispatch(topic(), delivery()) -> delivery()).
dispatch(Topic, Delivery = #delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            emqx_hooks:run('message.dropped', [undefined, Msg]),
            dropped(Topic), Delivery;
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg),
            Delivery#delivery{flows = [{dispatch, Topic, 1}|Flows]};
        Subscribers ->
            Count = lists:foldl(fun(Sub, Acc) ->
                                    dispatch(Sub, Topic, Msg), Acc + 1
                                end, 0, Subscribers),
            Delivery#delivery{flows = [{dispatch, Topic, Count}|Flows]}
    end.

dispatch(SubPid, Topic, Msg) when is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg};
dispatch({SubId, SubPid}, Topic, Msg) when is_binary(SubId), is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg};
dispatch(SubId, Topic, Msg) when is_binary(SubId) ->
   emqx_sm:dispatch(SubId, Topic, Msg);
dispatch({share, _Group, _Sub}, _Topic, _Msg) ->
    ignored.

dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqx_metrics:inc('messages/dropped').

delivery(Msg) ->
    #delivery{node = node(), message = Msg, flows = []}.

subscribers(Topic) ->
    try ets:lookup_element(?SUBSCRIBER, Topic, 2) catch error:badarg -> [] end.

subscriptions(Subscriber) ->
    lists:map(fun({_, {share, _Group, Topic}}) ->
                  subscription(Topic, Subscriber);
                 ({_, Topic}) ->
                  subscription(Topic, Subscriber)
        end, ets:lookup(?SUBSCRIPTION, Subscriber)).

subscription(Topic, Subscriber) ->
    {Topic, ets:lookup_element(?SUBOPTION, {Topic, Subscriber}, 2)}.

-spec(subscribed(topic(), subscriber()) -> boolean()).
subscribed(Topic, SubPid) when is_binary(Topic), is_pid(SubPid) ->
    ets:member(?SUBOPTION, {Topic, SubPid});
subscribed(Topic, SubId) when is_binary(Topic), is_binary(SubId) ->
    length(ets:match_object(?SUBOPTION, {{Topic, {SubId, '_'}}, '_'}, 1)) == 1;
subscribed(Topic, {SubId, SubPid}) when is_binary(Topic), is_binary(SubId), is_pid(SubPid) ->
    ets:member(?SUBOPTION, {Topic, {SubId, SubPid}}).

-spec(get_subopts(topic(), subscriber()) -> [suboption()]).
get_subopts(Topic, Subscriber) when is_binary(Topic) ->
    try ets:lookup_element(?SUBOPTION, {Topic, Subscriber}, 2)
    catch error:badarg -> []
    end.

-spec(set_subopts(topic(), subscriber(), [suboption()]) -> boolean()).
set_subopts(Topic, Subscriber, Opts) when is_binary(Topic), is_list(Opts) ->
    case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
        [{_, OldOpts}] ->
            Opts1 = lists:usort(lists:umerge(Opts, OldOpts)),
            ets:insert(?SUBOPTION, {{Topic, Subscriber}, Opts1});
        [] -> false
    end.

with_subpid(SubPid) when is_pid(SubPid) ->
    SubPid;
with_subpid(SubId) when is_binary(SubId) ->
    {SubId, self()};
with_subpid({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    {SubId, SubPid}.

async_call(Broker, Msg, Timeout) ->
    From = {self(), Tag = make_ref()},
    ok = gen_server:cast(Broker, {From, Msg}),
    receive
        {Tag, Reply} -> Reply
    after Timeout ->
        {error, timeout}
    end.

pick(SubPid) when is_pid(SubPid) ->
    gproc_pool:pick_worker(broker, SubPid);
pick(SubId) when is_binary(SubId) ->
    gproc_pool:pick_worker(broker, SubId);
pick({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    pick(SubPid).

-spec(topics() -> [topic()]).
topics() -> emqx_router:topics().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id, submon = emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Broker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({From, {subscribe, Topic, Subscriber, Options}}, State) ->
    case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
        [] ->
            Group = proplists:get_value(share, Options),
            true = do_subscribe(Group, Topic, Subscriber, Options),
            emqx_shared_sub:subscribe(Group, Topic, subpid(Subscriber)),
            emqx_router:add_route(From, Topic, destination(Options)),
            {noreply, monitor_subscriber(Subscriber, State)};
        [_] ->
            gen_server:reply(From, ok),
            {noreply, State}
    end;

handle_cast({From, {unsubscribe, Topic, Subscriber}}, State) ->
    case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
        [{_, Options}] ->
            Group = proplists:get_value(share, Options),
            true = do_unsubscribe(Group, Topic, Subscriber),
            emqx_shared_sub:unsubscribe(Group, Topic, subpid(Subscriber)),
            case ets:member(?SUBSCRIBER, Topic) of
                false -> emqx_router:del_route(From, Topic, destination(Options));
                true  -> gen_server:reply(From, ok)
            end;
        [] -> gen_server:reply(From, ok)
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_logger:error("[Broker] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{submon = SubMon}) ->
    Subscriber = case SubMon:find(SubPid) of
                     undefined -> SubPid;
                     SubId -> {SubId, SubPid}
                 end,
    Topics = lists:map(fun({_, {share, _, Topic}}) ->
                           Topic;
                          ({_, Topic}) ->
                           Topic
                       end, ets:lookup(?SUBSCRIPTION, Subscriber)),
    lists:foreach(
      fun(Topic) ->
        case ets:lookup(?SUBOPTION, {Topic, Subscriber}) of
            [{_, Options}] ->
                Group = proplists:get_value(share, Options),
                true = do_unsubscribe(Group, Topic, Subscriber),
                case ets:member(?SUBSCRIBER, Topic) of
                    false -> emqx_router:del_route(Topic, destination(Options));
                    true  -> ok
                end;
            [] -> ok
        end
    end, Topics),
    {noreply, demonitor_subscriber(SubPid, State)};

handle_info(Info, State) ->
    emqx_logger:error("[Broker] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    true = gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_subscribe(Group, Topic, Subscriber, Options) ->
    ets:insert(?SUBSCRIPTION, {Subscriber, shared(Group, Topic)}),
    ets:insert(?SUBSCRIBER, {Topic, shared(Group, Subscriber)}),
    ets:insert(?SUBOPTION, {{Topic, Subscriber}, Options}).

do_unsubscribe(Group, Topic, Subscriber) ->
    ets:delete_object(?SUBSCRIPTION, {Subscriber, shared(Group, Topic)}),
    ets:delete_object(?SUBSCRIBER, {Topic, shared(Group, Subscriber)}),
    ets:delete(?SUBOPTION, {Topic, Subscriber}).

monitor_subscriber(SubPid, State = #state{submon = SubMon}) when is_pid(SubPid) ->
    State#state{submon = SubMon:monitor(SubPid)};

monitor_subscriber({SubId, SubPid}, State = #state{submon = SubMon}) ->
    State#state{submon = SubMon:monitor(SubPid, SubId)}.

demonitor_subscriber(SubPid, State = #state{submon = SubMon}) ->
    State#state{submon = SubMon:demonitor(SubPid)}.

destination(Options) ->
    case proplists:get_value(share, Options) of
       undefined -> node();
       Group     -> {Group, node()}
    end.

subpid(SubPid) when is_pid(SubPid) ->
    SubPid;
subpid({_SubId, SubPid}) when is_pid(SubPid) ->
    SubPid.

shared(undefined, Name) -> Name;
shared(Group, Name)     -> {share, Group, Name}.

