%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc emqttd pooler.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_pooler).

-behaviour(gen_server).

-include("emqttd_internal.hrl").

%% Start the pool supervisor
-export([start_link/0]).

%% API Exports 
-export([start_link/2, submit/1, async_submit/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id}).

%%------------------------------------------------------------------------------
%% @doc Start Pooler Supervisor.
%% @end
%%------------------------------------------------------------------------------
start_link() ->
    emqttd_pool_sup:start_link(pooler, random, {?MODULE, start_link, []}).

%%%=============================================================================
%%% API
%%%=============================================================================
-spec start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, any()}.
start_link(Pool, Id) ->
    gen_server:start_link({local, name(Id)}, ?MODULE, [Pool, Id], []).

name(Id) ->
    list_to_atom(lists:concat([?MODULE, "_", integer_to_list(Id)])).

%%------------------------------------------------------------------------------
%% @doc Submit work to pooler
%% @end
%%------------------------------------------------------------------------------
submit(Fun) ->
    gen_server:call(worker(), {submit, Fun}, infinity).

%%------------------------------------------------------------------------------
%% @doc Submit work to pooler asynchronously
%% @end
%%------------------------------------------------------------------------------
async_submit(Fun) ->
    gen_server:cast(worker(), {async_submit, Fun}).

worker() ->
    gproc_pool:pick_worker(pooler).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Pool, Id]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id}}.

handle_call({submit, Fun}, _From, State) ->
    {reply, run(Fun), State};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({async_submit, Fun}, State) ->
    try run(Fun)
    catch _:Error ->
        lager:error("Pooler Error: ~p, ~p", [Error, erlang:get_stacktrace()])
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

run({M, F, A}) ->
    erlang:apply(M, F, A);
run(Fun) when is_function(Fun) ->
    Fun().

