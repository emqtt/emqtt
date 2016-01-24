%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
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
%%% @doc Trace MQTT packets/messages by ClientID or Topic.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_trace).

-behaviour(gen_server).

-include("emqttd_internal.hrl").

%% API Function Exports
-export([start_link/0]).

-export([start_trace/2, stop_trace/1, all_traces/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {level, traces}).

-type trace_who() :: {client | topic, binary()}.

-define(TRACE_OPTIONS, [{formatter_config, [time, " [",severity,"] ", message, "\n"]}]).

%%%=============================================================================
%%% API
%%%=============================================================================
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc Start to trace client or topic.
%% @end
%%------------------------------------------------------------------------------
-spec start_trace(trace_who(), string()) -> ok | {error, any()}.
start_trace({client, ClientId}, LogFile) ->
    start_trace({start_trace, {client, ClientId}, LogFile});

start_trace({topic, Topic}, LogFile) ->
    start_trace({start_trace, {topic, Topic}, LogFile}).

start_trace(Req) ->
    gen_server:call(?MODULE, Req, infinity).

%%------------------------------------------------------------------------------
%% @doc Stop tracing client or topic.
%% @end
%%------------------------------------------------------------------------------
-spec stop_trace(trace_who()) -> ok | {error, any()}.
stop_trace({client, ClientId}) ->
    gen_server:call(?MODULE, {stop_trace, {client, ClientId}});
stop_trace({topic, Topic}) ->
    gen_server:call(?MODULE, {stop_trace, {topic, Topic}}).

%%------------------------------------------------------------------------------
%% @doc Lookup all traces.
%% @end
%%------------------------------------------------------------------------------
-spec all_traces() -> [{Who :: trace_who(), LogFile :: string()}].
all_traces() ->
    gen_server:call(?MODULE, all_traces).

init([]) ->
    {ok, #state{level = info, traces = #{}}}.

handle_call({start_trace, Who, LogFile}, _From, State = #state{level = Level, traces = Traces}) ->
    case lager:trace_file(LogFile, [Who], Level, ?TRACE_OPTIONS) of
        {ok, exists} ->
            {reply, {error, existed}, State};
        {ok, Trace}  ->
            {reply, ok, State#state{traces = maps:put(Who, {Trace, LogFile}, Traces)}};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call({stop_trace, Who}, _From, State = #state{traces = Traces}) ->
    case maps:find(Who, Traces) of
        {ok, {Trace, _LogFile}} ->
            case lager:stop_trace(Trace) of
                ok -> ok;
                {error, Error} -> lager:error("Stop trace ~p error: ~p", [Who, Error])
            end,
            {reply, ok, State#state{traces = maps:remove(Who, Traces)}};
        error ->
            {reply, {error, not_found}, State}
    end;

handle_call(all_traces, _From, State = #state{traces = Traces}) ->
    {reply, [{Who, LogFile} || {Who, {_Trace, LogFile}}
                               <- maps:to_list(Traces)], State};

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

