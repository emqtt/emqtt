%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_manager).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([start_link/0]).

-export([
    find_bucket/2,
    insert_bucket/3,
    delete_bucket/2
]).

-export([
    register_name/2,
    unregister_name/1,
    whereis_name/1,
    send/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type bucket_ref() :: emqx_limiter_bucket_ref:bucket_ref().
-type limiter_name() :: {emqx_limiter:zone(), emqx_limiter:limiter_name()}.
-type tab_key() :: emqx_limiter:zone() | limiter_name().
-type tab_value() :: pid() | bucket_ref().

-define(TAB, ?MODULE).

%% counter record in ets table
-record(?TAB, {
    key :: tab_key(),
    value :: tab_value()
}).

-define(LIMITER_NAME(Zone, Name), {Zone, Name}).

%%--------------------------------------------------------------------
%%  name callback
%%--------------------------------------------------------------------
register_name(Name, Pid) ->
    case whereis_name(Name) of
        undefined ->
            ets:insert(?TAB, #?TAB{key = Name, value = Pid}),
            yes;
        _ ->
            no
    end.

unregister_name(Name) ->
    ets:delete(?TAB, Name).

whereis_name(Name) ->
    case ets:lookup(?TAB, Name) of
        [#?TAB{value = Pid}] ->
            Pid;
        _ ->
            undefined
    end.

send(Name, Msg) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            Pid ! Msg,
            Pid;
        undefined ->
            exit({badarg, {Name, Msg}})
    end.

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec find_bucket(emqx_limiter:zone(), emqx_limiter:limiter_name()) ->
    {ok, bucket_ref()} | undefined.
find_bucket(Zone, Name) ->
    case ets:lookup(?TAB, ?LIMITER_NAME(Zone, Name)) of
        [#?TAB{value = Bucket}] ->
            {ok, Bucket};
        _ ->
            undefined
    end.

-spec insert_bucket(
    emqx_limiter:zone(),
    emqx_limiter:limiter_name(),
    bucket_ref()
) -> boolean().
insert_bucket(Zone, Name, Bucket) ->
    ets:insert(
        ?TAB,
        #?TAB{key = ?LIMITER_NAME(Zone, Name), value = Bucket}
    ).

-spec delete_bucket(emqx_limiter:zone(), emqx_limiter:limiter_name()) -> true.
delete_bucket(Zone, Name) ->
    ets:delete(?TAB, ?LIMITER_NAME(Zone, Name)).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%%  gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    _ = ets:new(?TAB, [
        set,
        public,
        named_table,
        {keypos, #?TAB.key},
        {write_concurrency, true},
        {read_concurrency, true},
        {heir, erlang:whereis(emqx_limiter_sup), none}
    ]),
    {ok, #{}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
