%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% The sub config handlers maintain independent parts of the emqx config map
%% And there are a top level config handler maintains the overall config map.
-module(emqx_config_handler).

-include("logger.hrl").

-behaviour(gen_server).

%% API functions
-export([ start_link/0
        , add_handler/2
        , update_config/2
        , remove_config/1
        , merge_to_old_config/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MOD, {mod}).

-type handler_name() :: module().
-type handlers() :: #{emqx_config:config_key() => handlers(), ?MOD => handler_name()}.

-optional_callbacks([ pre_config_update/2
                    , post_config_update/2
                    ]).

-callback pre_config_update(emqx_config:update_request(), emqx_config:raw_config()) ->
    emqx_config:update_request().

-callback post_config_update(emqx_config:config(), emqx_config:config()) -> any().

-type state() :: #{
    handlers := handlers(),
    atom() => term()
}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {}, []).

-spec update_config(emqx_config:config_key_path(), emqx_config:update_request()) ->
    ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq) ->
    gen_server:call(?MODULE, {update_config, ConfKeyPath, UpdateReq}).

-spec remove_config(emqx_config:config_key_path()) ->
    ok | {error, term()}.
remove_config(ConfKeyPath) ->
    gen_server:call(?MODULE, {remove_config, ConfKeyPath}).

-spec add_handler(emqx_config:config_key_path(), handler_name()) -> ok.
add_handler(ConfKeyPath, HandlerName) ->
    gen_server:call(?MODULE, {add_child, ConfKeyPath, HandlerName}).

%%============================================================================

-spec init(term()) -> {ok, state()}.
init(_) ->
    {ok, #{handlers => #{?MOD => ?MODULE}}}.

handle_call({add_child, ConfKeyPath, HandlerName}, _From,
            State = #{handlers := Handlers}) ->
    {reply, ok, State#{handlers =>
        emqx_map_lib:deep_put(ConfKeyPath, Handlers, #{?MOD => HandlerName})}};

handle_call({update_config, ConfKeyPath, UpdateReq}, _From,
            #{handlers := Handlers} = State) ->
    OldConf = emqx_config:get(),
    OldRawConf = emqx_config:get_raw(),
    try NewRawConf = do_update_config(ConfKeyPath, Handlers, OldRawConf, UpdateReq),
        OverrideConf = update_override_config(ConfKeyPath, NewRawConf),
        Result = emqx_config:save_configs(NewRawConf, OverrideConf),
        do_post_config_update(ConfKeyPath, Handlers, OldConf, emqx_config:get()),
        {reply, Result, State}
    catch
        Error : Reason : ST ->
            ?LOG(error, "update config failed: ~p", [{Error, Reason, ST}]),
            {reply, {error, Reason}, State}
    end;

handle_call({remove_config, ConfKeyPath}, _From, #{handlers := Handlers} = State) ->
    OldConf = emqx_config:get(),
    OldRawConf = emqx_config:get_raw(),
    BinKeyPath = bin_path(ConfKeyPath),
    try NewRawConf = emqx_map_lib:deep_remove(BinKeyPath, OldRawConf),
        OverrideConf = emqx_map_lib:deep_remove(BinKeyPath, emqx_config:read_override_conf()),
        Result = emqx_config:save_configs(NewRawConf, OverrideConf),
        do_post_config_update(ConfKeyPath, Handlers, OldConf, emqx_config:get()),
        {reply, Result, State}
    catch
        Error : Reason : ST ->
            ?LOG(error, "update config failed: ~p", [{Error, Reason, ST}]),
            {reply, {error, Reason}, State}
    end;

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_update_config([], Handlers, OldRawConf, UpdateReq) ->
    call_pre_config_update(Handlers, OldRawConf, UpdateReq);
do_update_config([ConfKey | ConfKeyPath], Handlers, OldRawConf, UpdateReq) ->
    SubOldRawConf = get_sub_config(bin(ConfKey), OldRawConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    NewUpdateReq = do_update_config(ConfKeyPath, SubHandlers, SubOldRawConf, UpdateReq),
    call_pre_config_update(Handlers, OldRawConf, #{bin(ConfKey) => NewUpdateReq}).

do_post_config_update([], Handlers, OldConf, NewConf) ->
    call_post_config_update(Handlers, OldConf, NewConf);
do_post_config_update([ConfKey | ConfKeyPath], Handlers, OldConf, NewConf) ->
    SubOldConf = get_sub_config(ConfKey, OldConf),
    SubNewConf = get_sub_config(ConfKey, NewConf),
    SubHandlers = maps:get(ConfKey, Handlers, #{}),
    _ = do_post_config_update(ConfKeyPath, SubHandlers, SubOldConf, SubNewConf),
    call_post_config_update(Handlers, OldConf, NewConf).

get_sub_config(ConfKey, Conf) when is_map(Conf) ->
    maps:get(ConfKey, Conf, undefined);
get_sub_config(_, _Conf) -> %% the Conf is a primitive
    undefined.

call_pre_config_update(Handlers, OldRawConf, UpdateReq) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, pre_config_update, 2) of
        true -> HandlerName:pre_config_update(UpdateReq, OldRawConf);
        false -> merge_to_old_config(UpdateReq, OldRawConf)
    end.

call_post_config_update(Handlers, OldConf, NewConf) ->
    HandlerName = maps:get(?MOD, Handlers, undefined),
    case erlang:function_exported(HandlerName, post_config_update, 2) of
        true -> _ = HandlerName:post_config_update(NewConf, OldConf);
        false -> ok
    end.

%% The default callback of config handlers
%% the behaviour is overwriting the old config if:
%%   1. the old config is undefined
%%   2. either the old or the new config is not of map type
%% the behaviour is merging the new the config to the old config if they are maps.
merge_to_old_config(UpdateReq, RawConf) when is_map(UpdateReq), is_map(RawConf) ->
    maps:merge(RawConf, UpdateReq);
merge_to_old_config(UpdateReq, _RawConf) ->
    UpdateReq.

update_override_config(ConfKeyPath, RawConf) ->
    %% We don't save the entire config to emqx_override.conf, but only the part
    %% specified by the ConfKeyPath
    PartialConf = maps:with(root_keys(ConfKeyPath), RawConf),
    OldConf = emqx_config:read_override_conf(),
    maps:merge(OldConf, PartialConf).

root_keys([]) -> [];
root_keys([RootKey | _]) -> [bin(RootKey)].

bin_path(ConfKeyPath) -> [bin(Key) || Key <- ConfKeyPath].

bin(A) when is_atom(A) -> list_to_binary(atom_to_list(A));
bin(B) when is_binary(B) -> B;
bin(S) when is_list(S) -> list_to_binary(S).
