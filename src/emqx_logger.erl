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

-module(emqx_logger).

-compile({no_auto_import,[error/1]}).

-export([debug/1, debug/2, debug/3]).
-export([info/1, info/2, info/3]).
-export([warning/1, warning/2, warning/3]).
-export([error/1, error/2, error/3]).
-export([critical/1, critical/2, critical/3]).

-export([add_metadata_peername/1, add_metadata_client_id/1]).
-export([add_proc_metadata/1]).

-export([get_primary_log_level/0, set_primary_log_level/1]).
-export([get_log_handlers/0, get_log_handler/1, set_log_handler_level/2]).

debug(Msg) ->
    logger:debug(Msg).
debug(Format, Args) ->
    logger:debug(Format, Args).
debug(Metadata, Format, Args) when is_map(Metadata) ->
    logger:debug(Format, Args, Metadata).

info(Msg) ->
    logger:info(Msg).
info(Format, Args) ->
    logger:info(Format, Args).
info(Metadata, Format, Args) when is_map(Metadata) ->
    logger:info(Format, Args, Metadata).

warning(Msg) ->
    logger:warning(Msg).
warning(Format, Args) ->
    logger:warning(Format, Args).
warning(Metadata, Format, Args) when is_map(Metadata) ->
    logger:warning(Format, Args, Metadata).

error(Msg) ->
    logger:error(Msg).
error(Format, Args) ->
    logger:error(Format, Args).
error(Metadata, Format, Args) when is_map(Metadata) ->
    logger:error(Format, Args, Metadata).

critical(Msg) ->
    logger:critical(Msg).
critical(Format, Args) ->
    logger:critical(Format, Args).
critical(Metadata, Format, Args) when is_map(Metadata) ->
    logger:critical(Format, Args, Metadata).


add_metadata_client_id(ClientId) ->
    add_proc_metadata(#{client_id => ClientId}).

add_metadata_peername(Peername) ->
    add_proc_metadata(#{peername => Peername}).

add_proc_metadata(Meta) ->
    case logger:get_process_metadata() of
        undefined ->
            logger:set_process_metadata(Meta);
        OldMeta ->
            logger:set_process_metadata(maps:merge(OldMeta, Meta))
    end.

get_primary_log_level() ->
    #{level := Level} = logger:get_primary_config(),
    Level.

set_primary_log_level(Level) ->
    logger:set_primary_config(level, Level).

get_log_handlers() ->
    lists:map(fun log_hanlder_info/1, logger:get_handler_config()).

get_log_handler(HandlerId) ->
    {ok, Conf} = logger:get_handler_config(HandlerId),
    log_hanlder_info(Conf).

set_log_handler_level(HandlerId, Level) ->
    logger:set_handler_config(HandlerId, level, Level).

%%========================
%% Internal Functions
%%========================
log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := #{type := Type}}) when Type =:= standard_io;
                                                    Type =:= standard_error ->
    {Id, Level, console};
log_hanlder_info(#{id := Id, level := Level, module := logger_std_h,
                   config := #{type := Type}}) ->
    case Type of
        {file, Filename} -> {Id, Level, Filename};
        {file, Filename, _Opts} -> {Id, Level, Filename};
        _ -> {Id, Level, unknown}
    end;
log_hanlder_info(#{id := Id, level := Level, module := logger_disk_log_h,
                   config := #{file := Filename}}) ->
    {Id, Level, Filename};
log_hanlder_info(#{id := Id, level := Level, module := _OtherModule}) ->
    {Id, Level, unknown}.