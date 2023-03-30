%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Filesystem storage exporter
%%
%% This is conceptually a part of the Filesystem storage backend that defines
%% how and where complete tranfers are assembled into files and stored.

-module(emqx_ft_storage_exporter).

%% Export API
-export([start_export/3]).
-export([write/2]).
-export([complete/1]).
-export([discard/1]).

%% Listing API
-export([list/1]).

%% Lifecycle API
-export([update_exporter/2]).

%% Internal API
-export([exporter/1]).

-export_type([export/0]).

-type storage() :: emxt_ft_storage_fs:storage().
-type transfer() :: emqx_ft:transfer().
-type filemeta() :: emqx_ft:filemeta().
-type checksum() :: emqx_ft:checksum().

-type exporter_conf() :: map().
-type export_st() :: term().
-type hash_state() :: term().
-opaque export() :: #{
    mod := module(),
    st := export_st(),
    hash := hash_state(),
    filemeta := filemeta()
}.

%%------------------------------------------------------------------------------
%% Behaviour
%%------------------------------------------------------------------------------

-callback start_export(exporter_conf(), transfer(), filemeta()) ->
    {ok, export_st()} | {error, _Reason}.

%% Exprter must discard the export itself in case of error
-callback write(ExportSt :: export_st(), iodata()) ->
    {ok, ExportSt :: export_st()} | {error, _Reason}.

-callback complete(_ExportSt :: export_st(), _Checksum :: checksum()) ->
    ok | {error, _Reason}.

-callback discard(ExportSt :: export_st()) ->
    ok | {error, _Reason}.

-callback list(storage()) ->
    {ok, [emqx_ft_storage:file_info()]} | {error, _Reason}.

%% Lifecycle callbacks

-callback start(exporter_conf()) ->
    ok | {error, _Reason}.

-callback stop(exporter_conf()) ->
    ok.

-callback update(exporter_conf(), exporter_conf()) ->
    ok | {error, _Reason}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_export(storage(), transfer(), filemeta()) ->
    {ok, export()} | {error, _Reason}.
start_export(Storage, Transfer, Filemeta) ->
    {ExporterMod, ExporterConf} = exporter(Storage),
    case ExporterMod:start_export(ExporterConf, Transfer, Filemeta) of
        {ok, ExportSt} ->
            {ok, #{
                mod => ExporterMod,
                st => ExportSt,
                hash => init_checksum(Filemeta),
                filemeta => Filemeta
            }};
        {error, _} = Error ->
            Error
    end.

-spec write(export(), iodata()) ->
    {ok, export()} | {error, _Reason}.
write(#{mod := ExporterMod, st := ExportSt, hash := Hash} = Export, Content) ->
    case ExporterMod:write(ExportSt, Content) of
        {ok, ExportStNext} ->
            {ok, Export#{
                st := ExportStNext,
                hash := update_checksum(Hash, Content)
            }};
        {error, _} = Error ->
            Error
    end.

-spec complete(export()) ->
    ok | {error, _Reason}.
complete(#{mod := ExporterMod, st := ExportSt, hash := Hash, filemeta := Filemeta}) ->
    case verify_checksum(Hash, Filemeta) of
        {ok, Checksum} ->
            ExporterMod:complete(ExportSt, Checksum);
        {error, _} = Error ->
            _ = ExporterMod:discard(ExportSt),
            Error
    end.

-spec discard(export()) ->
    ok | {error, _Reason}.
discard(#{mod := ExporterMod, st := ExportSt}) ->
    ExporterMod:discard(ExportSt).

-spec list(storage()) ->
    {ok, [emqx_ft_storage:file_info()]} | {error, _Reason}.
list(Storage) ->
    {ExporterMod, ExporterOpts} = exporter(Storage),
    ExporterMod:list(ExporterOpts).

%% Lifecycle

-spec update_exporter(emqx_config:config(), emqx_config:config()) -> ok | {error, term()}.
update_exporter(
    #{exporter := #{type := OldType}} = OldConfig,
    #{exporter := #{type := OldType}} = NewConfig
) ->
    {ExporterMod, OldExporterOpts} = exporter(OldConfig),
    {_NewExporterMod, NewExporterOpts} = exporter(NewConfig),
    ExporterMod:update(OldExporterOpts, NewExporterOpts);
update_exporter(
    #{exporter := _} = OldConfig,
    #{exporter := _} = NewConfig
) ->
    {OldExporterMod, OldExporterOpts} = exporter(OldConfig),
    {NewExporterMod, NewExporterOpts} = exporter(NewConfig),
    ok = OldExporterMod:stop(OldExporterOpts),
    NewExporterMod:start(NewExporterOpts);
update_exporter(undefined, NewConfig) ->
    {ExporterMod, ExporterOpts} = exporter(NewConfig),
    ExporterMod:start(ExporterOpts);
update_exporter(OldConfig, undefined) ->
    {ExporterMod, ExporterOpts} = exporter(OldConfig),
    ExporterMod:stop(ExporterOpts);
update_exporter(_, _) ->
    ok.
%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

exporter(Storage) ->
    case maps:get(exporter, Storage) of
        #{type := local} = Options ->
            {emqx_ft_storage_exporter_fs, without_type(Options)};
        #{type := s3} = Options ->
            {emqx_ft_storage_exporter_s3, without_type(Options)}
    end.

without_type(#{type := _} = Options) ->
    maps:without([type], Options).

init_checksum(#{checksum := {Algo, _}}) ->
    crypto:hash_init(Algo);
init_checksum(#{}) ->
    crypto:hash_init(sha256).

update_checksum(Ctx, IoData) ->
    crypto:hash_update(Ctx, IoData).

verify_checksum(Ctx, #{checksum := {Algo, Digest} = Checksum}) ->
    case crypto:hash_final(Ctx) of
        Digest ->
            {ok, Checksum};
        Mismatch ->
            {error, {checksum, Algo, binary:encode_hex(Mismatch)}}
    end;
verify_checksum(Ctx, #{}) ->
    Digest = crypto:hash_final(Ctx),
    {ok, {sha256, Digest}}.
