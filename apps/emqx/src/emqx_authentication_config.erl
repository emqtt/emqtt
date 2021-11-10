%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Authenticator configuration management module.
-module(emqx_authentication_config).

-behaviour(emqx_config_handler).

-export([ pre_config_update/2
        , post_config_update/4
        ]).

-export([ authenticator_id/1
        , authn_type/1
        ]).

-ifdef(TEST).
-export([convert_certs/2, convert_certs/3, clear_certs/2]).
-endif.

-export_type([config/0]).

-include("logger.hrl").

-type parsed_config() :: #{mechanism := atom(),
                           backend => atom(),
                           atom() => term()}.
-type raw_config() :: #{binary() => term()}.
-type config() :: parsed_config() | raw_config().

-type authenticator_id() :: emqx_authentication:authenticator_id().
-type position() :: emqx_authentication:position().
-type chain_name() :: emqx_authentication:chain_name().
-type update_request() :: {create_authenticator, chain_name(), map()}
                        | {delete_authenticator, chain_name(), authenticator_id()}
                        | {update_authenticator, chain_name(), authenticator_id(), map()}
                        | {move_authenticator, chain_name(), authenticator_id(), position()}.

%%------------------------------------------------------------------------------
%% Callbacks of config handler
%%------------------------------------------------------------------------------

-spec pre_config_update(update_request(), emqx_config:raw_config())
    -> {ok, map() | list()} | {error, term()}.
pre_config_update(UpdateReq, OldConfig) ->
    try do_pre_config_update(UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, return_map(NewConfig)}
    catch
        throw : Reason ->
            {error, Reason}
    end.

do_pre_config_update({create_authenticator, ChainName, Config}, OldConfig) ->
    CertsDir = certs_dir(ChainName, Config),
    NConfig = convert_certs(CertsDir, Config),
    {ok, OldConfig ++ [NConfig]};
do_pre_config_update({delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(fun(OldConfig0) ->
                                AuthenticatorID =/= authenticator_id(OldConfig0)
                             end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    CertsDir = certs_dir(ChainName, AuthenticatorID),
    NewConfig = lists:map(
                    fun(OldConfig0) ->
                        case AuthenticatorID =:= authenticator_id(OldConfig0) of
                            true -> convert_certs(CertsDir, Config, OldConfig0);
                            false -> OldConfig0
                        end
                    end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} -> {error, Reason};
        {ok, Part1, [Found | Part2]} ->
            case Position of
                top ->
                    {ok, [Found | Part1] ++ Part2};
                bottom ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                {before, Before} ->
                    case split_by_id(Before, Part1 ++ Part2) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NPart1, [NFound | NPart2]} ->
                            {ok, NPart1 ++ [Found, NFound | NPart2]}
                    end
            end
    end.

-spec post_config_update(update_request(), map() | list(), emqx_config:raw_config(), emqx_config:app_envs())
    -> ok | {ok, map()} | {error, term()}.
post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_configs(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    _ = emqx_authentication:create_chain(ChainName),
    emqx_authentication:create_authenticator(ChainName, NConfig);
do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, OldConfig, _AppEnvs) ->
    case emqx_authentication:delete_authenticator(ChainName, AuthenticatorID) of
        ok ->
            [Config] = [Config0 || Config0 <- to_list(OldConfig), AuthenticatorID == authenticator_id(Config0)],
            CertsDir = certs_dir(ChainName, AuthenticatorID),
            ok = clear_certs(CertsDir, Config);
        {error, Reason} ->
            {error, Reason}
    end;
do_post_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    emqx_authentication:update_authenticator(ChainName, AuthenticatorID, NConfig);
do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    emqx_authentication:move_authenticator(ChainName, AuthenticatorID, Position).

check_config(Config) ->
    [Checked] = check_configs([Config]),
    Checked.

check_configs(Configs) ->
    Providers = emqx_authentication:get_providers(),
    lists:map(fun(C) -> do_check_conifg(C, Providers) end, Configs).

do_check_conifg(Config, Providers) ->
    Type = authn_type(Config),
    case maps:get(Type, Providers, false) of
        false ->
            ?SLOG(warning, #{msg => "unknown_authn_type",
                             type => Type,
                             providers => Providers}),
            throw({unknown_authn_type, Type});
        Module ->
            do_check_conifg(Type, Config, Module)
    end.

do_check_conifg(Type, Config, Module) ->
    F = case erlang:function_exported(Module, check_config, 1) of
            true ->
                fun Module:check_config/1;
            false ->
                fun(C) ->
                        #{config := R} =
                            hocon_schema:check_plain(Module, #{<<"config">> => C},
                                                     #{atom_key => true}),
                        R
                end
        end,
    try
        F(Config)
    catch
        C : E : S ->
            ?SLOG(warning, #{msg => "failed_to_check_config",
                             config => Config,
                             type => Type,
                             exception => C,
                             reason => E,
                             stacktrace => S
                            }),
            throw({bad_authenticator_config, #{type => Type, reason => E}})
    end.

return_map([L]) -> L;
return_map(L) -> L.

to_list(undefined) -> [];
to_list(M) when M =:= #{} -> [];
to_list(M) when is_map(M) -> [M];
to_list(L) when is_list(L) -> L.

convert_certs(CertsDir, Config) ->
    case emqx_tls_lib:ensure_ssl_files(CertsDir, maps:get(<<"ssl">>, Config, undefined)) of
        {ok, SSL} ->
            new_ssl_config(Config, SSL);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => bad_ssl_config}),
            throw({bad_ssl_config, Reason})
    end.

convert_certs(CertsDir, NewConfig, OldConfig) ->
    OldSSL = maps:get(<<"ssl">>, OldConfig, undefined),
    NewSSL = maps:get(<<"ssl">>, NewConfig, undefined),
    case emqx_tls_lib:ensure_ssl_files(CertsDir, NewSSL) of
        {ok, NewSSL1} ->
            ok = emqx_tls_lib:delete_ssl_files(CertsDir, NewSSL1, OldSSL),
            new_ssl_config(NewConfig, NewSSL1);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => bad_ssl_config}),
            throw({bad_ssl_config, Reason})
    end.

new_ssl_config(Config, undefined) -> Config;
new_ssl_config(Config, SSL) -> Config#{<<"ssl">> => SSL}.

clear_certs(CertsDir, Config) ->
    OldSSL = maps:get(<<"ssl">>, Config, undefined),
    ok = emqx_tls_lib:delete_ssl_files(CertsDir, undefined, OldSSL).

split_by_id(ID, AuthenticatorsConfig) ->
    case lists:foldl(
             fun(C, {P1, P2, F0}) ->
                 F = case ID =:= authenticator_id(C) of
                         true -> true;
                         false -> F0
                     end,
                 case F of
                     false -> {[C | P1], P2, F};
                     true -> {P1, [C | P2], F}
                 end
             end, {[], [], false}, AuthenticatorsConfig) of
        {_, _, false} ->
            {error, {not_found, {authenticator, ID}}};
        {Part1, Part2, true} ->
            {ok, lists:reverse(Part1), lists:reverse(Part2)}
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A).

%% @doc Make an authenticator ID from authenticator's config.
%% The authenticator config must contain a 'mechanism' key
%% and maybe a 'backend' key.
%% This function works with both parsed (atom keys) and raw (binary keys)
%% configurations.
-spec authenticator_id(config()) -> authenticator_id().
authenticator_id(#{mechanism := Mechanism0, backend := Backend0}) ->
    Mechanism = to_bin(Mechanism0),
    Backend = to_bin(Backend0),
    <<Mechanism/binary, ":", Backend/binary>>;
authenticator_id(#{mechanism := Mechanism}) ->
    to_bin(Mechanism);
authenticator_id(#{<<"mechanism">> := Mechanism, <<"backend">> := Backend}) ->
    <<Mechanism/binary, ":", Backend/binary>>;
authenticator_id(#{<<"mechanism">> := Mechanism}) ->
    Mechanism;
authenticator_id(_C) ->
    throw({missing_parameter, #{name => mechanism}}).

%% @doc Make the authentication type.
authn_type(#{mechanism := M, backend :=  B}) -> {atom(M), atom(B)};
authn_type(#{mechanism := M}) -> atom(M);
authn_type(#{<<"mechanism">> := M, <<"backend">> := B}) -> {atom(M), atom(B)};
authn_type(#{<<"mechanism">> := M}) -> atom(M).

atom(Bin) ->
    binary_to_existing_atom(Bin, utf8).

%% The relative dir for ssl files.
certs_dir(ChainName, ConfigOrID) ->
    DirName = dir(ChainName, ConfigOrID),
    SubDir = iolist_to_binary(filename:join(["authn", DirName])),
    binary:replace(SubDir, <<":">>, <<"-">>, [global]).

dir(ChainName, ID) when is_binary(ID) ->
    binary:replace(iolist_to_binary([to_bin(ChainName), "-", ID]), <<":">>, <<"-">>);
dir(ChainName, Config) when is_map(Config) ->
    dir(ChainName, authenticator_id(Config)).
