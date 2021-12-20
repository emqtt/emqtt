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

-module(emqx_authn_redis).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

-export([ namespace/0
        , roots/0
        , fields/1
        ]).

-export([ refs/0
        , create/2
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn-redis".

roots() ->
    [ {?CONF_NS, hoconsc:mk(hoconsc:union(refs()),
                            #{})}
    ].

fields(standalone) ->
    common_fields() ++ emqx_connector_redis:fields(single);

fields(cluster) ->
    common_fields() ++ emqx_connector_redis:fields(cluster);

fields(sentinel) ->
    common_fields() ++ emqx_connector_redis:fields(sentinel).

common_fields() ->
    [ {mechanism, emqx_authn_schema:mechanism('password-based')}
    , {backend, emqx_authn_schema:backend(redis)}
    , {cmd,                     fun cmd/1}
    , {password_hash_algorithm, fun emqx_authn_password_hashing:type_ro/1}
    ] ++ emqx_authn_schema:common_fields().

cmd(type) -> string();
cmd(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [ hoconsc:ref(?MODULE, standalone)
    , hoconsc:ref(?MODULE, cluster)
    , hoconsc:ref(?MODULE, sentinel)
    ].

create(_AuthenticatorID, Config) ->
    create(Config).

create(#{cmd := Cmd,
         password_hash_algorithm := Algorithm} = Config) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    try
        NCmd = parse_cmd(Cmd),
        ok = emqx_authn_utils:ensure_apps_started(Algorithm),
        State = maps:with(
                  [password_hash_algorithm, salt_position],
                  Config),
        ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
        NState = State#{
                   cmd => NCmd,
                   resource_id => ResourceId},
        case emqx_resource:create_local(ResourceId, emqx_connector_redis, Config) of
            {ok, already_created} ->
                {ok, NState};
            {ok, _} ->
                {ok, NState};
            {error, Reason} ->
                {error, Reason}
        end
    catch
        error:{unsupported_cmd, _Cmd} ->
            {error, {unsupported_cmd, Cmd}};
        error:missing_password_hash ->
            {error, missing_password_hash};
        error:{unsupported_fields, Fields} ->
            {error, {unsupported_fields, Fields}}
    end.

update(Config, State) ->
    case create(Config) of
        {ok, NewState} ->
            ok = destroy(State),
            {ok, NewState};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := Password} = Credential,
             #{cmd := {Command, Key, Fields},
               resource_id := ResourceId,
               password_hash_algorithm := Algorithm}) ->
    NKey = binary_to_list(iolist_to_binary(replace_placeholders(Key, Credential))),
    case emqx_resource:query(ResourceId, {cmd, [Command, NKey | Fields]}) of
        {ok, Values} ->
            case merge(Fields, Values) of
                #{<<"password_hash">> := _} = Selected ->
                    case emqx_authn_utils:check_password_from_selected_map(
                          Algorithm, Selected, Password) of
                        ok ->
                            {ok, emqx_authn_utils:is_superuser(Selected)};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                _ ->
                    ?SLOG(error, #{msg => "cannot_find_password_hash_field",
                                   resource => ResourceId}),
                    ignore
            end;
        {error, Reason} ->
            ?SLOG(error, #{msg => "redis_query_failed",
                           resource => ResourceId,
                           reason => Reason}),
            ignore
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Only support HGET and HMGET
parse_cmd(Cmd) ->
    case string:tokens(Cmd, " ") of
        [Command, Key, Field | Fields] when Command =:= "HGET" orelse Command =:= "HMGET" ->
            NFields = [Field | Fields],
            check_fields(NFields),
            NKey = parse_key(Key),
            {Command, NKey, NFields};
        _ ->
            error({unsupported_cmd, Cmd})
    end.

check_fields(Fields) ->
    HasPassHash = lists:member("password_hash", Fields),
    KnownFields = ["password_hash", "salt", "is_superuser"],
    UnknownFields = [F || F <- Fields, not lists:member(F, KnownFields)],

    case {HasPassHash, UnknownFields} of
        {true, []} -> ok;
        {true, _} -> error({unsupported_fields, UnknownFields});
        {false, _} -> error(missing_password_hash)
    end.

parse_key(Key) ->
    Tokens = re:split(Key, "(" ++ ?RE_PLACEHOLDER ++ ")", [{return, binary}, group, trim]),
    parse_key(Tokens, []).

parse_key([], Acc) ->
    lists:reverse(Acc);
parse_key([[Constant, Placeholder] | Tokens], Acc) ->
    parse_key(Tokens, [{placeholder, Placeholder}, {constant, Constant} | Acc]);
parse_key([[Constant] | Tokens], Acc) ->
    parse_key(Tokens, [{constant, Constant} | Acc]).

replace_placeholders(Key, Credential) ->
    lists:map(fun({constant, Constant}) ->
                  Constant;
                 ({placeholder, Placeholder}) ->
                  case emqx_authn_utils:replace_placeholder(Placeholder, Credential) of
                      undefined -> error({cannot_get_variable, Placeholder});
                      Value -> Value
                  end
              end, Key).

merge(Fields, Value) when not is_list(Value) ->
    merge(Fields, [Value]);
merge(Fields, Values) ->
    maps:from_list(
        [{list_to_binary(K), V}
            || {K, V} <- lists:zip(Fields, Values), V =/= undefined]).
