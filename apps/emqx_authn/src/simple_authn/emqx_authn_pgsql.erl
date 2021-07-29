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

-module(emqx_authn_pgsql).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0, fields/1 ]).

-export([ create/1
        , update/2
        , authenticate/2
        , destroy/1
        ]).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

structs() -> [config].

fields(config) ->
    [ {name,                    fun emqx_authn_schema:authenticator_name/1}
    , {mechanism,               {enum, ['password-based']}}
    , {server_type,             {enum, [pgsql]}}
    , {password_hash_algorithm, fun password_hash_algorithm/1}
    , {salt_position,           {enum, [prefix, suffix]}}
    , {query,                   fun query/1}
    ] ++ emqx_connector_schema_lib:relational_db_fields()
    ++ emqx_connector_schema_lib:ssl_fields().

password_hash_algorithm(type) -> string();
password_hash_algorithm(_) -> undefined.

query(type) -> string();
query(nullable) -> false;
query(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{ query := Query0
        , password_hash_algorithm := Algorithm
        , salt_position := SaltPosition
        , '_unique' := Unique
        } = Config) ->
    {Query, PlaceHolders} = parse_query(Query0),
    State = #{query => Query,
              placeholders => PlaceHolders,
              password_hash_algorithm => Algorithm,
              salt_position => SaltPosition},
    case emqx_resource:create_local(Unique, emqx_connector_pgsql, Config) of
        {ok, _} ->
            {ok, State#{resource_id => Unique}};
        {error, already_created} ->
            {ok, State#{resource_id => Unique}};
        {error, Reason} ->
            {error, Reason}
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
             #{resource_id := ResourceID,
               query := Query,
               placeholders := PlaceHolders} = State) ->
    Params = emqx_authn_utils:replace_placeholder(PlaceHolders, Credential),
    case emqx_resource:query(ResourceID, {sql, Query, Params}) of
        {ok, _Columns, []} -> ignore;
        {ok, Columns, Rows} ->
            %% TODO: Support superuser
            Selected = maps:from_list(lists:zip(Columns, Rows)),
            check_password(Password, Selected, State);
        {error, _Reason} ->
            ignore
    end.

destroy(#{resource_id := ResourceID}) ->
    _ = emqx_resource:remove_local(ResourceID),
    ok.
    
%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

check_password(undefined, _Algorithm, _Selected) ->
    {error, bad_username_or_password};
check_password(Password,
               #{password_hash := Hash},
               #{password_hash_algorithm := bcrypt}) ->
    {ok, Hash0} = bcrypt:hashpw(Password, Hash),
    case list_to_binary(Hash0) =:= Hash of
        true -> ok;
        false -> {error, bad_username_or_password}
    end;
check_password(Password,
               #{password_hash := Hash} = Selected,
               #{password_hash_algorithm := Algorithm,
                 salt_position := SaltPosition}) ->
    Salt = maps:get(salt, Selected, <<>>),
    Hash0 = case SaltPosition of
                prefix -> emqx_passwd:hash(Algorithm, <<Salt/binary, Password/binary>>);
                suffix -> emqx_passwd:hash(Algorithm, <<Password/binary, Salt/binary>>)
            end,
    case Hash0 =:= Hash of
        true -> ok;
        false -> {error, bad_username_or_password}
    end.

%% TODO: Support prepare
parse_query(Query) ->
    case re:run(Query, "\\$\\{[a-z0-9\\_]+\\}", [global, {capture, all, binary}]) of
        {match, Captured} ->
            PlaceHolders = [PlaceHolder || PlaceHolder <- Captured],
            Replacements = ["$" ++ integer_to_list(I) || I <- lists:seq(1, length(Captured))],
            NQuery = lists:foldl(fun({PlaceHolder, Replacement}, Query0) ->
                                     re:replace(Query0, <<"'\\", PlaceHolder/binary, "'">>, Replacement, [{return, binary}])
                                 end, Query, lists:zip(PlaceHolders, Replacements)),
            {NQuery, PlaceHolders};
        nomatch ->
            {Query, []}
    end.
