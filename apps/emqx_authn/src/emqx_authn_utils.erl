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

-module(emqx_authn_utils).

-include_lib("emqx/include/emqx_placeholder.hrl").

-export([ check_password_from_selected_map/3
        , replace_placeholders/2
        , replace_placeholder/2
        , is_superuser/1
        , bin/1
        , ensure_apps_started/1
        , cleanup_resources/0
        , make_resource_id/1
        ]).

-define(RESOURCE_GROUP, <<"emqx_authn">>).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

check_password_from_selected_map(_Algorithm, _Selected, undefined) ->
    {error, bad_username_or_password};
check_password_from_selected_map(
  Algorithm, #{<<"password_hash">> := Hash} = Selected, Password) ->
    Salt = maps:get(<<"salt">>, Selected, <<>>),
    case emqx_authn_password_hashing:check_password(Algorithm, Salt, Hash, Password) of
        true -> ok;
        false ->
            {error, bad_username_or_password}
    end.

replace_placeholders(PlaceHolders, Data) ->
    replace_placeholders(PlaceHolders, Data, []).

replace_placeholders([], _Credential, Acc) ->
    lists:reverse(Acc);
replace_placeholders([Placeholder | More], Credential, Acc) ->
    case replace_placeholder(Placeholder, Credential) of
        undefined ->
            error({cannot_get_variable, Placeholder});
        V ->
            replace_placeholders(More, Credential, [convert_to_sql_param(V) | Acc])
    end.

replace_placeholder(?PH_USERNAME, Credential) ->
    maps:get(username, Credential, undefined);
replace_placeholder(?PH_CLIENTID, Credential) ->
    maps:get(clientid, Credential, undefined);
replace_placeholder(?PH_PASSWORD, Credential) ->
    maps:get(password, Credential, undefined);
replace_placeholder(?PH_PEERHOST, Credential) ->
    maps:get(peerhost, Credential, undefined);
replace_placeholder(?PH_CERT_SUBJECT, Credential) ->
    maps:get(dn, Credential, undefined);
replace_placeholder(?PH_CERT_CN_NAME, Credential) ->
    maps:get(cn, Credential, undefined);
replace_placeholder(Constant, _) ->
    Constant.

is_superuser(#{<<"is_superuser">> := <<"">>}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := <<"0">>}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := 0}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := null}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := undefined}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := false}) ->
    #{is_superuser => false};
is_superuser(#{<<"is_superuser">> := _}) ->
    #{is_superuser => true};
is_superuser(#{}) ->
    #{is_superuser => false}.

ensure_apps_started(bcrypt) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    ok;
ensure_apps_started(_) ->
    ok.

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.

cleanup_resources() ->
    lists:foreach(
      fun emqx_resource:remove_local/1,
      emqx_resource:list_group_instances(?RESOURCE_GROUP)).

make_resource_id(Name) ->
    NameBin = bin(Name),
    emqx_resource:generate_id(?RESOURCE_GROUP, NameBin).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

convert_to_sql_param(undefined) ->
    null;
convert_to_sql_param(V) ->
    bin(V).
