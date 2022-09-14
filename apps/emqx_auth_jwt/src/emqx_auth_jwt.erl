%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_jwt).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[JWT]").

-export([ check_auth/3
        , check_acl/5
        , description/0
        ]).

-export([string_to_number/1]).

%%--------------------------------------------------------------------
%% Authentication callbacks
%%--------------------------------------------------------------------

check_auth(ClientInfo, AuthResult, #{from := From, checklists := Checklists}) ->
    case maps:find(From, ClientInfo) of
        error ->
            ok;
        {ok, undefined} ->
            ok;
        {ok, Token} ->
            case emqx_auth_jwt_svr:verify(Token) of
                {error, not_found} ->
                    ok;
                {error, not_token} ->
                    ok;
                {error, Reason} ->
                    {stop, AuthResult#{auth_result => Reason, anonymous => false}};
                {ok, Claims} ->
                    {stop, maps:merge(AuthResult, verify_claims(Checklists, Claims, ClientInfo))}
            end
    end.

check_acl(ClientInfo = #{jwt_claims := Claims},
          PubSub,
          Topic,
          _NoMatchAction,
          #{acl_claim_name := AclClaimName}) ->
    case Claims of
        #{AclClaimName := Acl, <<"exp">> := Exp} ->
            case is_expired(Exp) of
                true ->
                    ?DEBUG("acl_deny_due_to_jwt_expired", []),
                    deny;
                false ->
                    verify_acl(ClientInfo, Acl, PubSub, Topic)
            end;
        #{AclClaimName := Acl} ->
            verify_acl(ClientInfo, Acl, PubSub, Topic);
        _ ->
            ?DEBUG("no_acl_jwt_claim", []),
            ignore
    end;
check_acl(_ClientInfo,
          _PubSub,
          _Topic,
          _NoMatchAction,
          _AclEnv) ->
    ignore.

is_expired(Exp) when is_binary(Exp)  ->
    case string_to_number(Exp) of
        {ok, Val} ->
            is_expired(Val);
        _ ->
            ?DEBUG("acl_deny_due_to_invalid_jwt_exp:~p", [Exp]),
            true
    end;
is_expired(Exp) when is_number(Exp) ->
    Now = erlang:system_time(second),
    Now > Exp;
is_expired(Exp) ->
    ?DEBUG("acl_deny_due_to_invalid_jwt_exp:~p", [Exp]),
    true.

description() -> "Authentication with JWT".

string_to_number(Bin) when is_binary(Bin) ->
    string_to_number(Bin, fun erlang:binary_to_integer/1, fun erlang:binary_to_float/1);
string_to_number(Str) when is_list(Str) ->
    string_to_number(Str, fun erlang:list_to_integer/1, fun erlang:list_to_float/1);
string_to_number(_) ->
    false.

%%------------------------------------------------------------------------------
%% Verify Claims
%%--------------------------------------------------------------------

verify_acl(ClientInfo, #{<<"sub">> := SubTopics}, subscribe, Topic) when is_list(SubTopics) ->
    verify_acl(ClientInfo, SubTopics, Topic);
verify_acl(ClientInfo, #{<<"pub">> := PubTopics}, publish, Topic) when is_list(PubTopics) ->
    verify_acl(ClientInfo, PubTopics, Topic);
verify_acl(_ClientInfo, _Acl, _PubSub, _Topic) -> {stop, deny}.

verify_acl(_ClientInfo, [], _Topic) -> {stop, deny};
verify_acl(ClientInfo, [AclTopic | AclTopics], Topic) ->
    case match_topic(ClientInfo, AclTopic, Topic) of
        true -> {stop, allow};
        false -> verify_acl(ClientInfo, AclTopics, Topic)
    end.

verify_claims(Checklists, Claims, ClientInfo) ->
    case do_verify_claims(feedvar(Checklists, ClientInfo), Claims) of
        {error, Reason} ->
            #{auth_result => Reason, anonymous => false};
        ok ->
            #{auth_result => success, anonymous => false, jwt_claims => Claims}
    end.

do_verify_claims([], _Claims) ->
    ok;
do_verify_claims([{Key, Expected} | L], Claims) ->
    case maps:get(Key, Claims, undefined) =:= Expected of
        true -> do_verify_claims(L, Claims);
        false -> {error, {verify_claim_failed, Key}}
    end.

feedvar(Checklists, #{username := Username, clientid := ClientId}) ->
    lists:map(fun({K, <<"%u">>}) -> {K, Username};
                 ({K, <<"%c">>}) -> {K, ClientId};
                 ({K, Expected}) -> {K, Expected}
              end, Checklists).

match_topic(ClientInfo, AclTopic, Topic) ->
    AclTopicWords = emqx_topic:words(AclTopic),
    TopicWords = emqx_topic:words(Topic),
    AclTopicRendered = emqx_access_rule:feed_var(ClientInfo, AclTopicWords),
    emqx_topic:match(TopicWords, AclTopicRendered).

string_to_number(Str, IntFun, FloatFun) ->
    try
        {ok, IntFun(Str)}
    catch _:_ ->
        try
            {ok, FloatFun(Str)}
        catch _:_ ->
            false
        end
    end.
