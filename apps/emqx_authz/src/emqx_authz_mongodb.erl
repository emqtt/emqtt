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

-module(emqx_authz_mongodb).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").

-behaviour(emqx_authz).

%% AuthZ Callbacks
-export([ description/0
        , init/1
        , destroy/1
        , dry_run/1
        , authorize/4
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

description() ->
    "AuthZ with MongoDB".

init(Source) ->
    case emqx_authz_utils:create_resource(emqx_connector_mongo, Source) of
        {error, Reason} -> error({load_config_error, Reason});
        {ok, Id} -> Source#{annotations => #{id => Id}}
    end.

dry_run(Source) ->
    emqx_resource:create_dry_run(emqx_connector_mongo, Source).

destroy(#{annotations := #{id := Id}}) ->
    ok = emqx_resource:remove(Id).

authorize(Client, PubSub, Topic,
            #{collection := Collection,
              selector := Selector,
              annotations := #{id := ResourceID}
             }) ->
    case emqx_resource:query(ResourceID, {find, Collection, replvar(Selector, Client), #{}}) of
        {error, Reason} ->
            ?SLOG(error, #{msg => "query_mongo_error",
                           reason => Reason,
                           resource_id => ResourceID}),
            nomatch;
        [] -> nomatch;
        Rows ->
            Rules = [ emqx_authz_rule:compile({Permission, all, Action, Topics})
                     || #{<<"topics">> := Topics,
                          <<"permission">> := Permission,
                          <<"action">> := Action} <- Rows],
            do_authorize(Client, PubSub, Topic, Rules)
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    case emqx_authz_rule:match(Client, PubSub, Topic, Rule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

replvar(Selector, #{clientid := Clientid,
                    username := Username,
                    peerhost := IpAddress
                   }) ->
    Fun = fun
              InFun(K, V, AccIn) when is_map(V) ->
                  maps:put(K, maps:fold(InFun, AccIn, V), AccIn);
              InFun(K, V, AccIn) when is_list(V) ->
                  maps:put(K, [ begin
                                    [{K1, V1}] = maps:to_list(M),
                                    InFun(K1, V1, AccIn)
                                end || M <- V],
                           AccIn);
              InFun(K, V, AccIn) when is_binary(V) ->
                  V1 = re:replace( V,  emqx_authz:ph_to_re(?PH_S_CLIENTID)
                                 , bin(Clientid), [global, {return, binary}]),
                  V2 = re:replace( V1, emqx_authz:ph_to_re(?PH_S_USERNAME)
                                 , bin(Username), [global, {return, binary}]),
                  V3 = re:replace( V2, emqx_authz:ph_to_re(?PH_S_PEERHOST)
                                 , inet_parse:ntoa(IpAddress), [global, {return, binary}]),
                  maps:put(K, V3, AccIn);
              InFun(K, V, AccIn) -> maps:put(K, V, AccIn)
          end,
    maps:fold(Fun, #{}, Selector).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L);
bin(X) -> X.
