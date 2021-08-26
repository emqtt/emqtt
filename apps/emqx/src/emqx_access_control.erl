%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_access_control).

-include("emqx.hrl").

-export([ authenticate/1
        , authorize/3
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(authenticate(emqx_types:clientinfo()) ->
    {ok, map()} | {ok, map(), binary()} | {continue, map()} | {continue, binary(), map()} | {error, term()}).
authenticate(Credential) ->
    case run_hooks('client.authenticate', [Credential], {ok, #{superuser => false}}) of
        ok ->
            {ok, #{superuser => false}};
        Other ->
            Other
    end.

%% @doc Check Authorization
-spec authorize(emqx_types:clientinfo(), emqx_types:pubsub(), emqx_types:topic())
      -> allow | deny.
authorize(ClientInfo = #{zone := Zone}, PubSub, Topic) ->
    case emqx_authz_cache:is_enabled(Zone) of
        true  -> check_authorization_cache(ClientInfo, PubSub, Topic);
        false -> do_authorize(ClientInfo, PubSub, Topic)
    end.

check_authorization_cache(ClientInfo = #{zone := Zone}, PubSub, Topic) ->
    case emqx_authz_cache:get_authz_cache(Zone, PubSub, Topic) of
        not_found ->
            AuthzResult = do_authorize(ClientInfo, PubSub, Topic),
            emqx_authz_cache:put_authz_cache(Zone, PubSub, Topic, AuthzResult),
            AuthzResult;
        AuthzResult -> AuthzResult
    end.

do_authorize(ClientInfo = #{zone := Zone}, PubSub, Topic) ->
    NoMatch = emqx_config:get_zone_conf(Zone, [authorization, no_match]),
    case run_hooks('client.authorize', [ClientInfo, PubSub, Topic], NoMatch) of
        allow  -> allow;
        _Other -> deny
    end.

-compile({inline, [run_hooks/3]}).
run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run_fold(Name, Args, Acc).
