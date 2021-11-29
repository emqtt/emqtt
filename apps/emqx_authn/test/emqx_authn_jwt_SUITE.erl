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

-module(emqx_authn_jwt_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_authn.hrl").

-define(AUTHN_ID, <<"mechanism:jwt">>).

-define(JWKS_PORT, 33333).
-define(JWKS_PATH, "/jwks.json").


all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_authn]),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_authn]),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_jwt_authenticator_hmac_based(_) ->
    Secret = <<"abcdef">>,
    Config = #{mechanism => jwt,
               use_jwks => false,
               algorithm => hmac_based,
               secret => Secret,
               secret_base64_encoded => false,
               verify_claims => []},
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>},
    JWS = generate_jws(hmac_based, Payload, Secret),
    Credential = #{username => <<"myuser">>,
			       password => JWS},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State)),

    Payload1 = #{<<"username">> => <<"myuser">>, <<"is_superuser">> => true},
    JWS1 = generate_jws(hmac_based, Payload1, Secret),
    Credential1 = #{username => <<"myuser">>,
			        password => JWS1},
    ?assertEqual({ok, #{is_superuser => true}}, emqx_authn_jwt:authenticate(Credential1, State)),

    BadJWS = generate_jws(hmac_based, Payload, <<"bad_secret">>),
    Credential2 = Credential#{password => BadJWS},
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential2, State)),

    %% secret_base64_encoded
    Config2 = Config#{secret => base64:encode(Secret),
                      secret_base64_encoded => true},
    {ok, State2} = emqx_authn_jwt:update(Config2, State),
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State2)),

    %% invalid secret
    BadConfig = Config#{secret => <<"emqxsecret">>,
                        secret_base64_encoded => true},
    {error, {invalid_parameter, secret}} = emqx_authn_jwt:create(?AUTHN_ID, BadConfig),

    Config3 = Config#{verify_claims => [{<<"username">>, <<"${username}">>}]},
    {ok, State3} = emqx_authn_jwt:update(Config3, State2),
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State3)),
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential#{username => <<"otheruser">>}, State3)),

    %% Expiration
    Payload3 = #{ <<"username">> => <<"myuser">>
                , <<"exp">> => erlang:system_time(second) - 60},
    JWS3 = generate_jws(hmac_based, Payload3, Secret),
    Credential3 = Credential#{password => JWS3},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential3, State3)),

    Payload4 = #{ <<"username">> => <<"myuser">>
                , <<"exp">> => erlang:system_time(second) + 60},
    JWS4 = generate_jws(hmac_based, Payload4, Secret),
    Credential4 = Credential#{password => JWS4},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential4, State3)),

    %% Issued At
    Payload5 = #{ <<"username">> => <<"myuser">>
                , <<"iat">> => erlang:system_time(second) - 60},
    JWS5 = generate_jws(hmac_based, Payload5, Secret),
    Credential5 = Credential#{password => JWS5},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential5, State3)),

    Payload6 = #{ <<"username">> => <<"myuser">>
                , <<"iat">> => erlang:system_time(second) + 60},
    JWS6 = generate_jws(hmac_based, Payload6, Secret),
    Credential6 = Credential#{password => JWS6},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential6, State3)),

    %% Not Before
    Payload7 = #{ <<"username">> => <<"myuser">>
                , <<"nbf">> => erlang:system_time(second) - 60},
    JWS7 = generate_jws(hmac_based, Payload7, Secret),
    Credential7 = Credential6#{password => JWS7},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential7, State3)),

    Payload8 = #{ <<"username">> => <<"myuser">>
                , <<"nbf">> => erlang:system_time(second) + 60},
    JWS8 = generate_jws(hmac_based, Payload8, Secret),
    Credential8 = Credential#{password => JWS8},
    ?assertEqual({error, bad_username_or_password}, emqx_authn_jwt:authenticate(Credential8, State3)),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State3)),
    ok.

t_jwt_authenticator_public_key(_) ->
    PublicKey = test_rsa_key(public),
    PrivateKey = test_rsa_key(private),
    Config = #{mechanism => jwt,
               use_jwks => false,
               algorithm => public_key,
               certificate => PublicKey,
               verify_claims => []},
    {ok, State} = emqx_authn_jwt:create(?AUTHN_ID, Config),

    Payload = #{<<"username">> => <<"myuser">>},
    JWS = generate_jws(public_key, Payload, PrivateKey),
    Credential = #{username => <<"myuser">>,
			       password => JWS},
    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State)),
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential#{password => <<"badpassword">>}, State)),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State)),
    ok.

t_jwks_renewal(_Config) ->
    ok = emqx_authn_http_test_server:start(?JWKS_PORT, ?JWKS_PATH),
    ok = emqx_authn_http_test_server:set_handler(fun jwks_handler/2),

    PrivateKey = test_rsa_key(private),
    Payload = #{<<"username">> => <<"myuser">>},
    JWS = generate_jws(public_key, Payload, PrivateKey),
    Credential = #{username => <<"myuser">>,
			       password => JWS},

    BadConfig = #{mechanism => jwt,
                  algorithm => public_key,
                  ssl => #{enable => false},
                  verify_claims => [],

                  use_jwks => true,
                  endpoint => "http://127.0.0.1:" ++ integer_to_list(?JWKS_PORT + 1) ++ ?JWKS_PATH,
                  refresh_interval => 1000
                 },

    ok = snabbkaffe:start_trace(),

    {{ok, State0}, _} = ?wait_async_action(
                           emqx_authn_jwt:create(?AUTHN_ID, BadConfig),
                           #{?snk_kind := jwks_endpoint_response},
                           1000),

    ok = snabbkaffe:stop(),

    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential, State0)),
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential#{password => <<"badpassword">>}, State0)),

    GoodConfig = BadConfig#{endpoint =>
                            "http://127.0.0.1:" ++ integer_to_list(?JWKS_PORT) ++ ?JWKS_PATH},

    ok = snabbkaffe:start_trace(),

    {{ok, State1}, _} = ?wait_async_action(
                           emqx_authn_jwt:update(GoodConfig, State0),
                           #{?snk_kind := jwks_endpoint_response},
                           1000),

    ok = snabbkaffe:stop(),

    ?assertEqual({ok, #{is_superuser => false}}, emqx_authn_jwt:authenticate(Credential, State1)),
    ?assertEqual(ignore, emqx_authn_jwt:authenticate(Credential#{password => <<"badpassword">>}, State1)),

    ?assertEqual(ok, emqx_authn_jwt:destroy(State1)),
    ok = emqx_authn_http_test_server:stop().

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

jwks_handler(Req0, State) ->
    JWK = jose_jwk:from_pem_file(test_rsa_key(public)),
    JWKS = jose_jwk_set:to_map([JWK], #{}),
    Req = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"application/json">>},
            jiffy:encode(JWKS),
            Req0),
    {ok, Req, State}.

test_rsa_key(public) ->
    Dir = code:lib_dir(emqx_authn, test),
    list_to_binary(filename:join([Dir, "data/public_key.pem"]));

test_rsa_key(private) ->
    Dir = code:lib_dir(emqx_authn, test),
    list_to_binary(filename:join([Dir, "data/private_key.pem"])).

generate_jws(hmac_based, Payload, Secret) ->
    JWK = jose_jwk:from_oct(Secret),
    Header = #{ <<"alg">> => <<"HS256">>
              , <<"typ">> => <<"JWT">>
              },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS;
generate_jws(public_key, Payload, PrivateKey) ->
    JWK = jose_jwk:from_pem_file(PrivateKey),
    Header = #{ <<"alg">> => <<"RS256">>
              , <<"typ">> => <<"JWT">>
              },
    Signed = jose_jwt:sign(JWK, Header, Payload),
    {_, JWS} = jose_jws:compact(Signed),
    JWS.
