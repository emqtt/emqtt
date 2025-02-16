%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_conf_schema_inject).

-export([schemas/0]).

schemas() ->
    schemas(emqx_release:edition()).

schemas(Edition) ->
    mria(Edition) ++
        auth_ext(Edition) ++
        cluster_linking(Edition) ++
        authn(Edition) ++
        authz() ++
        shared_subs(Edition) ++
        customized(Edition).

mria(ce) ->
    [];
mria(ee) ->
    [emqx_enterprise_schema].

auth_ext(ce) ->
    [];
auth_ext(ee) ->
    [emqx_auth_ext_schema].

cluster_linking(ce) ->
    [];
cluster_linking(ee) ->
    [emqx_cluster_link_schema].

authn(Edition) ->
    [{emqx_authn_schema, authn_mods(Edition)}].

authn_mods(ce) ->
    [
        emqx_authn_mnesia_schema,
        emqx_authn_mysql_schema,
        emqx_authn_postgresql_schema,
        emqx_authn_mongodb_schema,
        emqx_authn_redis_schema,
        emqx_authn_http_schema,
        emqx_authn_jwt_schema,
        emqx_authn_scram_mnesia_schema,
        emqx_authn_ldap_schema
    ];
authn_mods(ee) ->
    authn_mods(ce) ++
        [
            emqx_gcp_device_authn_schema,
            emqx_authn_scram_restapi_schema,
            emqx_authn_kerberos_schema,
            emqx_authn_cinfo_schema
        ].

authz() ->
    [{emqx_authz_schema, authz_mods()}].

authz_mods() ->
    [
        emqx_authz_file_schema,
        emqx_authz_mnesia_schema,
        emqx_authz_http_schema,
        emqx_authz_redis_schema,
        emqx_authz_mysql_schema,
        emqx_authz_postgresql_schema,
        emqx_authz_mongodb_schema,
        emqx_authz_ldap_schema
    ].

shared_subs(ee) ->
    [emqx_ds_shared_sub_schema];
shared_subs(ce) ->
    [].

%% Add more schemas here.
customized(ee) ->
    [emqx_bridge_mqtt_connector_schema];
customized(ce) ->
    [emqx_bridge_mqtt_connector_schema].
