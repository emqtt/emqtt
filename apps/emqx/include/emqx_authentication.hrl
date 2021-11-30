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

-ifndef(EMQX_AUTHENTICATION_HRL).
-define(EMQX_AUTHENTICATION_HRL, true).

%% config root name all auth providers have to agree on.
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME, "authentication").
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM, authentication).
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY, <<"authentication">>).

%% persistent term key to put all authn config schemas as on HOCON schema type.
%% see emqx_schema.erl for more details
%% and emqx_conf_schema for an examples
-define(EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY, emqx_authentication_schema_module).

-endif.
