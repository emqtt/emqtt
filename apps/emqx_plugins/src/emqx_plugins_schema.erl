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

-module(emqx_plugins_schema).

-behaviour(hocon_schema).

-export([ roots/0
        , fields/1
        , namespace/0
        ]).

-include_lib("typerefl/include/types.hrl").
-include("emqx_plugins.hrl").

namespace() -> "plugin".

roots() -> [?CONF_ROOT].

fields(?CONF_ROOT) ->
    #{fields => root_fields(),
      desc => """
Manage EMQ X plugins.
<br>
Plugins can be pre-built as a part of EMQ X package,
or installed as a standalone package in a location specified by
<code>install_dir</code> config key
<br>
The standalone-installed plugins are referred to as 'external' plugins.
"""
     };
fields(state) ->
    #{ fields => state_fields(),
       desc => "A per-plugin config to describe the desired state of the plugin."
     }.

state_fields() ->
    [ {name_vsn,
       hoconsc:mk(string(),
                  #{ desc => "The {name}-{version} of the plugin.<br>"
                             "It should match the plugin application name-vsn as the "
                             "for the plugin release package name<br>"
                             "For example: my_plugin-0.1.0."
                   , nullable => false
                   })}
    , {enable,
       hoconsc:mk(boolean(),
                  #{ desc => "Set to 'true' to enable this plugin"
                   , nullable => false
                   })}
    ].

root_fields() ->
    [ {states, fun states/1}
    , {install_dir, fun install_dir/1}
    ].

states(type) -> hoconsc:array(hoconsc:ref(state));
states(nullable) -> true;
states(default) -> [];
states(desc) -> "An array of plugins in the desired states.<br>"
                "The plugins are started in the defined order";
states(_) -> undefined.

install_dir(type) -> string();
install_dir(nullable) -> true;
install_dir(default) -> "plugins"; %% runner's root dir
install_dir(T) when T =/= desc -> undefined;
install_dir(desc) -> """
In which directory are the external plugins installed.
The plugin beam files and configuration files should reside in
the sub-directory named as <code>emqx_foo_bar-0.1.0</code>.
<br>
NOTE: For security reasons, this directory should **NOT** be writable
by anyone expect for <code>emqx</code> (or any user which runs EMQ X)
""".
