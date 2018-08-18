%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_access_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include_lib("common_test/include/ct.hrl").

-define(AC, emqx_access_control).

-import(emqx_access_rule, [compile/1, match/3]).

all() ->
    [{group, access_control},
     {group, acl_cache},
     {group, access_control_cache_mode},
     {group, access_rule}
     ].

groups() ->
    [{access_control, [sequence],
       [reload_acl,
        register_mod,
        unregister_mod,
        check_acl_1,
        check_acl_2
        ]},
     {access_control_cache_mode, [],
       [
        acl_cache_basic,
        acl_cache_expiry,
        acl_cache_cleanup,
        acl_cache_full
        ]},
     {acl_cache, [], [
       put_get_del_cache,
       cache_update,
       cache_expiry,
       cache_full_replacement,
       cache_cleanup,
       cache_full_cleanup
     ]},
     {access_rule, [],
       [compile_rule,
        match_rule]}].

init_per_group(Group, Config) when  Group =:= access_control;
                                    Group =:= access_control_cache_mode ->
    prepare_config(Group),
    application:load(emqx),
    Config;
init_per_group(_Group, Config) ->
    Config.

prepare_config(Group = access_control) ->
    set_acl_config_file(Group),
    application:set_env(emqx, acl_cache_size, 0);
prepare_config(Group = access_control_cache_mode) ->
    set_acl_config_file(Group),
    application:set_env(emqx, acl_cache_size, 100).

set_acl_config_file(_Group) ->
    Rules = [{allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]},
             {allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]},
             {allow, {user, "admin"}, pubsub, ["a/b/c", "d/e/f/#"]},
             {allow, {client, "testClient"}, subscribe, ["testTopics/testClient"]},
             {allow, all, subscribe, ["clients/%c"]},
             {allow, all, pubsub, ["users/%u/#"]},
             {deny, all, subscribe, ["$SYS/#", "#"]},
             {deny, all}],
    write_config("access_SUITE_acl.conf", Rules),
    application:set_env(emqx, acl_file, "access_SUITE_acl.conf").


write_config(Filename, Terms) ->
    file:write_file(Filename, [io_lib:format("~tp.~n", [Term]) || Term <- Terms]).

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    {ok, _Pid} = ?AC:start_link(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    ok.

per_testcase_config(acl_cache_full, Config) ->
    Config;
per_testcase_config(_TestCase, Config) ->
    Config.


%%--------------------------------------------------------------------
%% emqx_access_control
%%--------------------------------------------------------------------

reload_acl(_) ->
    [ok] = ?AC:reload_acl().

register_mod(_) ->
    ok = ?AC:register_mod(acl, emqx_acl_test_mod, []),
    {error, already_existed} = ?AC:register_mod(acl, emqx_acl_test_mod, []),
    {emqx_acl_test_mod, _, 0} = hd(?AC:lookup_mods(acl)),
    ok = ?AC:register_mod(auth, emqx_auth_anonymous_test_mod,[]),
    ok = ?AC:register_mod(auth, emqx_auth_dashboard, [], 99),
    [{emqx_auth_dashboard, _, 99},
     {emqx_auth_anonymous_test_mod, _, 0}] = ?AC:lookup_mods(auth).

unregister_mod(_) ->
    ok = ?AC:register_mod(acl, emqx_acl_test_mod, []),
    {emqx_acl_test_mod, _, 0} = hd(?AC:lookup_mods(acl)),
    ok = ?AC:unregister_mod(acl, emqx_acl_test_mod),
    timer:sleep(5),
    {emqx_acl_internal, _, 0}= hd(?AC:lookup_mods(acl)),
    ok = ?AC:register_mod(auth, emqx_auth_anonymous_test_mod,[]),
    [{emqx_auth_anonymous_test_mod, _, 0}] = ?AC:lookup_mods(auth),

    ok = ?AC:unregister_mod(auth, emqx_auth_anonymous_test_mod),
    timer:sleep(5),
    [] = ?AC:lookup_mods(auth).

check_acl_1(_) ->
    SelfUser = #client{id = <<"client1">>, username = <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),
    deny = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1/x/y">>),
    allow = ?AC:check_acl(SelfUser, publish, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"a/b/c">>).
check_acl_2(_) ->
    SelfUser = #client{id = <<"client2">>, username = <<"xyz">>},
    deny = ?AC:check_acl(SelfUser, subscribe, <<"a/b/c">>).

acl_cache_basic(_) ->
    SelfUser = #client{id = <<"client1">>, username = <<"testuser">>},
    not_found = ?AC:get_acl_cache(subscribe, <<"users/testuser/1">>),
    not_found = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),

    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    allow = ?AC:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_expiry(_) ->
    application:set_env(emqx, acl_cache_ttl, 1000),

    SelfUser = #client{id = <<"client1">>, username = <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),
    allow = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),
    ct:sleep(1100),
    not_found = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_full(_) ->
    application:set_env(emqx, acl_cache_size, 1),

    SelfUser = #client{id = <<"client1">>, username = <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    %% the older ones (the <<"users/testuser/1">>) will be evicted first
    not_found = ?AC:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),
    ok.

acl_cache_cleanup(_) ->
    %% The acl cache will try to evict memory, if the size is full and the newest
    %%   cache entry is expired
    application:set_env(emqx, acl_cache_ttl, 1000),
    application:set_env(emqx, acl_cache_size, 2),

    SelfUser = #client{id = <<"client1">>, username = <<"testuser">>},
    allow = ?AC:check_acl(SelfUser, subscribe, <<"users/testuser/1">>),
    allow = ?AC:check_acl(SelfUser, subscribe, <<"clients/client1">>),

    allow = ?AC:get_acl_cache(subscribe, <<"users/testuser/1">>),
    allow = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),

    ct:sleep(1100),
    %% now the cache is full and the newest one - "clients/client1"
    %%  should be expired, so we'll try to cleanup before putting the next cache entry
    deny = ?AC:check_acl(SelfUser, subscribe, <<"#">>),

    not_found = ?AC:get_acl_cache(subscribe, <<"users/testuser/1">>),
    not_found = ?AC:get_acl_cache(subscribe, <<"clients/client1">>),
    deny = ?AC:get_acl_cache(subscribe, <<"#">>),
    ok.

put_get_del_cache(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_size, 30),

    not_found = ?AC:get_acl_cache(publish, <<"a">>),
    ok = ?AC:put_acl_cache(publish, <<"a">>, allow),
    allow = ?AC:get_acl_cache(publish, <<"a">>),

    not_found = ?AC:get_acl_cache(subscribe, <<"b">>),
    ok = ?AC:put_acl_cache(subscribe, <<"b">>, deny),
    deny = ?AC:get_acl_cache(subscribe, <<"b">>),

    2 = ?AC:get_cache_size(),
    {subscribe, <<"b">>} = ?AC:get_newest_key().

cache_expiry(_) ->
    application:set_env(emqx, acl_cache_ttl, 1000),
    application:set_env(emqx, acl_cache_size, 30),
    ok = ?AC:put_acl_cache(subscribe, <<"a">>, allow),
    allow = ?AC:get_acl_cache(subscribe, <<"a">>),

    ct:sleep(1100),
    not_found = ?AC:get_acl_cache(subscribe, <<"a">>),

    ok = ?AC:put_acl_cache(subscribe, <<"a">>, deny),
    deny = ?AC:get_acl_cache(subscribe, <<"a">>),

    ct:sleep(1100),
    not_found = ?AC:get_acl_cache(subscribe, <<"a">>).

cache_update(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_size, 30),
    [] = ?AC:dump_acl_cache(),

    ok = ?AC:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"b">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"c">>, allow),
    3 = ?AC:get_cache_size(),
    {publish, <<"c">>} = ?AC:get_newest_key(),

    %% update the 2nd one
    ok = ?AC:put_acl_cache(publish, <<"b">>, allow),
    %ct:pal("dump acl cache: ~p~n", [?AC:dump_acl_cache()]),

    3 = ?AC:get_cache_size(),
    {publish, <<"b">>} = ?AC:get_newest_key().

cache_full_replacement(_) ->
    application:set_env(emqx, acl_cache_ttl, 300000),
    application:set_env(emqx, acl_cache_size, 3),
    ok = ?AC:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"b">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"c">>, allow),
    allow = ?AC:get_acl_cache(subscribe, <<"a">>),
    allow = ?AC:get_acl_cache(publish, <<"b">>),
    allow = ?AC:get_acl_cache(publish, <<"c">>),
    3 = ?AC:get_cache_size(),
    {publish, <<"c">>} = ?AC:get_newest_key(),

    ok = ?AC:put_acl_cache(publish, <<"d">>, deny),
    3 = ?AC:get_cache_size(),
    {publish, <<"d">>} = ?AC:get_newest_key(),

    ok = ?AC:put_acl_cache(publish, <<"e">>, deny),
    3 = ?AC:get_cache_size(),
    {publish, <<"e">>} = ?AC:get_newest_key(),

    not_found = ?AC:get_acl_cache(subscribe, <<"a">>),
    not_found = ?AC:get_acl_cache(publish, <<"b">>),
    allow = ?AC:get_acl_cache(publish, <<"c">>).

cache_cleanup(_) ->
    application:set_env(emqx, acl_cache_ttl, 1000),
    application:set_env(emqx, acl_cache_size, 30),
    ok = ?AC:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"b">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"c">>, allow),
    3 = ?AC:get_cache_size(),

    ct:sleep(1100),
    ?AC:cleanup_acl_cache(),
    0 = ?AC:get_cache_size().

cache_full_cleanup(_) ->
    application:set_env(emqx, acl_cache_ttl, 1000),
    application:set_env(emqx, acl_cache_size, 3),
    ok = ?AC:put_acl_cache(subscribe, <<"a">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"b">>, allow),
    ok = ?AC:put_acl_cache(publish, <<"c">>, allow),
    3 = ?AC:get_cache_size(),

    ct:sleep(1100),
    %% verify auto cleanup upon cache full
    ok = ?AC:put_acl_cache(subscribe, <<"d">>, deny),
    1 = ?AC:get_cache_size().

%%--------------------------------------------------------------------
%% emqx_access_rule
%%--------------------------------------------------------------------

compile_rule(_) ->

    {allow, {'and', [{ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}},
                     {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]}),
    {allow, {'or', [{ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}},
                    {user, <<"user">>}]}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"user">>}]}, subscribe, ["$SYS/#", "#"]}),

    {allow, {ipaddr, {{127,0,0,1}, {127,0,0,1}, 32}}, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]}),
    {allow, {user, <<"testuser">>}, subscribe, [ [<<"a">>, <<"b">>, <<"c">>], [<<"d">>, <<"e">>, <<"f">>, '#'] ]} =
        compile({allow, {user, "testuser"}, subscribe, ["a/b/c", "d/e/f/#"]}),
    {allow, {user, <<"admin">>}, pubsub, [ [<<"d">>, <<"e">>, <<"f">>, '#'] ]} =
        compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]}),
    {allow, {client, <<"testClient">>}, publish, [ [<<"testTopics">>, <<"testClient">>] ]} =
        compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]}),
    {allow, all, pubsub, [{pattern, [<<"clients">>, <<"%c">>]}]} =
        compile({allow, all, pubsub, ["clients/%c"]}),
    {allow, all, subscribe, [{pattern, [<<"users">>, <<"%u">>, '#']}]} =
        compile({allow, all, subscribe, ["users/%u/#"]}),
    {deny, all, subscribe, [ [<<"$SYS">>, '#'], ['#'] ]} =
        compile({deny, all, subscribe, ["$SYS/#", "#"]}),
    {allow, all} = compile({allow, all}),
    {deny, all} = compile({deny, all}).

match_rule(_) ->
    User = #client{peername = {{127,0,0,1}, 2948}, id = <<"testClient">>, username = <<"TestUser">>},
    User2 = #client{peername = {{192,168,0,10}, 3028}, id = <<"testClient">>, username = <<"TestUser">>},

    {matched, allow} = match(User, <<"Test/Topic">>, {allow, all}),
    {matched, deny} = match(User, <<"Test/Topic">>, {deny, all}),
    {matched, allow} = match(User, <<"Test/Topic">>, compile({allow, {ipaddr, "127.0.0.1"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User2, <<"Test/Topic">>, compile({allow, {ipaddr, "192.168.0.1/24"}, subscribe, ["$SYS/#", "#"]})),
    {matched, allow} = match(User, <<"d/e/f/x">>, compile({allow, {user, "TestUser"}, subscribe, ["a/b/c", "d/e/f/#"]})),
    nomatch = match(User, <<"d/e/f/x">>, compile({allow, {user, "admin"}, pubsub, ["d/e/f/#"]})),
    {matched, allow} = match(User, <<"testTopics/testClient">>, compile({allow, {client, "testClient"}, publish, ["testTopics/testClient"]})),
    {matched, allow} = match(User, <<"clients/testClient">>, compile({allow, all, pubsub, ["clients/%c"]})),
    {matched, allow} = match(#client{username = <<"user2">>}, <<"users/user2/abc/def">>,
                             compile({allow, all, subscribe, ["users/%u/#"]})),
    {matched, deny} = match(User, <<"d/e/f">>, compile({deny, all, subscribe, ["$SYS/#", "#"]})),
    Rule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, <<"Topic">>}),
    nomatch = match(User, <<"Topic">>, Rule),
    AndRule = compile({allow, {'and', [{ipaddr, "127.0.0.1"}, {user, <<"TestUser">>}]}, publish, <<"Topic">>}),
    {matched, allow} = match(User, <<"Topic">>, AndRule),
    OrRule = compile({allow, {'or', [{ipaddr, "127.0.0.1"}, {user, <<"WrongUser">>}]}, publish, ["Topic"]}),
    {matched, allow} = match(User, <<"Topic">>, OrRule).
