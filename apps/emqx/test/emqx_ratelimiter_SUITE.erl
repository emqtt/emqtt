%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ratelimiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, <<"">>).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(RATE(Rate), to_rate(Rate)).
-define(NOW, erlang:system_time(millisecond)).

-define(ZONE, emqx_limiter:internal_allocator()).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    ok = load_conf(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

load_conf() ->
    emqx_common_test_helpers:load_config(emqx_limiter_schema, ?BASE_CONF).

init_config() ->
    emqx_config:init_load(emqx_limiter_schema, ?BASE_CONF).

%%--------------------------------------------------------------------
%% Test Cases Bucket Level
%%--------------------------------------------------------------------
t_check(_) ->
    {ok, Rate} = emqx_limiter_schema:to_rate("1000/s"),
    P1 = emqx_limiter_private:create(#{rate => Rate, burst => 0}),
    {R1, P2} = emqx_limiter:check(1000, P1),
    {R2, P3} = emqx_limiter:check(100, P2),
    timer:sleep(150),
    {R3, _} = emqx_limiter:check(100, P3),
    ?assertEqual(true, R1),
    ?assertEqual(false, R2),
    ?assertEqual(true, R3),

    emqx_limiter_allocator:add_bucket(t_check, #{rate => Rate, burst => 0}),
    {ok, Ref} = emqx_limiter_manager:find_bucket(emqx_limiter:internal_allocator(), t_check),
    S1 = emqx_limiter_shared:create(Ref),
    {SR1, S2} = emqx_limiter:check(1000, S1),
    {SR2, S3} = emqx_limiter:check(100, S2),
    timer:sleep(150),
    {SR3, _} = emqx_limiter:check(100, S3),
    ?assertEqual(true, SR1),
    ?assertEqual(false, SR2),
    ?assertEqual(true, SR3),

    emqx_limiter_allocator:delete_bucket(t_check),
    ok.

t_restore(_) ->
    {ok, Rate} = emqx_limiter_schema:to_rate("1000/s"),
    P1 = emqx_limiter_private:create(#{rate => Rate, burst => 0}),
    {R1, P2} = emqx_limiter:check(1000, P1),
    P3 = emqx_limiter:restore(100, P2),
    {R3, _} = emqx_limiter:check(100, P3),
    ?assertEqual(true, R1),
    ?assertEqual(true, R3),

    emqx_limiter_allocator:add_bucket(t_check, #{rate => 100, burst => 0}),
    {ok, Ref} = emqx_limiter_manager:find_bucket(emqx_limiter:internal_allocator(), t_check),
    S1 = emqx_limiter_shared:create(Ref),
    {SR1, S2} = emqx_limiter:check(1000, S1),
    S3 = emqx_limiter:restore(100, S2),
    {SR3, _} = emqx_limiter:check(100, S3),
    ?assertEqual(true, SR1),
    ?assertEqual(true, SR3),

    emqx_limiter_allocator:delete_bucket(t_check),
    ok.

t_capacity(_) ->
    {ok, Rate} = emqx_limiter_schema:to_rate("2000/s"),
    ?assertMatch(#{capacity := 2000}, emqx_limiter_private:create(#{rate => Rate, burst => 0})),
    ok.

%%--------------------------------------------------------------------
%% Test Cases container
%%--------------------------------------------------------------------
t_create_with_undefined(_) ->
    ?assertEqual(undefined, emqx_limiter_container:create_by_names([messages], undefined, ?ZONE)).

t_create_with_empty(_) ->
    ?assertEqual(undefined, emqx_limiter_container:create_by_names([messages], #{}, ?ZONE)).

t_create_with_private_only(_) ->
    C = emqx_limiter_container:create_by_names([messages], #{messages_rate => 1}, ?ZONE),
    #{messages := MList} = C,
    ?assertEqual(1, erlang:length(MList)),
    ok.

t_create_with_shard_only(_) ->
    emqx_limiter_allocator:add_bucket(messages, #{rate => 100, burst => 0}),
    C = emqx_limiter_container:create_by_names([messages], #{}, ?ZONE),
    #{messages := MList} = C,
    ?assertEqual(1, erlang:length(MList)),
    emqx_limiter_allocator:delete_bucket(messages),
    ok.

t_create_with_both(_) ->
    emqx_limiter_allocator:add_bucket(messages, #{rate => 100, burst => 0}),
    C = emqx_limiter_container:create_by_names([messages], #{messages_rate => 1}, ?ZONE),
    #{messages := MList} = C,
    ?assertEqual(2, erlang:length(MList)),
    emqx_limiter_allocator:delete_bucket(messages),
    ok.

t_create_with_types(_) ->
    emqx_limiter_allocator:add_bucket(messages, #{rate => 100, burst => 0}),
    C = emqx_limiter_container:create_by_names(
        [messages, bytes], #{messages_rate => 1, bytes_rate => 1}, ?ZONE
    ),
    #{messages := MList} = C,
    ?assertEqual(2, erlang:length(MList)),
    #{bytes := BList} = C,
    ?assertEqual(1, erlang:length(BList)),
    emqx_limiter_allocator:delete_bucket(messages),
    ok.

t_check_container(_) ->
    {ok, MRate} = emqx_limiter_schema:to_rate("1000/s"),
    {ok, BRate} = emqx_limiter_schema:to_rate("500/s"),
    C = emqx_limiter_container:create_by_names(
        [messages, bytes],
        #{
            messages_rate => MRate,
            bytes_rate => BRate
        },
        ?ZONE
    ),
    {R1, C1} = emqx_limiter_container:check([{messages, 1000}, {bytes, 500}], C),
    {R2, C2} = emqx_limiter_container:check([{messages, 10}, {bytes, 10}], C1),
    timer:sleep(100),
    {R3, C3} = emqx_limiter_container:check([{messages, 100}, {bytes, 50}], C2),
    {R4, C4} = emqx_limiter_container:check([{messages, 10}, {bytes, 10}], C3),
    timer:sleep(100),
    {R5, _C5} = emqx_limiter_container:check([{messages, 110}, {bytes, 50}], C4),
    ?assertEqual(true, R1),
    ?assertEqual(false, R2),
    ?assertEqual(true, R3),
    ?assertEqual(false, R4),
    ?assertEqual(false, R5),
    ok.

%%--------------------------------------------------------------------
%% Test Cases misc
%%--------------------------------------------------------------------

t_schema_unit(_) ->
    M = emqx_limiter_schema,
    ?assertEqual(limiter, M:namespace()),

    %% infinity
    ?assertEqual({ok, infinity}, M:to_rate(" infinity ")),

    %% xMB
    ?assertMatch({ok, _}, M:to_rate("100")),
    ?assertMatch({ok, _}, M:to_rate("  100   ")),
    ?assertMatch({ok, _}, M:to_rate("100MB")),

    %% xMB/s
    ?assertMatch({ok, _}, M:to_rate("100/s")),
    ?assertMatch({ok, _}, M:to_rate("100MB/s")),

    %% xMB/ys
    ?assertMatch({ok, _}, M:to_rate("100/10s")),
    ?assertMatch({ok, _}, M:to_rate("100MB/10s")),

    ?assertMatch({error, _}, M:to_rate("infini")),
    ?assertMatch({error, _}, M:to_rate("0")),
    ?assertMatch({error, _}, M:to_rate("MB")),
    ?assertMatch({error, _}, M:to_rate("10s")),
    ?assertMatch({error, _}, M:to_rate("100MB/")),
    ?assertMatch({error, _}, M:to_rate("100MB/xx")),
    ?assertMatch({error, _}, M:to_rate("100MB/1")),
    ?assertMatch({error, _}, M:to_rate("100/10x")),

    ?assertEqual({ok, infinity}, M:to_capacity("infinity")),
    ?assertEqual({ok, 100}, M:to_capacity("100")),
    ?assertEqual({ok, 100 * 1024}, M:to_capacity("100KB")),
    ?assertEqual({ok, 100 * 1024 * 1024}, M:to_capacity("100MB")),
    ?assertEqual({ok, 100 * 1024 * 1024 * 1024}, M:to_capacity("100GB")),
    ok.
