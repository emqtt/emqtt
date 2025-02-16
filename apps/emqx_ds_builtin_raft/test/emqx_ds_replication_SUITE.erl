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
-module(emqx_ds_replication_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("../../emqx/include/emqx.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DB, testdb).

-define(ON(NODES, BODY),
    emqx_ds_test_helpers:on(NODES, fun() -> BODY end)
).

opts(Config, Overrides) ->
    Layout = ?config(layout, Config),
    emqx_utils_maps:deep_merge(
        #{
            backend => builtin_raft,
            storage => Layout,
            n_shards => 16,
            n_sites => 1,
            replication_factor => 3,
            replication_options => #{
                wal_max_size_bytes => 64,
                wal_max_batch_size => 1024,
                snapshot_interval => 128
            }
        },
        Overrides
    ).

appspec(ra) ->
    {ra, #{
        %% NOTE: Recover quicker in case the node sending a snapshot goes down or crash.
        %% TODO: Probably need to have those tighter timeouts as defaults.
        override_env => [{receive_snapshot_timeout, 5_000}]
    }};
appspec(emqx_durable_storage) ->
    {emqx_durable_storage, #{
        before_start => fun snabbkaffe:fix_ct_logging/0,
        override_env => [{egress_flush_interval, 1}]
    }};
appspec(emqx_ds_builtin_raft) ->
    {emqx_ds_builtin_raft, #{
        after_start => fun() -> logger:set_module_level(ra_server, info) end
    }}.

t_metadata(init, Config) ->
    Apps = emqx_cth_suite:start([emqx_ds_builtin_raft], #{
        work_dir => ?config(work_dir, Config)
    }),
    [{apps, Apps} | Config];
t_metadata('end', Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    Config.

t_metadata(_Config) ->
    DB = ?FUNCTION_NAME,
    NShards = 1,
    Options = #{
        backend => builtin_raft,
        storage => {emqx_ds_storage_reference, #{}},
        n_shards => NShards,
        n_sites => 1,
        replication_factor => 1,
        replication_options => #{}
    },
    try
        ?assertMatch(ok, emqx_ds:open_db(DB, Options)),
        %% Check metadata:
        %%    We have only one site:
        [Site] = emqx_ds_replication_layer_meta:sites(),
        %%    Check all shards:
        Shards = emqx_ds_replication_layer_meta:shards(DB),
        %%    Since there is only one site all shards should be allocated
        %%    to this site:
        MyShards = emqx_ds_replication_layer_meta:my_shards(DB),
        ?assertEqual(NShards, length(Shards)),
        lists:foreach(
            fun(Shard) ->
                ?assertEqual(
                    [Site], emqx_ds_replication_layer_meta:replica_set(DB, Shard)
                )
            end,
            Shards
        ),
        ?assertEqual(lists:sort(Shards), lists:sort(MyShards))
    after
        ?assertMatch(ok, emqx_ds:drop_db(DB))
    end.

t_replication_transfers_snapshots(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_replication_transfers_snapshots1, #{apps => Apps}},
            {t_replication_transfers_snapshots2, #{apps => Apps}},
            {t_replication_transfers_snapshots3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    [{nodes, Nodes}, {specs, NodeSpecs} | Config];
t_replication_transfers_snapshots('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_replication_transfers_snapshots(Config) ->
    NMsgs = 400,
    NClients = 5,
    {Stream, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),

    Nodes = [Node, NodeOffline | _] = ?config(nodes, Config),
    _Specs = [_, SpecOffline | _] = ?config(specs, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Initialize DB on all nodes and wait for it to be online.
            Opts = opts(Config, #{n_shards => 1, n_sites => 3}),
            assert_db_open(Nodes, ?DB, Opts),

            %% Stop the DB on the "offline" node.
            ?wait_async_action(
                ok = emqx_cth_cluster:stop_node(NodeOffline),
                #{?snk_kind := ds_ra_state_enter, state := leader},
                5_000
            ),

            %% Fill the storage with messages and few additional generations.
            emqx_ds_test_helpers:apply_stream(?DB, Nodes -- [NodeOffline], Stream),

            %% Restart the node.
            [NodeOffline] = emqx_cth_cluster:restart(SpecOffline),
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{
                    ?snk_kind := dsrepl_snapshot_accepted,
                    ?snk_meta := #{node := NodeOffline}
                })
            ),

            ok = ?ON(
                NodeOffline,
                emqx_ds:open_db(?DB, opts(Config, #{}))
            ),

            %% Trigger storage operation and wait the replica to be restored.
            _ = add_generation(Node, ?DB),
            ?assertMatch(
                {ok, _},
                snabbkaffe:receive_events(SRef)
            ),

            %% Wait until any pending replication activities are finished (e.g. Raft log entries).
            ok = timer:sleep(3_000),

            %% Check that the DB has been restored:
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_preconditions_idempotent(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_preconditions_idempotent1, #{apps => Apps}},
            {t_preconditions_idempotent2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {specs, Specs} | Config];
t_preconditions_idempotent('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_preconditions_idempotent(Config) ->
    C1 = <<"C1">>,
    Topic1 = <<"t/foo">>,
    Topic2 = <<"t/bar/xyz">>,

    Nodes = [N1, N2] = ?config(nodes, Config),
    _Specs = [NS1, _] = ?config(specs, Config),
    Opts = opts(Config, #{
        n_shards => 1,
        n_sites => 2,
        replication_factor => 3,
        append_only => false,
        replication_options => #{
            %% Make sure snapshots are taken eagerly.
            snapshot_interval => 6
        }
    }),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            assert_db_open(Nodes, ?DB, Opts),

            %% Store several messages.
            Messages = [
                message(C1, Topic1, <<"T1/0">>, 0),
                message(C1, Topic2, <<"T2/0">>, 0),
                message(C1, Topic1, <<"T1/1">>, 1),
                message(C1, Topic2, <<"T2/2">>, 1),
                message(C1, Topic1, <<"T1/2">>, 2),
                message(C1, Topic2, <<"T2/2">>, 2),
                message(C1, Topic1, <<"T1/100">>, 100)
            ],
            [ok = ?ON(N2, emqx_ds:store_batch(?DB, [M], #{sync => true})) || M <- Messages],

            %% Add a generation. This will cause the storage layer to flush.
            Since1 = 300,
            ok = ?ON(N2, emqx_ds_replication_layer:add_generation(?DB, Since1)),

            %% Store batches with preconditions.
            Batch1 = #dsbatch{
                preconditions = [
                    %% Appears later, as part of `Batch2`.
                    {if_exists, #message_matcher{
                        from = C1, topic = Topic1, timestamp = 400, payload = '_'
                    }}
                ],
                operations = [
                    message(C1, Topic1, <<"Should not be here">>, 500)
                ]
            },
            ?assertMatch(
                %% No `{Topic1, _TS = 400}` message yet, should fail.
                {error, _, {precondition_failed, _}},
                ?ON(N2, emqx_ds:store_batch(?DB, Batch1, #{sync => true}))
            ),
            Batch2 = [
                message(C1, Topic1, <<"T1/400">>, 400),
                message(C1, Topic2, <<"T2/400">>, 400)
            ],
            ?assertEqual(
                %% Only now `{Topic1, _TS = 400}` should be stored.
                ok,
                ?ON(N2, emqx_ds:store_batch(?DB, Batch2, #{sync => true}))
            ),

            %% Restart N1 and wait until it is ready.
            [N1] = emqx_cth_cluster:restart(NS1),
            RestartedAt1 = erlang:monotonic_time(millisecond),
            ok = ?ON(N1, emqx_ds:open_db(?DB, Opts)),
            SinceRestarted1 = erlang:monotonic_time(millisecond) - RestartedAt1,
            wait_db_bootstrapped([N1], ?DB, infinity, SinceRestarted1),

            %% Both replicas should still contain the same set of messages.
            [N1Msgs1, N2Msgs1] = ?ON(
                Nodes,
                emqx_ds_test_helpers:storage_consume({?DB, <<"0">>}, ['#'])
            ),
            emqx_ds_test_helpers:assert_same_set(N1Msgs1, N2Msgs1),

            Batch3 = #dsbatch{
                preconditions = [
                    %% Exists at this point.
                    {unless_exists, #message_matcher{
                        from = C1, topic = Topic1, timestamp = 400, payload = '_'
                    }}
                ],
                operations = [
                    message(C1, Topic2, <<"Should not be here">>, 500)
                ]
            },
            ?assertMatch(
                %% There is `{Topic1, _TS = 400}` message yet, should fail.
                {error, _, {precondition_failed, _}},
                ?ON(N2, emqx_ds:store_batch(?DB, Batch3, #{sync => true}))
            ),
            Batch4 = [
                {delete, #message_matcher{
                    from = C1, topic = Topic1, timestamp = 400, payload = '_'
                }}
            ],
            ?assertEqual(
                %% Only now `{Topic1, _TS = 400}` should be deleted.
                ok,
                ?ON(N2, emqx_ds:store_batch(?DB, Batch4, #{sync => true}))
            ),

            %% Add one more generation, idempotency should still hold if it's
            %% the last log entry.
            Since2 = 600,
            ok = ?ON(N2, emqx_ds_replication_layer:add_generation(?DB, Since2)),

            %% Restart N1 and wait until it is ready.
            [N1] = emqx_cth_cluster:restart(NS1),
            RestartedAt2 = erlang:monotonic_time(millisecond),
            ok = ?ON(N1, emqx_ds:open_db(?DB, Opts)),
            SinceRestarted2 = erlang:monotonic_time(millisecond) - RestartedAt2,
            wait_db_bootstrapped([N1], ?DB, infinity, SinceRestarted2),

            %% But both replicas should still contain the same set of messages.
            [N1Msgs2, N2Msgs2] = ?ON(
                Nodes,
                emqx_ds_test_helpers:storage_consume({?DB, <<"0">>}, ['#'])
            ),
            emqx_ds_test_helpers:assert_same_set(N1Msgs2, N2Msgs2)
        end,
        fun(Trace) ->
            %% Expect Raft log entries following `add_generation/2` to be reapplied
            %% twice, once per each restart.
            Events = ?of_kind(ds_ra_apply_batch, ?of_node(N1, Trace)),
            ?assertMatch(
                %% Batch1, Batch2, Batch1, Batch2, Batch3, Batch4, Batch1, Batch2, Batch3, Batch4
                [_, _, _, _, _, _, _, _, _, _],
                [E || E = #{latest := L} <- Events, L > (_Since1 = 300)]
            )
        end
    ).

t_rebalance(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance1, #{apps => Apps}},
            {t_rebalance2, #{apps => Apps}},
            {t_rebalance3, #{apps => Apps}},
            {t_rebalance4, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

%% This testcase verifies that the storage rebalancing works correctly:
%% 1. Join/leave operations are applied successfully.
%% 2. Message data survives the rebalancing.
%% 3. Shard cluster membership converges to the target replica allocation.
%% 4. Replication factor is respected.
t_rebalance(Config) ->
    NMsgs = 50,
    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),
    Nodes = [N1, N2 | _] = ?config(nodes, Config),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            Sites = [S1, S2 | _] = [ds_repl_meta(N, this_site) || N <- Nodes],
            %% 1. Initialize DB on the first node.
            Opts = opts(Config, #{n_shards => 16, n_sites => 1, replication_factor => 3}),
            assert_db_open(Nodes, ?DB, Opts),

            %% 1.1 Kick all sites except S1 from the replica set as
            %% the initial condition:
            ?assertMatch(
                {ok, [_]},
                ?ON(N1, emqx_ds_replication_layer_meta:assign_db_sites(?DB, [S1]))
            ),
            ?retry(1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),
            ?retry(500, 10, ?assertMatch(Shards when length(Shards) == 16, shards_online(N1, ?DB))),

            ct:pal("Sites: ~p~n", [Sites]),

            Sequence = [
                %% Join the second site to the DB replication sites:
                {N1, join_db_site, S2},
                %% Should be a no-op:
                {N2, join_db_site, S2},
                %% Now join the rest of the sites:
                {N2, assign_db_sites, Sites}
            ],
            Stream1 = emqx_utils_stream:interleave(
                [
                    {20, Stream0},
                    emqx_utils_stream:const(add_generation)
                ],
                false
            ),
            Stream = emqx_utils_stream:interleave(
                [
                    {50, Stream1},
                    emqx_utils_stream:list(Sequence)
                ],
                true
            ),

            %% 1.2 Verify that all nodes have the same view of metadata storage:
            [
                ?defer_assert(
                    ?assertEqual(
                        [S1],
                        ?ON(Node, emqx_ds_replication_layer_meta:db_sites(?DB)),
                        #{
                            msg => "Initially, only S1 should be responsible for all shards",
                            node => Node
                        }
                    )
                )
             || Node <- Nodes
            ],

            %% 2. Start filling the storage:
            emqx_ds_test_helpers:apply_stream(?DB, Nodes, Stream),
            timer:sleep(5000),
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams),
            [
                ?defer_assert(
                    ?assertEqual(
                        16 * 3 div length(Nodes),
                        n_shards_online(Node, ?DB),
                        "Each node is now responsible for 3/4 of the shards"
                    )
                )
             || Node <- Nodes
            ],

            %% Verify that the set of shard servers matches the target allocation.
            Allocation = [ds_repl_meta(N, my_shards, [?DB]) || N <- Nodes],
            ShardServers = [
                {{Shard, N}, shard_server_info(N, ?DB, Shard, Site, readiness)}
             || {N, Site, Shards} <- lists:zip3(Nodes, Sites, Allocation),
                Shard <- Shards
            ],
            ?assert(
                lists:all(fun({_Server, Status}) -> Status == ready end, ShardServers),
                ShardServers
            ),

            %% Scale down the cluster by removing the first node.
            ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S1])),
            ct:pal("Transitions (~p -> ~p): ~p~n", [
                Sites, tl(Sites), emqx_ds_test_helpers:transitions(N1, ?DB)
            ]),
            ?retry(1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N2, ?DB))),

            %% Verify that at the end each node is now responsible for each shard.
            ?defer_assert(
                ?assertEqual(
                    [0, 16, 16, 16],
                    [n_shards_online(N, ?DB) || N <- Nodes]
                )
            ),

            %% Verify that the messages are once again preserved after the rebalance:
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_join_leave_errors(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_join_leave_errors1, #{apps => Apps}},
            {t_join_leave_errors2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_join_leave_errors('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_join_leave_errors(Config) ->
    %% This testcase verifies that logical errors arising during handling of
    %% join/leave operations are reported correctly.
    DB = ?FUNCTION_NAME,
    [N1, N2] = ?config(nodes, Config),
    Opts = opts(Config, #{n_shards => 16, n_sites => 1, replication_factor => 3}),

    ?check_trace(
        begin
            ?assertEqual(ok, erpc:call(N1, emqx_ds, open_db, [DB, Opts])),
            ?assertEqual(ok, erpc:call(N2, emqx_ds, open_db, [DB, Opts])),

            [S1, S2] = [ds_repl_meta(N, this_site) || N <- [N1, N2]],

            ?assertEqual(
                lists:sort([S1, S2]), lists:sort(ds_repl_meta(N1, db_sites, [DB]))
            ),

            %% Attempts to join a nonexistent DB / site.
            ?assertEqual(
                {error, {nonexistent_db, boo}},
                ds_repl_meta(N1, join_db_site, [_DB = boo, S1])
            ),
            ?assertEqual(
                {error, {nonexistent_sites, [<<"NO-MANS-SITE">>]}},
                ds_repl_meta(N1, join_db_site, [DB, <<"NO-MANS-SITE">>])
            ),
            %% NOTE: Leaving a non-existent site is not an error.
            ?assertEqual(
                {ok, unchanged},
                ds_repl_meta(N1, leave_db_site, [DB, <<"NO-MANS-SITE">>])
            ),

            %% Should be no-op.
            ?assertEqual({ok, unchanged}, ds_repl_meta(N1, join_db_site, [DB, S1])),
            ?assertEqual([], emqx_ds_test_helpers:transitions(N1, DB)),

            %% Leave S2:
            ?assertEqual(
                {ok, [S1]},
                ds_repl_meta(N1, leave_db_site, [DB, S2])
            ),
            %% Impossible to leave the last site:
            ?assertEqual(
                {error, {too_few_sites, []}},
                ds_repl_meta(N1, leave_db_site, [DB, S1])
            ),

            %% "Move" the DB to the other node.
            ?assertMatch({ok, _}, ds_repl_meta(N1, join_db_site, [DB, S2])),
            ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [DB, S1])),
            ?retry(
                1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, DB))
            ),

            %% Should be no-op.
            ?assertMatch({ok, _}, ds_repl_meta(N2, leave_db_site, [DB, S1])),
            ?assertEqual([], emqx_ds_test_helpers:transitions(N1, DB))
        end,
        []
    ).

t_rebalance_chaotic_converges(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_rebalance_chaotic_converges1, #{apps => Apps}},
            {t_rebalance_chaotic_converges2, #{apps => Apps}},
            {t_rebalance_chaotic_converges3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_rebalance_chaotic_converges('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_chaotic_converges(Config) ->
    %% This testcase verifies that even a very chaotic sequence of join/leave
    %% operations will still be handled consistently, and that the shard
    %% allocation will converge to the target state.

    NMsgs = 500,
    Nodes = [N1, N2, N3] = ?config(nodes, Config),

    NClients = 5,
    {Stream0, TopicStreams} = emqx_ds_test_helpers:interleaved_topic_messages(
        ?FUNCTION_NAME, NClients, NMsgs
    ),

    ?check_trace(
        #{},
        begin
            Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
            ct:pal("Sites: ~p~n", [Sites]),

            %% Initialize DB on first two nodes.
            Opts = opts(Config, #{n_shards => 16, n_sites => 2, replication_factor => 3}),

            %% Open DB:
            assert_db_open(Nodes, ?DB, Opts),

            %% Kick N3 from the replica set as the initial condition:
            ?assertMatch(
                {ok, [_, _]},
                ?ON(N1, emqx_ds_replication_layer_meta:assign_db_sites(?DB, [S1, S2]))
            ),
            ?retry(1000, 10, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),

            Sequence = [
                {N1, join_db_site, S3},
                {N2, leave_db_site, S2},
                {N3, leave_db_site, S1},
                {N1, join_db_site, S2},
                {N2, join_db_site, S1},
                {N3, leave_db_site, S3},
                {N1, leave_db_site, S1},
                {N2, join_db_site, S3}
            ],

            %% Interleaved list of events:
            Stream = emqx_utils_stream:interleave(
                [
                    {50, Stream0},
                    emqx_utils_stream:list(Sequence)
                ],
                true
            ),

            ?retry(500, 10, ?assertEqual([16, 16], [n_shards_online(N, ?DB) || N <- [N1, N2]])),
            ?assertEqual(
                lists:sort([S1, S2]),
                ds_repl_meta(N1, db_sites, [?DB]),
                "Initially, the DB is assigned to [S1, S2]"
            ),

            emqx_ds_test_helpers:apply_stream(?DB, Nodes, Stream),

            %% Wait for the last transition to complete.
            ?retry(1000, 30, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),

            ?defer_assert(
                ?assertEqual(
                    lists:sort([S2, S3]),
                    ds_repl_meta(N1, db_sites, [?DB])
                )
            ),

            %% Wait until the LTS timestamp is updated:
            timer:sleep(5000),
            assert_db_stable(Nodes, ?DB),

            %% Check that all messages are still there.
            emqx_ds_test_helpers:verify_stream_effects(?DB, ?FUNCTION_NAME, Nodes, TopicStreams)
        end,
        []
    ).

t_rebalance_offline_restarts(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_offline_restarts1, #{apps => Apps}},
            {t_rebalance_offline_restarts2, #{apps => Apps}},
            {t_rebalance_offline_restarts3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_rebalance_offline_restarts('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_rebalance_offline_restarts(Config) ->
    %% This testcase verifies that rebalancing progresses if nodes restart or
    %% go offline and never come back.
    ok = snabbkaffe:start_trace(),

    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    _Specs = [NS1, NS2, _] = ?config(nodespecs, Config),

    %% Initialize DB on all 3 nodes.
    Opts = opts(Config, #{n_shards => 8, n_sites => 3, replication_factor => 3}),
    assert_db_open(Nodes, ?DB, Opts),

    ?retry(
        1000,
        5,
        ?assertEqual([8 || _ <- Nodes], [n_shards_online(N, ?DB) || N <- Nodes])
    ),

    %% Find out which sites are there.
    Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
    ct:pal("Sites: ~p~n", [Sites]),

    %% Shut down N3 and then remove it from the DB.
    ok = emqx_cth_cluster:stop_node(N3),
    ?assertMatch({ok, _}, ds_repl_meta(N1, leave_db_site, [?DB, S3])),
    Transitions = emqx_ds_test_helpers:transitions(N1, ?DB),
    ct:pal("Transitions: ~p~n", [Transitions]),

    %% Wait until at least one transition completes.
    ?block_until(#{?snk_kind := dsrepl_shard_transition_end}),

    %% Restart N1 and N2.
    [N1] = emqx_cth_cluster:restart(NS1),
    [N2] = emqx_cth_cluster:restart(NS2),
    ?assertEqual(
        [{ok, ok}, {ok, ok}],
        erpc:multicall([N1, N2], emqx_ds, open_db, [?DB, Opts])
    ),

    %% Target state should still be reached eventually.
    ?retry(1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N1, ?DB))),
    ?assertEqual(lists:sort([S1, S2]), ds_repl_meta(N1, db_sites, [?DB])).

t_rebalance_tolerate_lost(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_rebalance_tolerate_lost1, #{apps => Apps}},
            {t_rebalance_tolerate_lost2, #{apps => Apps}},
            {t_rebalance_tolerate_lost3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    [{nodespecs, Specs} | Config];
t_rebalance_tolerate_lost('end', _Config) ->
    ok = snabbkaffe:stop().

t_rebalance_tolerate_lost(Config) ->
    %% This testcase verifies that rebalancing can conclude if there are shards
    %% with replicas residing exclusively on nodes that left the cluster (w/o
    %% handing the data off first).

    [NS1, NS2, NS3] = ?config(nodespecs, Config),
    MsgStream = emqx_ds_test_helpers:topic_messages(?FUNCTION_NAME, <<"C1">>),

    %% Start and initialize DB on a first node.
    %% The same usually happens with current defaults.
    [N1] = emqx_cth_cluster:start([NS1]),
    Opts = opts(Config, #{n_shards => 4, n_sites => 1, replication_factor => 3}),
    assert_db_open([N1], ?DB, Opts),

    %% Start and initialize DB on rest of the nodes.
    [N2, N3] = emqx_cth_cluster:start([NS2, NS3]),
    assert_db_open([N2, N3], ?DB, Opts),

    %% Find out which sites are there.
    Nodes = [N1, N2, N3],
    Sites = [S1, S2, S3] = [ds_repl_meta(N, this_site) || N <- Nodes],
    ct:pal("Sites: ~p", [Sites]),

    ct:pal("DS Status [healthy cluster]:", []),
    ?ON(N2, emqx_ds_replication_layer_meta:print_status()),

    %% Shut down N1 and then make it leave the cluster.
    %% This will lead to a situation when DB is residing on out-of-cluster nodes only.
    ok = emqx_cth_cluster:stop_node(N1),
    _ = ?retry(200, 5, [N2, N3] = ?ON(N2, mria:cluster_nodes(running))),
    ok = ?ON(N2, ekka:force_leave(N1)),
    ok = timer:sleep(1_000),

    ct:pal("DS Status [lost node holding the data]:", []),
    ?ON(N2, emqx_ds_replication_layer_meta:print_status()),

    %% Attempt to forget S1 should fail.
    ?assertEqual(
        {error, {member_of_replica_sets, [?DB]}},
        ?ON(N2, emqx_ds_replication_layer_meta:forget_site(S1))
    ),

    %% Now turn S2 and S3 into members of DB replica set, excluding S1 in effect.
    {ok, _} = ds_repl_meta(N2, assign_db_sites, [?DB, [S2, S3]]),
    ct:pal("DS Status [rebalancing planned]:", []),
    ?ON(N2, emqx_ds_replication_layer_meta:print_status()),

    %% Target state should still be reached eventually.
    ?retry(1000, 20, ?assertEqual([], emqx_ds_test_helpers:transitions(N2, ?DB))),
    ?assertEqual(lists:sort([S2, S3]), ds_repl_meta(N2, db_sites, [?DB])),

    ct:pal("DS Status [rebalancing concluded]:", []),
    ?ON(N2, emqx_ds_replication_layer_meta:print_status()),

    %% Messages can now again be persisted successfully.
    {Msgs1, MsgStream1} = emqx_utils_stream:consume(50, MsgStream),
    {Msgs2, _MsgStream} = emqx_utils_stream:consume(50, MsgStream1),
    ?assertEqual(ok, ?ON(N2, emqx_ds:store_batch(?DB, Msgs1))),
    ?assertEqual(ok, ?ON(N3, emqx_ds:store_batch(?DB, Msgs2))),
    MsgsPersisted = ?ON(N2, emqx_ds_test_helpers:consume(?DB, ['#'])),
    ok = emqx_ds_test_helpers:diff_messages(Msgs1 ++ Msgs2, MsgsPersisted),

    %% Attempt to forget S1 should now succeed.
    ?assertEqual(ok, ?ON(N2, emqx_ds_replication_layer_meta:forget_site(S1))),

    ok = emqx_cth_cluster:stop(Nodes).

t_drop_generation(Config) ->
    Apps = [appspec(emqx_durable_storage), emqx_ds_builtin_raft],
    [_, _, NS3] =
        NodeSpecs = emqx_cth_cluster:mk_nodespecs(
            [
                {t_drop_generation1, #{apps => Apps}},
                {t_drop_generation2, #{apps => Apps}},
                {t_drop_generation3, #{apps => Apps}}
            ],
            #{
                work_dir => ?config(work_dir, Config)
            }
        ),

    Nodes = [N1, _, N3] = emqx_cth_cluster:start(NodeSpecs),
    ?check_trace(
        try
            %% Initialize DB on all 3 nodes.
            Opts = opts(Config, #{n_shards => 1, n_sites => 3, replication_factor => 3}),
            ?assertEqual(
                [{ok, ok} || _ <- Nodes],
                erpc:multicall(Nodes, emqx_ds, open_db, [?DB, Opts])
            ),
            timer:sleep(1000),
            %% Create a generation while all nodes are online:
            ?ON(N1, ?assertMatch(ok, emqx_ds:add_generation(?DB))),
            ?ON(
                Nodes,
                ?assertEqual(
                    [{<<"0">>, 1}, {<<"0">>, 2}],
                    maps:keys(emqx_ds:list_generations_with_lifetimes(?DB))
                )
            ),
            %% Drop generation while all nodes are online:
            ?ON(N1, ?assertMatch(ok, emqx_ds:drop_generation(?DB, {<<"0">>, 1}))),
            ?ON(
                Nodes,
                ?assertEqual(
                    [{<<"0">>, 2}],
                    maps:keys(emqx_ds:list_generations_with_lifetimes(?DB))
                )
            ),
            %% Ston N3, then create and drop generation when it's offline:
            ok = emqx_cth_cluster:stop_node(N3),
            ?ON(
                N1,
                begin
                    ok = emqx_ds:add_generation(?DB),
                    ok = emqx_ds:drop_generation(?DB, {<<"0">>, 2})
                end
            ),
            %% Restart N3 and verify that it reached the consistent state:
            emqx_cth_cluster:restart(NS3),
            ok = ?ON(N3, emqx_ds:open_db(?DB, Opts)),
            %% N3 can be in unstalbe state right now, but it still
            %% must successfully return streams:
            ?ON(
                Nodes,
                ?assertEqual([], emqx_ds:get_streams(?DB, ['#'], 0))
            ),
            timer:sleep(1000),
            ?ON(
                Nodes,
                ?assertEqual(
                    [{<<"0">>, 3}],
                    maps:keys(emqx_ds:list_generations_with_lifetimes(?DB))
                )
            )
        after
            emqx_cth_cluster:stop(Nodes)
        end,
        fun(_Trace) ->
            %% TODO: some idempotency errors still happen
            %% ?assertMatch([], ?of_kind(ds_storage_layer_failed_to_drop_generation, Trace)),
            true
        end
    ).

t_error_mapping_replication_layer(init, Config) ->
    Apps = emqx_cth_suite:start([emqx_ds_builtin_raft], #{
        work_dir => ?config(work_dir, Config)
    }),
    ok = snabbkaffe:start_trace(),
    ok = emqx_ds_test_helpers:mock_rpc(),
    [{apps, Apps} | Config];
t_error_mapping_replication_layer('end', Config) ->
    emqx_ds_test_helpers:unmock_rpc(),
    snabbkaffe:stop(),
    emqx_cth_suite:stop(?config(apps, Config)),
    Config.

t_error_mapping_replication_layer(Config) ->
    %% This checks that the replication layer maps recoverable errors correctly.

    DB = ?FUNCTION_NAME,
    ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config, #{n_shards => 2}))),
    [Shard1, Shard2] = emqx_ds_replication_layer_meta:shards(DB),

    TopicFilter = emqx_topic:words(<<"foo/#">>),
    Msgs = [
        message(<<"C1">>, <<"foo/bar">>, <<"1">>, 0),
        message(<<"C1">>, <<"foo/baz">>, <<"2">>, 1),
        message(<<"C2">>, <<"foo/foo">>, <<"3">>, 2),
        message(<<"C3">>, <<"foo/xyz">>, <<"4">>, 3),
        message(<<"C4">>, <<"foo/bar">>, <<"5">>, 4),
        message(<<"C5">>, <<"foo/oof">>, <<"6">>, 5)
    ],

    ?assertMatch(ok, emqx_ds:store_batch(DB, Msgs)),

    ?block_until(#{?snk_kind := emqx_ds_buffer_flush, shard := Shard1}),
    ?block_until(#{?snk_kind := emqx_ds_buffer_flush, shard := Shard2}),

    Streams0 = emqx_ds:get_streams(DB, TopicFilter, 0),
    Iterators0 = lists:map(
        fun({_Rank, S}) ->
            {ok, Iter} = emqx_ds:make_iterator(DB, S, TopicFilter, 0),
            Iter
        end,
        Streams0
    ),

    %% Disrupt the link to the second shard.
    ok = emqx_ds_test_helpers:mock_rpc_result(
        fun(_Node, emqx_ds_replication_layer, _Function, Args) ->
            case Args of
                [DB, Shard1 | _] -> passthrough;
                [DB, Shard2 | _] -> unavailable
            end
        end
    ),

    %% Result of `emqx_ds:get_streams/3` will just contain partial results, not an error.
    Streams1 = emqx_ds:get_streams(DB, TopicFilter, 0),
    ?assert(
        length(Streams1) > 0 andalso length(Streams1) =< length(Streams0),
        Streams1
    ),

    %% At least one of `emqx_ds:make_iterator/4` will end in an error.
    Results1 = lists:map(
        fun({_Rank, S}) ->
            case emqx_ds:make_iterator(DB, S, TopicFilter, 0) of
                Ok = {ok, _Iter} ->
                    Ok;
                Error = {error, recoverable, {erpc, _}} ->
                    Error;
                Other ->
                    ct:fail({unexpected_result, Other})
            end
        end,
        Streams0
    ),
    ?assert(
        length([error || {error, _, _} <- Results1]) > 0,
        Results1
    ),

    %% At least one of `emqx_ds:next/3` over initial set of iterators will end in an error.
    Results2 = lists:map(
        fun(Iter) ->
            case emqx_ds:next(DB, Iter, _BatchSize = 42) of
                Ok = {ok, _Iter, _} ->
                    Ok;
                Error = {error, recoverable, {badrpc, _}} ->
                    Error;
                Other ->
                    ct:fail({unexpected_result, Other})
            end
        end,
        Iterators0
    ),
    ?assert(
        length([error || {error, _, _} <- Results2]) > 0,
        Results2
    ),

    %% Calling `emqx_ds:poll/3` succeeds, but some poll requests should fail anyway.
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := ds_repl_poll_shard_failed}),
        length(Streams0),
        500
    ),
    UserData = ?FUNCTION_NAME,
    {ok, _PollRef} = emqx_ds:poll(DB, [{UserData, I} || I <- Iterators0], #{timeout => 1_000}),
    ?assertMatch(
        {timeout, Events} when length(Events) > 0,
        snabbkaffe:receive_events(SRef)
    ).

%% This testcase verifies the behavior of `store_batch' operation
%% when the underlying code experiences recoverable or unrecoverable
%% problems.
t_store_batch_fail(init, Config) ->
    Apps = emqx_cth_suite:start([emqx_ds_builtin_raft], #{
        work_dir => ?config(work_dir, Config)
    }),
    [{apps, Apps} | Config];
t_store_batch_fail('end', Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    Config.

t_store_batch_fail(Config) ->
    DB = ?FUNCTION_NAME,
    ?check_trace(
        #{timetrap => 15_000},
        try
            ok = meck:new(emqx_ds_storage_layer, [passthrough, no_history]),
            ?assertMatch(ok, emqx_ds:open_db(DB, opts(Config, #{n_shards => 2}))),
            %% Success:
            Batch1 = [
                message(<<"C1">>, <<"foo/bar">>, <<"1">>, 1),
                message(<<"C1">>, <<"foo/bar">>, <<"2">>, 1)
            ],
            ?assertMatch(ok, emqx_ds:store_batch(DB, Batch1, #{sync => true})),
            %% Inject unrecoverable error:
            ok = meck:expect(emqx_ds_storage_layer, store_batch, fun(_DB, _Shard, _Messages, _) ->
                {error, unrecoverable, mock}
            end),
            Batch2 = [
                message(<<"C1">>, <<"foo/bar">>, <<"3">>, 1),
                message(<<"C1">>, <<"foo/bar">>, <<"4">>, 1)
            ],
            ?assertMatch(
                {error, unrecoverable, mock}, emqx_ds:store_batch(DB, Batch2, #{sync => true})
            ),
            ok = meck:unload(emqx_ds_storage_layer),
            %% Inject a recoveralbe error:
            ok = meck:new(ra, [passthrough, no_history]),
            ok = meck:expect(ra, process_command, fun(Servers, Shard, Command) ->
                ?tp(ra_command, #{servers => Servers, shard => Shard, command => Command}),
                {timeout, mock}
            end),
            Batch3 = [
                message(<<"C1">>, <<"foo/bar">>, <<"5">>, 2),
                message(<<"C2">>, <<"foo/bar">>, <<"6">>, 2),
                message(<<"C1">>, <<"foo/bar">>, <<"7">>, 3),
                message(<<"C2">>, <<"foo/bar">>, <<"8">>, 3)
            ],
            %% Note: due to idempotency issues the number of retries
            %% is currently set to 0:
            ?assertMatch(
                {error, recoverable, {timeout, mock}},
                emqx_ds:store_batch(DB, Batch3, #{sync => true})
            ),
            ok = meck:unload(ra),
            ?assertMatch(ok, emqx_ds:store_batch(DB, Batch3, #{sync => true})),
            lists:sort(emqx_ds_test_helpers:consume_per_stream(DB, ['#'], 0))
        after
            meck:unload()
        end,
        [
            {"message ordering", fun(StoredMessages, _Trace) ->
                [{_, Stream1}, {_, Stream2}] = StoredMessages,
                ?assertMatch(
                    [
                        #message{payload = <<"1">>},
                        #message{payload = <<"2">>},
                        #message{payload = <<"5">>},
                        #message{payload = <<"7">>}
                    ],
                    Stream1
                ),
                ?assertMatch(
                    [
                        #message{payload = <<"6">>},
                        #message{payload = <<"8">>}
                    ],
                    Stream2
                )
            end}
        ]
    ).

t_crash_restart_recover(init, Config) ->
    Apps = [appspec(ra), appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Specs = emqx_cth_cluster:mk_nodespecs(
        [
            {t_crash_stop_recover1, #{apps => Apps}},
            {t_crash_stop_recover2, #{apps => Apps}},
            {t_crash_stop_recover3, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    Nodes = emqx_cth_cluster:start(Specs),
    [{nodes, Nodes}, {nodespecs, Specs} | Config];
t_crash_restart_recover('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_crash_restart_recover(Config) ->
    %% This testcase verifies that in the event of abrupt site failure message data is
    %% correctly preserved.
    Nodes = [N1, N2, N3] = ?config(nodes, Config),
    _Specs = [_, NS2, NS3] = ?config(nodespecs, Config),
    DBOpts = opts(Config, #{n_shards => 16, n_sites => 3, replication_factor => 3}),

    %% Prepare test event stream.
    NMsgs = 400,
    NClients = 8,
    {Stream0, TopicStreams} =
        emqx_ds_test_helpers:interleaved_topic_messages(?FUNCTION_NAME, NClients, NMsgs),
    Stream1 = emqx_utils_stream:interleave(
        [
            {300, Stream0},
            emqx_utils_stream:const(add_generation)
        ],
        false
    ),
    Stream = emqx_utils_stream:interleave(
        [
            {1000, Stream1},
            emqx_utils_stream:list([
                fun() -> kill_restart_node_async(N2, NS2, DBOpts) end,
                fun() -> kill_restart_node_async(N3, NS3, DBOpts) end
            ])
        ],
        true
    ),

    ?check_trace(
        #{timetrap => 60_000},
        begin
            %% Initialize DB on all nodes.
            assert_db_open(Nodes, ?DB, DBOpts),

            %% Apply the test events, including simulated node crashes.
            NodeStream = emqx_utils_stream:const(N1),
            StartedAt = erlang:monotonic_time(millisecond),
            emqx_ds_test_helpers:apply_stream(?DB, NodeStream, Stream, 0),

            %% It's expected to lose few messages when leaders are abruptly killed.
            MatchFlushFailed = ?match_event(#{?snk_kind := emqx_ds_buffer_flush_failed}),
            {ok, SubRef} = snabbkaffe:subscribe(MatchFlushFailed, NMsgs, _Timeout = 5000, infinity),
            {timeout, Events} = snabbkaffe:receive_events(SubRef),
            LostMessages = [
                emqx_ds_test_helpers:message_canonical_form(M)
             || #{batch := Messages} <- Events, M <- Messages
            ],
            ct:pal("Some messages were lost: ~p", [LostMessages]),
            ?assert(length(LostMessages) < NMsgs div 20),

            %% Wait until crashed nodes are ready.
            SinceStarted = erlang:monotonic_time(millisecond) - StartedAt,
            wait_db_bootstrapped([N2, N3], ?DB, infinity, SinceStarted),

            %% Verify that all the successfully persisted messages are there.
            VerifyClient = fun({ClientId, ExpectedStream}) ->
                Topic = emqx_ds_test_helpers:client_topic(?FUNCTION_NAME, ClientId),
                ClientNodes = nodes_of_clientid(ClientId, Nodes),
                DSStream1 = ds_topic_stream(ClientId, Topic, hd(ClientNodes)),
                %% Do nodes contain same messages for a client?
                lists:foreach(
                    fun(ClientNode) ->
                        DSStream = ds_topic_stream(ClientId, Topic, ClientNode),
                        ?defer_assert(emqx_ds_test_helpers:diff_messages(DSStream1, DSStream))
                    end,
                    tl(ClientNodes)
                ),
                %% Does any messages were lost unexpectedly?
                {_, DSMessages} = lists:unzip(emqx_utils_stream:consume(DSStream1)),
                ExpectedMessages = emqx_utils_stream:consume(ExpectedStream),
                MissingMessages = emqx_ds_test_helpers:message_set_subtract(
                    ExpectedMessages, DSMessages
                ),
                ?defer_assert(
                    ?assertEqual(
                        [],
                        emqx_ds_test_helpers:sublist(MissingMessages -- LostMessages),
                        emqx_ds_test_helpers:sublist(DSMessages)
                    )
                )
            end,
            lists:foreach(VerifyClient, TopicStreams)
        end,
        []
    ).

t_poll(init, Config) ->
    Apps = [appspec(emqx_durable_storage), appspec(emqx_ds_builtin_raft)],
    Nodes = emqx_cth_cluster:start(
        [
            {t_poll1, #{apps => Apps}},
            {t_poll2, #{apps => Apps}}
        ],
        #{work_dir => ?config(work_dir, Config)}
    ),
    [{nodes, Nodes} | Config];
t_poll('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(nodes, Config)).

t_poll(Config) ->
    Nodes = [N1 | _] = ?config(nodes, Config),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            %% Initialize DB on all nodes and wait for it to come online.
            Opts = opts(Config, #{n_shards => 1}),
            assert_db_open(Nodes, ?DB, Opts),

            %% Insert data:
            Batch1 = [
                message(<<"C1">>, <<"foo/bar">>, <<"1">>, 1),
                message(<<"C1">>, <<"foo/bar">>, <<"2">>, 1)
            ],
            ?ON(N1, ok = emqx_ds:store_batch(?DB, Batch1, #{sync => true})),
            %% Create initial iterators:
            Its0 = ?ON(
                N1,
                begin
                    TF = [<<"foo">>, <<"bar">>],
                    [
                        begin
                            {ok, It} = emqx_ds:make_iterator(?DB, Stream, TF, 0),
                            {make_ref(), It}
                        end
                     || {_Rank, Stream} <- emqx_ds:get_streams(?DB, TF, 0)
                    ]
                end
            ),
            [{Token1, _}] = Its0,
            %% Check poll funtionality:
            CheckF =
                fun() ->
                    {ok, Alias} = emqx_ds:poll(?DB, Its0, #{timeout => 1000}),
                    [{Token1, {ok, NextIt, Batch}}] = emqx_ds_test_helpers:collect_poll_replies(
                        Alias, 1000
                    ),
                    ?assertMatch(
                        [{_, #message{payload = <<"1">>}}, {_, #message{payload = <<"2">>}}],
                        Batch
                    ),
                    ?assertMatch(
                        {ok, _It, []},
                        emqx_ds:next(?DB, NextIt, 100),
                        "The returned iterator is correct"
                    )
                end,
            [?defer_assert(emqx_ds_test_helpers:on(N, CheckF)) || N <- Nodes]
        end,
        []
    ).

nodes_of_clientid(ClientId, Nodes) ->
    emqx_ds_test_helpers:nodes_of_clientid(?DB, ClientId, Nodes).

ds_topic_stream(ClientId, ClientTopic, Node) ->
    emqx_ds_test_helpers:ds_topic_stream(?DB, ClientId, ClientTopic, Node).

is_message_lost(Message, MessagesLost) ->
    lists:any(
        fun(ML) ->
            emqx_ds_test_helpers:message_eq([clientid, topic, payload], Message, ML)
        end,
        MessagesLost
    ).

kill_restart_node_async(Node, Spec, DBOpts) ->
    erlang:spawn_link(?MODULE, kill_restart_node, [Node, Spec, DBOpts]).

kill_restart_node(Node, Spec, DBOpts) ->
    ok = emqx_cth_peer:kill(Node),
    ?tp(test_cluster_node_killed, #{node => Node}),
    _ = emqx_cth_cluster:restart(Spec),
    ok = erpc:call(Node, emqx_ds, open_db, [?DB, DBOpts]).

%%

assert_db_open(Nodes, DB, Opts) ->
    ?assertEqual(
        [{ok, ok} || _ <- Nodes],
        erpc:multicall(Nodes, emqx_ds, open_db, [DB, Opts])
    ),
    wait_db_bootstrapped(Nodes, ?DB).

assert_db_stable([Node | _], DB) ->
    Shards = ds_repl_meta(Node, shards, [DB]),
    ?assertMatch(
        _Leadership = [_ | _],
        db_leadership(Node, DB, Shards)
    ).

wait_db_bootstrapped(Nodes, DB) ->
    wait_db_bootstrapped(Nodes, DB, infinity, infinity).

wait_db_bootstrapped(Nodes, DB, Timeout, BackInTime) ->
    SRefs = [
        snabbkaffe:subscribe(
            ?match_event(#{
                ?snk_kind := emqx_ds_replshard_bootstrapped,
                ?snk_meta := #{node := Node},
                db := DB,
                shard := Shard
            }),
            1,
            Timeout,
            BackInTime
        )
     || Node <- Nodes,
        Shard <- ds_repl_meta(Node, my_shards, [DB])
    ],
    lists:foreach(
        fun({ok, SRef}) ->
            ?assertMatch({ok, [_]}, snabbkaffe:receive_events(SRef))
        end,
        SRefs
    ).

%%

db_leadership(Node, DB, Shards) ->
    Leadership = [{S, shard_leadership(Node, DB, S)} || S <- Shards],
    Inconsistent = [SL || SL = {_, Leaders} <- Leadership, map_size(Leaders) > 1],
    case Inconsistent of
        [] ->
            Leadership;
        [_ | _] ->
            {error, inconsistent, Inconsistent}
    end.

shard_leadership(Node, DB, Shard) ->
    ReplicaSet = ds_repl_meta(Node, replica_set, [DB, Shard]),
    Nodes = [ds_repl_meta(Node, node, [Site]) || Site <- ReplicaSet],
    lists:foldl(
        fun({Site, SN}, Acc) -> Acc#{shard_leader(SN, DB, Shard, Site) => SN} end,
        #{},
        lists:zip(ReplicaSet, Nodes)
    ).

shard_leader(Node, DB, Shard, Site) ->
    shard_server_info(Node, DB, Shard, Site, leader).

shard_server_info(Node, DB, Shard, Site, Info) ->
    ?ON(
        Node,
        begin
            Server = emqx_ds_replication_layer_shard:shard_server(DB, Shard, Site),
            emqx_ds_replication_layer_shard:server_info(Info, Server)
        end
    ).

ds_repl_meta(Node, Fun) ->
    ds_repl_meta(Node, Fun, []).

ds_repl_meta(Node, Fun, Args) ->
    try
        erpc:call(Node, emqx_ds_replication_layer_meta, Fun, Args)
    catch
        EC:Err:Stack ->
            ct:pal("emqx_ds_replication_layer_meta:~p(~p) @~p failed:~n~p:~p~nStack: ~p", [
                Fun, Args, Node, EC, Err, Stack
            ]),
            error(meta_op_failed)
    end.

shards_online(Node, DB) ->
    erpc:call(Node, emqx_ds_builtin_raft_db_sup, which_shards, [DB]).

n_shards_online(Node, DB) ->
    length(shards_online(Node, DB)).

add_generation(Node, DB) ->
    ok = erpc:call(Node, emqx_ds, add_generation, [DB]),
    [].

message(ClientId, Topic, Payload, PublishedAt) ->
    #message{
        from = ClientId,
        topic = Topic,
        payload = Payload,
        timestamp = PublishedAt,
        id = emqx_guid:gen()
    }.

compare_message(M1, M2) ->
    {M1#message.from, M1#message.timestamp} < {M2#message.from, M2#message.timestamp}.

consume(Node, DB, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, consume, [DB, TopicFilter, StartTime]).

consume_shard(Node, DB, Shard, TopicFilter, StartTime) ->
    erpc:call(Node, emqx_ds_test_helpers, storage_consume, [{DB, Shard}, TopicFilter, StartTime]).

probably(P, Fun) ->
    case rand:uniform() of
        X when X < P -> Fun();
        _ -> []
    end.

sample(N, List) ->
    L = length(List),
    case L =< N of
        true ->
            L;
        false ->
            H = N div 2,
            Filler = integer_to_list(L - N) ++ " more",
            lists:sublist(List, H) ++ [Filler] ++ lists:sublist(List, L - H, L)
    end.

%%

suite() -> [{timetrap, {seconds, 120}}].

all() ->
    [{group, Grp} || {Grp, _} <- groups()].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {bitfield_lts, TCs -- [t_poll]},
        {skipstream_lts, TCs}
    ].

init_per_group(Group, Config) ->
    LayoutConf =
        case Group of
            skipstream_lts ->
                {emqx_ds_storage_skipstream_lts, #{with_guid => true}};
            bitfield_lts ->
                {emqx_ds_storage_bitfield_lts, #{}}
        end,
    [{layout, LayoutConf} | Config].

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(TCName, Config0) ->
    Config1 = [{work_dir, emqx_cth_suite:work_dir(TCName, Config0)} | Config0],
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config1).

end_per_testcase(TCName, Config) ->
    ok = snabbkaffe:stop(),
    Result = emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config),
    catch emqx_ds:drop_db(TCName),
    emqx_cth_suite:clean_work_dir(?config(work_dir, Config)),
    Result.
