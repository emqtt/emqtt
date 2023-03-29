%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_fs_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_ft/include/emqx_ft_storage_fs.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_ft),
    ok = emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([]),
    ok.

init_per_testcase(TC, Config) ->
    ok = emqx_common_test_helpers:start_app(
        emqx_ft,
        fun(emqx_ft) ->
            emqx_ft_test_helpers:load_config(#{
                storage => #{
                    type => local,
                    segments => #{
                        root => emqx_ft_test_helpers:root(Config, node(), [TC, segments])
                    },
                    exporter => #{
                        type => local,
                        root => emqx_ft_test_helpers:root(Config, node(), [TC, exports])
                    }
                }
            })
        end
    ),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TC, _Config) ->
    ok = snabbkaffe:stop(),
    ok = application:stop(emqx_ft),
    ok.

%%

-define(NSEGS(Filesize, SegmentSize), (ceil(Filesize / SegmentSize) + 1)).

t_gc_triggers_periodically(_Config) ->
    Interval = 500,
    ok = set_gc_config(interval, Interval),
    ok = emqx_ft_storage_fs_gc:reset(emqx_ft_conf:storage()),
    ?check_trace(
        timer:sleep(Interval * 3),
        fun(Trace) ->
            [Event, _ | _] = ?of_kind(garbage_collection, Trace),
            ?assertMatch(
                #{
                    stats := #gcstats{
                        files = 0,
                        directories = 0,
                        space = 0,
                        errors = #{} = Errors
                    }
                } when map_size(Errors) == 0,
                Event
            )
        end
    ).

t_gc_triggers_manually(_Config) ->
    ?check_trace(
        ?assertMatch(
            #gcstats{files = 0, directories = 0, space = 0, errors = #{} = Errors} when
                map_size(Errors) == 0,
            emqx_ft_storage_fs_gc:collect(emqx_ft_conf:storage())
        ),
        fun(Trace) ->
            [Event] = ?of_kind(garbage_collection, Trace),
            ?assertMatch(
                #{stats := #gcstats{}},
                Event
            )
        end
    ).

t_gc_complete_transfers(_Config) ->
    Storage = emqx_ft_conf:storage(),
    ok = set_gc_config(minimum_segments_ttl, 0),
    ok = set_gc_config(maximum_segments_ttl, 3),
    ok = set_gc_config(interval, 500),
    ok = emqx_ft_storage_fs_gc:reset(Storage),
    Transfers = [
        {
            T1 = {<<"client1">>, mk_file_id()},
            #{name => "cat.cur", segments_ttl => 10},
            emqx_ft_content_gen:new({?LINE, S1 = 42}, SS1 = 16)
        },
        {
            T2 = {<<"client2">>, mk_file_id()},
            #{name => "cat.ico", segments_ttl => 10},
            emqx_ft_content_gen:new({?LINE, S2 = 420}, SS2 = 64)
        },
        {
            T3 = {<<"client42">>, mk_file_id()},
            #{name => "cat.jpg", segments_ttl => 10},
            emqx_ft_content_gen:new({?LINE, S3 = 42000}, SS3 = 1024)
        }
    ],
    % 1. Start all transfers
    TransferSizes = emqx_misc:pmap(
        fun(Transfer) -> start_transfer(Storage, Transfer) end,
        Transfers
    ),
    ?assertEqual([S1, S2, S3], TransferSizes),
    ?assertMatch(
        #gcstats{files = 0, directories = 0, errors = #{} = Es} when map_size(Es) == 0,
        emqx_ft_storage_fs_gc:collect(Storage)
    ),
    % 2. Complete just the first transfer
    {ok, {ok, Event}} = ?wait_async_action(
        ?assertEqual(ok, complete_transfer(Storage, T1, S1)),
        #{?snk_kind := garbage_collection},
        1000
    ),
    ?assertMatch(
        #{
            stats := #gcstats{
                files = Files,
                directories = 2,
                space = Space,
                errors = #{} = Es
            }
        } when Files == ?NSEGS(S1, SS1) andalso Space > S1 andalso map_size(Es) == 0,
        Event
    ),
    % 3. Complete rest of transfers
    {ok, Sub} = snabbkaffe_collector:subscribe(
        ?match_event(#{?snk_kind := garbage_collection}),
        2,
        1000,
        0
    ),
    ?assertEqual(
        [ok, ok],
        emqx_misc:pmap(
            fun({Transfer, Size}) -> complete_transfer(Storage, Transfer, Size) end,
            [{T2, S2}, {T3, S3}]
        )
    ),
    {ok, Events} = snabbkaffe_collector:receive_events(Sub),
    CFiles = lists:sum([Stats#gcstats.files || #{stats := Stats} <- Events]),
    CDirectories = lists:sum([Stats#gcstats.directories || #{stats := Stats} <- Events]),
    CSpace = lists:sum([Stats#gcstats.space || #{stats := Stats} <- Events]),
    CErrors = lists:foldl(
        fun maps:merge/2,
        #{},
        [Stats#gcstats.errors || #{stats := Stats} <- Events]
    ),
    ?assertEqual(?NSEGS(S2, SS2) + ?NSEGS(S3, SS3), CFiles),
    ?assertEqual(2 + 2, CDirectories),
    ?assertMatch(Space when Space > S2 + S3, CSpace),
    ?assertMatch(Errors when map_size(Errors) == 0, CErrors),
    % 4. Ensure that empty transfer directories will be eventually collected
    {ok, _} = ?block_until(
        #{
            ?snk_kind := garbage_collection,
            stats := #gcstats{
                files = 0,
                directories = 6,
                space = 0
            }
        },
        5000,
        0
    ).

t_gc_incomplete_transfers(_Config) ->
    ok = set_gc_config(minimum_segments_ttl, 0),
    ok = set_gc_config(maximum_segments_ttl, 4),
    Storage = emqx_ft_conf:storage(),
    Transfers = [
        {
            {<<"client43"/utf8>>, <<"file-🦕"/utf8>>},
            #{name => "dog.cur", segments_ttl => 1},
            emqx_ft_content_gen:new({?LINE, S1 = 123}, SS1 = 32)
        },
        {
            {<<"client44">>, <<"file-🦖"/utf8>>},
            #{name => "dog.ico", segments_ttl => 2},
            emqx_ft_content_gen:new({?LINE, S2 = 456}, SS2 = 64)
        },
        {
            {<<"client1337">>, <<"file-🦀"/utf8>>},
            #{name => "dog.jpg", segments_ttl => 3000},
            emqx_ft_content_gen:new({?LINE, S3 = 7890}, SS3 = 128)
        },
        {
            {<<"client31337">>, <<"file-⏳"/utf8>>},
            #{name => "dog.jpg"},
            emqx_ft_content_gen:new({?LINE, S4 = 1230}, SS4 = 256)
        }
    ],
    % 1. Start transfers, send all the segments but don't trigger completion.
    _ = emqx_misc:pmap(fun(Transfer) -> start_transfer(Storage, Transfer) end, Transfers),
    % 2. Enable periodic GC every 0.5 seconds.
    ok = set_gc_config(interval, 500),
    ok = emqx_ft_storage_fs_gc:reset(Storage),
    % 3. First we need the first transfer to be collected.
    {ok, _} = ?block_until(
        #{
            ?snk_kind := garbage_collection,
            stats := #gcstats{
                files = Files,
                directories = 4,
                space = Space
            }
        } when Files == (?NSEGS(S1, SS1)) andalso Space > S1,
        5000,
        0
    ),
    % 4. Then the second one.
    {ok, _} = ?block_until(
        #{
            ?snk_kind := garbage_collection,
            stats := #gcstats{
                files = Files,
                directories = 4,
                space = Space
            }
        } when Files == (?NSEGS(S2, SS2)) andalso Space > S2,
        5000,
        0
    ),
    % 5. Then transfers 3 and 4 because 3rd has too big TTL and 4th has no specific TTL.
    {ok, _} = ?block_until(
        #{
            ?snk_kind := garbage_collection,
            stats := #gcstats{
                files = Files,
                directories = 4 * 2,
                space = Space
            }
        } when Files == (?NSEGS(S3, SS3) + ?NSEGS(S4, SS4)) andalso Space > S3 + S4,
        5000,
        0
    ).

t_gc_handling_errors(_Config) ->
    ok = set_gc_config(minimum_segments_ttl, 0),
    ok = set_gc_config(maximum_segments_ttl, 0),
    Storage = emqx_ft_conf:storage(),
    Transfer1 = {<<"client1">>, mk_file_id()},
    Transfer2 = {<<"client2">>, mk_file_id()},
    Filemeta = #{name => "oops.pdf"},
    Size = 420,
    SegSize = 16,
    _ = start_transfer(
        Storage,
        {Transfer1, Filemeta, emqx_ft_content_gen:new({?LINE, Size}, SegSize)}
    ),
    _ = start_transfer(
        Storage,
        {Transfer2, Filemeta, emqx_ft_content_gen:new({?LINE, Size}, SegSize)}
    ),
    % 1. Throw some chaos in the transfer directory.
    DirFragment1 = emqx_ft_storage_fs:get_subdir(Storage, Transfer1, fragment),
    DirTemporary1 = emqx_ft_storage_fs:get_subdir(Storage, Transfer1, temporary),
    PathShadyLink = filename:join(DirTemporary1, "linked-here"),
    ok = file:make_symlink(DirFragment1, PathShadyLink),
    DirTransfer2 = emqx_ft_storage_fs:get_subdir(Storage, Transfer2),
    PathTripUp = filename:join(DirTransfer2, "trip-up-here"),
    ok = file:write_file(PathTripUp, <<"HAHA">>),
    ok = timer:sleep(timer:seconds(1)),
    % 2. Observe the errors are reported consistently.
    ?check_trace(
        ?assertMatch(
            #gcstats{
                files = Files,
                directories = 3,
                space = Space,
                errors = #{
                    % NOTE: dangling symlink looks like `enoent` for some reason
                    {file, PathShadyLink} := {unexpected, _},
                    {directory, DirTransfer2} := eexist
                }
            } when Files == ?NSEGS(Size, SegSize) * 2 andalso Space > Size * 2,
            emqx_ft_storage_fs_gc:collect(Storage)
        ),
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        errors := #{
                            {file, PathShadyLink} := {unexpected, _},
                            {directory, DirTransfer2} := eexist
                        }
                    }
                ],
                ?of_kind("garbage_collection_errors", Trace)
            )
        end
    ).

%%

set_gc_config(Name, Value) ->
    emqx_config:put([file_transfer, storage, segments, gc, Name], Value).

start_transfer(Storage, {Transfer, Meta, Gen}) ->
    ?assertEqual(
        ok,
        emqx_ft_storage_fs:store_filemeta(Storage, Transfer, Meta)
    ),
    emqx_ft_content_gen:fold(
        fun({Content, SegmentNum, #{chunk_size := SegmentSize}}, _Transferred) ->
            Offset = (SegmentNum - 1) * SegmentSize,
            ?assertEqual(
                ok,
                emqx_ft_storage_fs:store_segment(Storage, Transfer, {Offset, Content})
            ),
            Offset + byte_size(Content)
        end,
        0,
        Gen
    ).

complete_transfer(Storage, Transfer, Size) ->
    complete_transfer(Storage, Transfer, Size, 100).

complete_transfer(Storage, Transfer, Size, Timeout) ->
    {async, Pid} = emqx_ft_storage_fs:assemble(Storage, Transfer, Size),
    MRef = erlang:monitor(process, Pid),
    Pid ! kickoff,
    receive
        {'DOWN', MRef, process, Pid, {shutdown, Result}} ->
            Result
    after Timeout ->
        ct:fail("Assembler did not finish in time")
    end.

mk_file_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).
