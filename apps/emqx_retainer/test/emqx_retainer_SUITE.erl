%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this ifle except in compliance with the License.
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

-module(emqx_retainer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_retainer.hrl").

-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    [
        {group, mnesia_without_indices},
        {group, mnesia_with_indices},
        {group, mnesia_reindex},
        {group, index_agnostic},
        {group, disabled}
    ].

groups() ->
    [
        {mnesia_without_indices, [sequence], index_related_tests()},
        {mnesia_with_indices, [sequence], index_related_tests()},
        {mnesia_reindex, [sequence], [t_reindex]},
        {index_agnostic, [sequence], [t_disable_then_start, t_start_stop_on_setting_change]},
        {disabled, [t_disabled]}
    ].

index_related_tests() ->
    emqx_common_test_helpers:all(?MODULE) --
        [t_reindex, t_disable_then_start, t_disabled, t_start_stop_on_setting_change].

%% erlfmt-ignore
-define(BASE_CONF, <<"
retainer {
  enable = true
  msg_clear_interval = 0s
  msg_expiry_interval = 0s
  max_payload_size = 1MB
  delivery_rate = \"1000/s\"
  max_publish_rate = \"100000/s\"
  flow_control {
    batch_read_number = 0
    batch_deliver_number = 0
  }
  backend {
    type = built_in_database
    storage_type = ram
    max_retained_messages = 0
  }
}
">>).

%% erlfmt-ignore
-define(DISABLED_CONF, <<"
mqtt {
  retain_available = false
}
">>).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_group(mnesia_without_indices = Group, Config) ->
    start_apps(Group, [{index, false} | Config]);
init_per_group(mnesia_reindex = Group, Config) ->
    start_apps(Group, Config);
init_per_group(Group, Config) ->
    start_apps(Group, Config).

end_per_group(_Group, Config) ->
    emqx_retainer_mnesia:populate_index_meta(),
    stop_apps(Config),
    Config.

init_per_testcase(t_disabled, Config) ->
    snabbkaffe:start_trace(),
    Config;
init_per_testcase(_TestCase, Config) ->
    snabbkaffe:start_trace(),
    case ?config(index, Config) of
        false ->
            mnesia:clear_table(?TAB_INDEX_META);
        _ ->
            emqx_retainer_mnesia:populate_index_meta()
    end,
    emqx_retainer:clean(),
    reset_rates_to_default(),
    Config.

end_per_testcase(t_disabled, _Config) ->
    snabbkaffe:stop(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    snabbkaffe:stop(),
    reset_rates_to_default(),
    ok.

emqx_retainer_app_spec() ->
    {emqx_retainer, ?BASE_CONF}.

emqx_conf_app_spec(disabled) ->
    {emqx_conf, ?DISABLED_CONF};
emqx_conf_app_spec(_) ->
    emqx_conf.

start_apps(Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf_app_spec(Group),
            emqx_retainer_app_spec()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

stop_apps(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_store_and_clean(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    emqtt:publish(
        C1,
        <<"retained">>,
        <<"this is a retained message">>,
        [{qos, 0}, {retain, true}]
    ),
    timer:sleep(100),

    {ok, _, List} = emqx_retainer:page_read(<<"retained">>, 1, 10),
    ?assertEqual(1, length(List)),
    ?assertMatch(
        {ok, [#message{payload = <<"this is a retained message">>}]},
        emqx_retainer:read_message(<<"retained">>)
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),
    ?assertMatch(
        {ok, [#message{payload = <<"this is a retained message">>}]},
        emqx_retainer:read_message(<<"retained">>)
    ),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    timer:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    ?assertMatch(
        {ok, []},
        emqx_retainer:read_message(<<"retained">>)
    ),

    ok = emqx_retainer:clean(),
    {ok, _, List2} = emqx_retainer:page_read(<<"retained">>, 1, 10),
    ?assertEqual(0, length(List2)),
    ?assertMatch(
        {ok, []},
        emqx_retainer:read_message(<<"retained">>)
    ),

    ok = emqtt:disconnect(C1).

t_retain_handling(Config) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    ok = emqx_retainer:clean(),

    %% rh = 0, no wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    %% rh = 0, has wildcard, and with empty retained message
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(1))),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/#">>),

    publish(
        C1,
        <<"retained">>,
        <<"this is a retained message">>,
        [{qos, 0}, {retain, true}],
        Config
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(1, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 1}]),
    ?assertEqual(0, length(receive_messages(1))),

    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 2}]),
    ?assertEqual(0, length(receive_messages(1))),

    emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

t_wildcard_subscription(Config) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/a/b/c">>,
        <<"this is a retained message 2">>,
        [{qos, 0}, {retain, true}]
    ),
    publish(
        C1,
        <<"/x/y/z">>,
        <<"this is a retained message 3">>,
        [{qos, 0}, {retain, true}],
        Config
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+/b/#">>, 0),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"/+/y/#">>, 0),
    Msgs = receive_messages(4),
    ?assertEqual(4, length(Msgs), #{msgs => Msgs}),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"retained/a/b/c">>, <<"">>, [{qos, 0}, {retain, true}]),
    emqtt:publish(C1, <<"/x/y/z">>, <<"">>, [{qos, 0}, {retain, true}]),
    ok = emqtt:disconnect(C1).

't_wildcard_no_$_prefix'(_Config) ->
    {ok, C0} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C0),
    emqtt:publish(
        C0,
        <<"$test/t/0">>,
        <<"this is a retained message with $ prefix in topic">>,
        [{qos, 1}, {retain, true}]
    ),
    emqtt:publish(
        C0,
        <<"$test/test/1">>,
        <<"this is another retained message with $ prefix in topic">>,
        [{qos, 1}, {retain, true}]
    ),

    emqtt:publish(
        C0,
        <<"t/1">>,
        <<"this is a retained message 1">>,
        [{qos, 1}, {retain, true}]
    ),
    emqtt:publish(
        C0,
        <<"t/2">>,
        <<"this is a retained message 2">>,
        [{qos, 1}, {retain, true}]
    ),
    emqtt:publish(
        C0,
        <<"/t/3">>,
        <<"this is a retained message 3">>,
        [{qos, 1}, {retain, true}]
    ),

    %%%%%%%%%%
    %% C1 subscribes to `#'
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"#">>, 0),
    %% Matched 5 msgs but only receive 3 msgs, 2 ignored
    %% (`$test/t/0` and `$test/test/1` with `$` prefix in topic are ignored)
    Msgs1 = receive_messages(5),
    ?assertMatch(
        %% The order in which messages are received is not always the same as the order in which they are published.
        %% The received order follows the order in which the indexes match.
        %% i.e.
        %%   The first level of the topic `/t/3` is empty.
        %%   So it will be the first message that be matched and be sent.
        [
            #{topic := <<"/t/3">>},
            #{topic := <<"t/1">>},
            #{topic := <<"t/2">>}
        ],
        Msgs1
    ),
    ok = emqtt:disconnect(C1),

    %%%%%%%%%%
    %% C2 subscribes to `$test/#'
    {ok, C2} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C2),
    {ok, #{}, [0]} = emqtt:subscribe(C2, <<"$test/#">>, 0),
    %% Matched 2 msgs and receive them all, no ignored
    Msgs2 = receive_messages(2),
    ?assertMatch(
        [
            #{topic := <<"$test/t/0">>},
            #{topic := <<"$test/test/1">>}
        ],
        Msgs2
    ),
    ok = emqtt:disconnect(C2),

    %%%%%%%%%%
    %% C3 subscribes to `+/+'
    {ok, C3} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C3),
    {ok, #{}, [0]} = emqtt:subscribe(C3, <<"+/+">>, 0),
    %% Matched 2 msgs and receive them all, no ignored
    Msgs3 = receive_messages(2),
    ?assertMatch(
        [
            #{topic := <<"t/1">>},
            #{topic := <<"t/2">>}
        ],
        Msgs3
    ),
    ok = emqtt:disconnect(C3),

    %%%%%%%%%%
    %% C4 subscribes to `+/t/#'
    {ok, C4} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C4),
    {ok, #{}, [0]} = emqtt:subscribe(C4, <<"+/t/#">>, 0),
    %% Matched 2 msgs but only receive 1 msgs, 1 ignored
    %% (`$test/t/0` with `$` prefix in topic are ignored)
    Msgs4 = receive_messages(1),
    ?assertMatch(
        [
            #{topic := <<"/t/3">>}
        ],
        Msgs4
    ),
    ok = emqtt:disconnect(C4),

    ?assertNotReceive(_),

    ok.

t_message_expiry(Config) ->
    ConfMod = fun(Conf) ->
        Conf#{<<"delivery_rate">> := <<"infinity">>}
    end,
    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),

        emqtt:publish(
            C1,
            <<"retained/0">>,
            #{'Message-Expiry-Interval' => 0},
            <<"don't expire">>,
            [{qos, 0}, {retain, true}]
        ),
        emqtt:publish(
            C1,
            <<"retained/1">>,
            #{'Message-Expiry-Interval' => 2},
            <<"expire">>,
            [{qos, 0}, {retain, true}]
        ),
        emqtt:publish(
            C1,
            <<"retained/2">>,
            #{'Message-Expiry-Interval' => 5},
            <<"don't expire">>,
            [{qos, 0}, {retain, true}]
        ),
        emqtt:publish(
            C1,
            <<"retained/3">>,
            <<"don't expire">>,
            [{qos, 0}, {retain, true}]
        ),
        publish(
            C1,
            <<"$SYS/retained/4">>,
            <<"don't expire">>,
            [{qos, 0}, {retain, true}],
            Config
        ),

        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
        ?assertEqual(5, length(receive_messages(5))),
        {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/+">>),
        {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"$SYS/retained/+">>),

        timer:sleep(3000),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, 0),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"$SYS/retained/+">>, 0),
        ?assertEqual(4, length(receive_messages(5))),

        emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
        emqtt:publish(C1, <<"retained/1">>, <<"">>, [{qos, 0}, {retain, true}]),
        emqtt:publish(C1, <<"retained/2">>, <<"">>, [{qos, 0}, {retain, true}]),
        emqtt:publish(C1, <<"retained/3">>, <<"">>, [{qos, 0}, {retain, true}]),
        emqtt:publish(C1, <<"$SYS/retained/4">>, <<"">>, [{qos, 0}, {retain, true}]),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(Config, ConfMod, Case).

t_message_expiry_2(Config) ->
    ConfMod = fun(Conf) ->
        Conf#{<<"msg_expiry_interval">> := <<"2s">>}
    end,
    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),
        publish(C1, <<"retained">>, <<"expire">>, [{qos, 0}, {retain, true}], Config),

        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(1, length(receive_messages(1))),
        timer:sleep(4000),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(0, length(receive_messages(1))),
        {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained">>),

        emqtt:publish(C1, <<"retained">>, <<"">>, [{qos, 0}, {retain, true}]),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(Config, ConfMod, Case).

t_table_full(Config) ->
    ConfMod = fun(Conf) ->
        Conf#{<<"backend">> => #{<<"max_retained_messages">> => <<"1">>}}
    end,
    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),
        emqtt:publish(C1, <<"retained/t/1">>, <<"a">>, [{qos, 0}, {retain, true}]),
        emqtt:publish(C1, <<"retained/t/2">>, <<"b">>, [{qos, 0}, {retain, true}]),
        ct:sleep(100),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/t/1">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(1, length(receive_messages(1))),
        {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/t/2">>, [{qos, 0}, {rh, 0}]),
        ?assertEqual(0, length(receive_messages(1))),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(Config, ConfMod, Case).

t_clean(Config) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    publish(
        C1,
        <<"retained/test/0">>,
        <<"this is a retained message 2">>,
        [{qos, 0}, {retain, true}],
        Config
    ),
    ct:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),

    ok = emqx_retainer:delete(<<"retained/test/0">>),
    ok = emqx_retainer:delete(<<"retained/+">>),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(0, length(receive_messages(3))),

    ok = emqtt:disconnect(C1).

t_stop_publish_clear_msg(_) ->
    emqx_retainer:update_config(#{<<"stop_publish_clear_msg">> => true}),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(1, length(receive_messages(1))),

    emqtt:publish(C1, <<"retained/0">>, <<"">>, [{qos, 0}, {retain, true}]),
    ?assertEqual(0, length(receive_messages(1))),

    emqx_retainer:update_config(#{<<"stop_publish_clear_msg">> => false}),
    ok = emqtt:disconnect(C1).

t_flow_control(_) ->
    %% Setup slow delivery
    emqx_retainer:update_config(#{
        <<"delivery_rate">> => <<"1/1s">>
    }),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqtt:publish(
        C1,
        <<"retained/0">>,
        <<"this is a retained message 0">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/1">>,
        <<"this is a retained message 1">>,
        [{qos, 0}, {retain, true}]
    ),
    emqtt:publish(
        C1,
        <<"retained/2">>,
        <<"this is a retained message 2">>,
        [{qos, 0}, {retain, true}]
    ),
    ct:sleep(100),
    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/#">>, [{qos, 0}, {rh, 0}]),
    ?assertEqual(3, length(receive_messages(3))),
    ok.

t_publish_rate_limit(_) ->
    %% Setup tight publish rates
    emqx_retainer:update_config(#{
        <<"max_publish_rate">> => <<"1/1s">>
    }),

    %% Connect client
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    snabbkaffe:start_trace(),

    %% Should see failure after the first message
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 2),
        #{?snk_kind := retain_failed_for_rate_exceeded_limit, topic := <<"t/2">>},
        5000
    ),

    %% Wait for the refill timer to refill the tokens
    ct:sleep(1100),

    %% Should see success after the first message
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 1),
        #{?snk_kind := retain_within_limit, topic := <<"t/1">>},
        5000
    ),
    %% And a failure again since we have 1 token per second
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 1),
        #{?snk_kind := retain_failed_for_rate_exceeded_limit, topic := <<"t/1">>},
        5000
    ),
    snabbkaffe:stop(),

    %% No more failures after setting the rate limit to infinity
    emqx_retainer:update_config(#{<<"max_publish_rate">> => <<"infinity">>}),
    ?check_trace(
        ?wait_async_action(
            publish_messages(C1, 100),
            #{?snk_kind := retain_within_limit, topic := <<"t/100">>},
            5000
        ),
        fun(Trace) ->
            ?assertEqual(
                [],
                ?of_kind(retain_failed_for_rate_exceeded_limit, Trace)
            )
        end
    ),

    %% Check that the rate limit is still working after restarting the app
    ok = application:stop(emqx_retainer),
    ok = application:ensure_started(emqx_retainer),
    ?check_trace(
        ?wait_async_action(
            publish_messages(C1, 100),
            #{?snk_kind := retain_within_limit, topic := <<"t/100">>},
            5000
        ),
        fun(Trace) ->
            ?assertEqual(
                [],
                ?of_kind(retain_failed_for_rate_exceeded_limit, Trace)
            )
        end
    ),

    %% Start with tight rate limit
    emqx_retainer:update_config(#{<<"max_publish_rate">> => <<"1/1s">>}),
    ok = application:stop(emqx_retainer),
    ok = application:ensure_started(emqx_retainer),

    %% Check that the rate limit is still working
    snabbkaffe:start_trace(),
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 2),
        #{?snk_kind := retain_failed_for_rate_exceeded_limit, topic := <<"t/2">>},
        5000
    ),

    %% Check that the tokens do not accumulate, still only 1 token
    ct:sleep(2200),
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 1),
        #{?snk_kind := retain_within_limit, topic := <<"t/1">>},
        5000
    ),
    {_, {ok, _}} = ?wait_async_action(
        publish_messages(C1, 1),
        #{?snk_kind := retain_failed_for_rate_exceeded_limit, topic := <<"t/1">>},
        5000
    ),

    ok = emqtt:disconnect(C1),
    ok.

t_delete_rate_limit(_) ->
    %% Setup tight publish rates
    emqx_retainer:update_config(#{
        <<"max_publish_rate">> => <<"1/1s">>
    }),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    ?check_trace(
        begin
            {_, {ok, _}} = ?wait_async_action(
                lists:foreach(
                    fun(_) ->
                        emqtt:publish(C1, <<"retained/topic">>, <<"">>, [{qos, 0}, {retain, true}])
                    end,
                    lists:seq(1, 2)
                ),
                #{
                    ?snk_kind := retained_delete_failed_for_rate_exceeded_limit,
                    topic := <<"retained/topic">>
                },
                5000
            )
        end,
        []
    ),

    ok = emqtt:disconnect(C1),
    ok.

t_clear_expired(Config) ->
    ConfMod = fun(Conf) ->
        Conf#{
            <<"msg_clear_interval">> := <<"1s">>,
            <<"msg_expiry_interval">> := <<"3s">>
        }
    end,

    Case = fun() ->
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),

        lists:foreach(
            fun(I) ->
                emqtt:publish(
                    C1,
                    <<"retained/", (I + 60):8/unsigned-integer>>,
                    #{'Message-Expiry-Interval' => 3},
                    <<"retained">>,
                    [{qos, 0}, {retain, true}]
                )
            end,
            lists:seq(1, 5)
        ),
        timer:sleep(1000),

        {ok, _, List} = emqx_retainer:page_read(<<"retained/+">>, _Deadline = 0, 1, 10),
        ?assertEqual(5, erlang:length(List)),

        timer:sleep(4500),

        {ok, _, List2} = emqx_retainer:page_read(<<"retained/+">>, _Deadline = 0, 1, 10),
        ?assertEqual(0, erlang:length(List2)),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(Config, ConfMod, Case).

t_max_payload_size(Config) ->
    ConfMod = fun(Conf) -> Conf#{<<"max_payload_size">> := <<"1kb">>} end,
    Case = fun() ->
        emqx_retainer:clean(),
        timer:sleep(500),
        {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
        {ok, _} = emqtt:connect(C1),
        Payload = iolist_to_binary(lists:duplicate(1024, <<"0">>)),
        emqtt:publish(
            C1,
            <<"retained/1">>,
            #{},
            Payload,
            [{qos, 0}, {retain, true}]
        ),
        emqtt:publish(
            C1,
            <<"retained/2">>,
            #{},
            <<"1", Payload/binary>>,
            [{qos, 0}, {retain, true}]
        ),

        timer:sleep(500),
        {ok, _, List} = emqx_retainer:page_read(<<"retained/+">>, 1, 10),
        ?assertEqual(1, erlang:length(List)),

        ok = emqtt:disconnect(C1)
    end,
    with_conf(Config, ConfMod, Case).

t_page_read(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    ok = emqx_retainer:clean(),
    timer:sleep(500),

    Fun = fun(I) ->
        emqtt:publish(
            C1,
            <<"retained/", (I + 60)>>,
            <<"this is a retained message">>,
            [{qos, 0}, {retain, true}]
        )
    end,
    lists:foreach(Fun, lists:seq(1, 9)),
    timer:sleep(200),

    {ok, _, List} = emqx_retainer:page_read(<<"retained/+">>, 1, 5),
    ?assertEqual(5, length(List)),

    {ok, _, List2} = emqx_retainer:page_read(<<"retained/+">>, 2, 5),
    ?assertEqual(4, length(List2)),

    ok = emqtt:disconnect(C1).

t_only_for_coverage(_) ->
    ?assertEqual(retainer, emqx_retainer_schema:namespace()),
    ignored = gen_server:call(emqx_retainer, unexpected),
    ok = gen_server:cast(emqx_retainer, unexpected),
    unexpected = erlang:send(erlang:whereis(emqx_retainer), unexpected),

    Dispatcher = emqx_retainer_dispatcher:worker(),
    ignored = gen_server:call(Dispatcher, unexpected),
    ok = gen_server:cast(Dispatcher, unexpected),
    unexpected = erlang:send(Dispatcher, unexpected),
    true = erlang:exit(Dispatcher, normal),
    ok.

t_reindex(_) ->
    remove_delivery_rate(),
    {ok, C} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    ok = emqx_retainer:clean(),
    ok = emqx_retainer_mnesia:reindex([[1, 3]], false, fun(_Done) -> ok end),

    %% Prepare retained messages for "retained/N1/N2" topics
    ?check_trace(
        ?wait_async_action(
            lists:foreach(
                fun(N1) ->
                    lists:foreach(
                        fun(N2) ->
                            emqtt:publish(
                                C,
                                erlang:iolist_to_binary(
                                    io_lib:format("retained/~5..0w/~5..0w", [N1, N2])
                                ),
                                <<"this is a retained message">>,
                                [{qos, 0}, {retain, true}]
                            )
                        end,
                        lists:seq(1, 10)
                    )
                end,
                lists:seq(1, 1000)
            ),
            #{?snk_kind := message_retained, topic := <<"retained/01000/00010">>},
            5000
        ),
        []
    ),

    ?check_trace(
        ?wait_async_action(
            begin
                %% Spawn reindexing in the background
                spawn_link(
                    fun() ->
                        timer:sleep(1000),
                        emqx_retainer_mnesia:reindex(
                            [[1, 4]],
                            false,
                            fun(Done) ->
                                ?tp(
                                    info,
                                    reindexing_progress,
                                    #{done => Done}
                                )
                            end
                        )
                    end
                ),

                %% Subscribe to "retained/N/+" for some time, while reindexing is in progress
                T = erlang:monotonic_time(millisecond),
                ok = test_retain_while_reindexing(C, T + 3000)
            end,
            #{?snk_kind := reindexing_progress, done := 10000},
            10000
        ),
        fun(Trace) ->
            ?assertMatch(
                [_ | _],
                lists:filter(
                    fun
                        (#{done := 10000}) -> true;
                        (_) -> false
                    end,
                    ?of_kind(reindexing_progress, Trace)
                )
            )
        end
    ),
    reset_delivery_rate_to_default(),
    ok.

t_get_basic_usage_info(_Config) ->
    ?assertEqual(#{retained_messages => 0}, emqx_retainer:get_basic_usage_info()),
    lists:foreach(
        fun(N) ->
            Num = integer_to_binary(N),
            Message = emqx_message:make(<<"retained/", Num/binary>>, <<"payload">>),
            ok = emqx_retainer_publisher:store_retained(Message)
        end,
        lists:seq(1, 5)
    ),
    ct:sleep(100),
    ?assertEqual(#{retained_messages => 5}, emqx_retainer:get_basic_usage_info()),
    ok.

%% test whether the app can start normally after disabling emqx_retainer
%% fix: https://github.com/emqx/emqx/pull/8911
%%
%% stop/start in enable/disable state
t_disable_then_start(_Config) ->
    %% Disable retainer by config
    set_retain_available(false),
    ?assertWaitEvent(
        erlang:send(emqx_retainer, update_status),
        #{?snk_kind := retainer_status_updated},
        5000
    ),
    ?assertNot(is_retainer_started()),
    ok = application:stop(emqx_retainer),
    ?assertNot(is_retainer_started()),
    ok = application:ensure_started(emqx_retainer),
    ?assertNot(is_retainer_started()),
    set_retain_available(true),
    ?assert(is_retainer_started()),
    ok = application:stop(emqx_retainer),
    ?assertNot(is_retainer_started()),
    ok = application:ensure_started(emqx_retainer),
    ?assert(is_retainer_started()),
    ok.

t_start_stop_on_setting_change(_Config) ->
    emqx_conf:update([zones], #{}, #{override_to => cluster}),
    set_retain_available(false),
    ?assertWaitEvent(
        erlang:send(emqx_retainer, update_status),
        #{?snk_kind := retainer_status_updated},
        5000
    ),
    %% No zones and retainer is disabled, so it should not be started
    ?assertNot(is_retainer_started()),

    %% Enable retainer by default
    set_retain_available(true),
    ?assert(is_retainer_started()),

    %% Disable by default and enable in zone
    set_retain_available(false),
    ?assertWaitEvent(
        erlang:send(emqx_retainer, update_status),
        #{?snk_kind := retainer_status_updated},
        5000
    ),
    ?assertNot(is_retainer_started()),
    set_retain_available_for_zone(default, true),
    ?assert(is_retainer_started()),

    %% Enable by default and disable explicitly in zones, the retainer should still be started
    set_retain_available(true),
    ?assert(is_retainer_started()),
    set_retain_available_for_zone(default, false),
    set_retain_available_for_zone(zone1, false),
    ?assertWaitEvent(
        erlang:send(emqx_retainer, update_status),
        #{?snk_kind := retainer_status_updated},
        5000
    ),
    ?assert(is_retainer_started()),
    ok.

t_disabled(_Config) ->
    ?assertNot(emqx_retainer:is_started()),
    ?assertNot(emqx_retainer:is_enabled()),
    ?assertEqual(ok, emqx_retainer:clean()),
    ?assertEqual({ok, false, []}, emqx_retainer:page_read(undefined, 1, 100)).

t_deliver_when_banned(_) ->
    Client1 = <<"c1">>,
    Client2 = <<"c2">>,

    {ok, C1} = emqtt:start_link([{clientid, Client1}, {clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),

    lists:foreach(
        fun(I) ->
            Topic = erlang:list_to_binary(io_lib:format("retained/~p", [I])),
            Msg = emqx_message:make(Client2, 0, Topic, <<"this is a retained message">>),
            Msg2 = emqx_message:set_flag(retain, Msg),
            emqx:publish(Msg2)
        end,
        lists:seq(1, 3)
    ),

    Now = erlang:system_time(second),
    Who = emqx_banned:who(clientid, Client2),

    emqx_banned:create(#{
        who => Who,
        by => <<"test">>,
        reason => <<"test">>,
        at => Now,
        until => Now + 120
    }),

    timer:sleep(100),

    snabbkaffe:start_trace(),
    {ok, SubRef} =
        snabbkaffe:subscribe(
            ?match_event(#{?snk_kind := ignore_retained_message_due_to_banned}),
            _NEvents = 3,
            _Timeout = 10000,
            0
        ),

    {ok, #{}, [0]} = emqtt:subscribe(C1, <<"retained/+">>, [{qos, 0}, {rh, 0}]),

    {ok, Trace} = snabbkaffe:receive_events(SubRef),
    ?assertEqual(3, length(?of_kind(ignore_retained_message_due_to_banned, Trace))),
    snabbkaffe:stop(),
    emqx_banned:delete(Who),
    {ok, #{}, [0]} = emqtt:unsubscribe(C1, <<"retained/+">>),
    ok = emqtt:disconnect(C1).

t_compatibility_for_deliver_rate(_) ->
    Parser = fun(Conf) ->
        {ok, RawConf} = hocon:binary(Conf, #{format => map}),
        hocon_tconf:check_plain(emqx_retainer_schema, RawConf, #{
            required => false, atom_key => false
        })
    end,
    Infinity = <<"retainer.deliver_rate = \"infinity\"">>,
    ?assertMatch(
        #{
            <<"retainer">> :=
                #{
                    <<"flow_control">> := #{
                        <<"batch_deliver_number">> := 0,
                        <<"batch_read_number">> := 0,
                        <<"batch_deliver_limiter">> := 0
                    }
                }
        },
        Parser(Infinity)
    ),

    R1 = <<"retainer.deliver_rate = \"1000/s\"">>,
    ?assertMatch(
        #{
            <<"retainer">> :=
                #{
                    <<"flow_control">> := #{
                        <<"batch_deliver_number">> := 1000,
                        <<"batch_read_number">> := 1000,
                        <<"batch_deliver_limiter">> := 1.0
                    }
                }
        },
        Parser(R1)
    ),

    DeliveryInf = <<"retainer.delivery_rate = \"infinity\"">>,
    ?assertMatch(
        #{
            <<"retainer">> :=
                #{
                    <<"flow_control">> := #{
                        <<"batch_deliver_number">> := 0,
                        <<"batch_read_number">> := 0,
                        <<"batch_deliver_limiter">> := 0
                    }
                }
        },
        Parser(DeliveryInf)
    ).

t_update_config(_) ->
    OldConf = emqx_config:get_raw([retainer]),
    NewConf = emqx_utils_maps:deep_put([<<"backend">>, <<"storage_type">>], OldConf, <<"disk">>),
    emqx_retainer:update_config(NewConf).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

test_retain_while_reindexing(C, Deadline) ->
    case erlang:monotonic_time(millisecond) > Deadline of
        true ->
            ok;
        false ->
            N = rand:uniform(1000),
            Topic = iolist_to_binary([
                <<"retained/">>,
                io_lib:format("~5..0w", [N]),
                <<"/+">>
            ]),
            {ok, #{}, [0]} = emqtt:subscribe(C, Topic, [{qos, 0}, {rh, 0}]),
            Messages = receive_messages(10),
            ?assertEqual(10, length(Messages)),
            {ok, #{}, [0]} = emqtt:unsubscribe(C, Topic),
            test_retain_while_reindexing(C, Deadline)
    end.

receive_messages(Count) ->
    lists:reverse(
        receive_messages(Count, [])
    ).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            ct:log("Msg: ~p ~n", [Msg]),
            receive_messages(Count - 1, [Msg | Msgs]);
        Other ->
            ct:print("Other Msg: ~p~n", [Other]),
            receive_messages(Count, Msgs)
    after 2000 ->
        Msgs
    end.

with_conf(CTConfig, ConfMod, Case) ->
    Conf = emqx:get_raw_config([retainer]),
    NewConf = ConfMod(Conf),
    emqx_retainer:update_config(NewConf),
    ?config(index, CTConfig) =:= false andalso mria:clear_table(?TAB_INDEX_META),
    try
        Case(),
        {ok, _} = emqx_retainer:update_config(Conf)
    catch
        Type:Error:Strace ->
            emqx_retainer:update_config(Conf),
            erlang:raise(Type, Error, Strace)
    end.

publish(Client, Topic, Payload, Opts, TCConfig) ->
    PublishOpts = publish_opts(TCConfig),
    do_publish(Client, Topic, Payload, Opts, PublishOpts).

publish_opts(TCConfig) ->
    Timeout = proplists:get_value(publish_wait_timeout, TCConfig, undefined),
    Predicate =
        case proplists:get_value(publish_wait_predicate, TCConfig, undefined) of
            undefined -> undefined;
            {NEvents, Pred} -> {predicate, {NEvents, Pred, Timeout}};
            Pred -> {predicate, {1, Pred, Timeout}}
        end,
    Sleep =
        case proplists:get_value(sleep_after_publish, TCConfig, undefined) of
            undefined -> undefined;
            Time -> {sleep, Time}
        end,
    emqx_maybe:define(Predicate, Sleep).

do_publish(Client, Topic, Payload, Opts, undefined) ->
    emqtt:publish(Client, Topic, Payload, Opts);
do_publish(Client, Topic, Payload, Opts, {predicate, {NEvents, Predicate, Timeout}}) ->
    %% Do not delete this clause: it's used by other retainer implementation tests
    {ok, SRef0} = snabbkaffe:subscribe(Predicate, NEvents, Timeout),
    Res = emqtt:publish(Client, Topic, Payload, Opts),
    {ok, _} = snabbkaffe:receive_events(SRef0),
    Res;
do_publish(Client, Topic, Payload, Opts, {sleep, Time}) ->
    %% Do not delete this clause: it's used by other retainer implementation tests
    Res = emqtt:publish(Client, Topic, Payload, Opts),
    ct:sleep(Time),
    Res.

reset_rates_to_default() ->
    emqx_retainer_app:delete_buckets(),
    emqx_retainer:update_config(#{
        <<"delivery_rate">> => <<"1000/s">>,
        <<"max_publish_rate">> => <<"100000/s">>,
        <<"flow_control">> =>
            #{
                <<"batch_read_number">> => 0,
                <<"batch_deliver_number">> => 0
            }
    }),
    emqx_retainer_app:init_buckets().

publish_messages(C1, Count) ->
    lists:foreach(
        fun(I) ->
            emqtt:publish(
                C1,
                iolist_to_binary(io_lib:format("t/~p", [I])),
                <<"this is a retained message">>,
                [{qos, 0}, {retain, true}]
            )
        end,
        lists:seq(1, Count)
    ).

set_retain_available(Enabled) ->
    RawConf0 = emqx_config:get_raw([mqtt]),
    RawConf = maps:put(<<"retain_available">>, Enabled, RawConf0),
    emqx_conf:update([mqtt], RawConf, #{override_to => cluster}).

set_retain_available_for_zone(Zone, Enabled) ->
    RawZoneConf0 = emqx_config:get_raw([zones]),
    RawZoneConf = emqx_utils_maps:deep_put(
        [atom_to_binary(Zone), <<"mqtt">>, <<"retain_available">>],
        RawZoneConf0,
        Enabled
    ),
    emqx_conf:update([zones], RawZoneConf, #{override_to => cluster}).

is_retainer_started() ->
    %% We indirectly check the actual state of the retainer by checking
    %% if the dispatcher has any active workers.
    gproc_pool:active_workers(emqx_retainer_dispatcher) /= [].

remove_delivery_rate() ->
    emqx_limiter_allocator:delete_bucket(?DISPATCHER_LIMITER_ID),
    emqx_retainer:update_config(#{
        <<"delivery_rate">> => <<"infinity">>
    }).

reset_delivery_rate_to_default() ->
    {ok, Rate} = emqx_limiter_schema:to_rate("1000/s"),
    emqx_limiter_allocator:delete_bucket(?DISPATCHER_LIMITER_ID),
    emqx_limiter_allocator:add_bucket(?DISPATCHER_LIMITER_ID, #{rate => Rate, burst => 0}),
    emqx_retainer:update_config(#{
        <<"delivery_rate">> => <<"1000/s">>
    }).
