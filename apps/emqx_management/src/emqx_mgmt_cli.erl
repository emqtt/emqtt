%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_cli).

-feature(maybe_expr, enable).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_cm.hrl").
-include_lib("emqx/include/emqx_router.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DATA_BACKUP_OPTS, #{print_fun => fun emqx_ctl:print/2}).
-define(EXCLUSIVE_TAB, emqx_exclusive_subscription).

-export([load/0]).

-export([
    status/1,
    broker/1,
    cluster/1,
    clients/1,
    topics/1,
    subscriptions/1,
    plugins/1,
    listeners/1,
    vm/1,
    mnesia/1,
    trace/1,
    traces/1,
    log/1,
    authz/1,
    pem_cache/1,
    olp/1,
    data/1,
    ds/1,
    cluster_info/0,
    exclusive/1
]).

-spec load() -> ok.
load() ->
    Cmds = [Fun || {Fun, _} <- ?MODULE:module_info(exports), is_cmd(Fun)],
    lists:foreach(fun(Cmd) -> emqx_ctl:register_command(Cmd, {?MODULE, Cmd}, []) end, Cmds).

is_cmd(Fun) ->
    not lists:member(Fun, [init, load, module_info, cluster_info]).

%%--------------------------------------------------------------------
%% @doc Node status

status([]) ->
    {InternalStatus, _ProvidedStatus} = init:get_status(),
    emqx_ctl:print("Node ~p ~ts is ~p~n", [node(), emqx_app:get_release(), InternalStatus]);
status(_) ->
    emqx_ctl:usage("status", "Show broker status").

%%--------------------------------------------------------------------
%% @doc Query broker

broker([]) ->
    Funs = [sysdescr, version, datetime],
    [emqx_ctl:print("~-10s: ~ts~n", [Fun, emqx_sys:Fun()]) || Fun <- Funs],
    emqx_ctl:print("~-10s: ~ts~n", [
        uptime, emqx_utils_calendar:human_readable_duration_string(emqx_sys:uptime())
    ]);
broker(["stats"]) ->
    [
        emqx_ctl:print("~-30s: ~w~n", [Stat, Val])
     || {Stat, Val} <- lists:sort(emqx_stats:getstats())
    ];
broker(["metrics"]) ->
    [
        emqx_ctl:print("~-30s: ~w~n", [Metric, Val])
     || {Metric, Val} <- lists:sort(emqx_metrics:all())
    ];
broker(_) ->
    emqx_ctl:usage([
        {"broker", "Show broker version, uptime and description"},
        {"broker stats", "Show broker statistics of clients, topics, subscribers"},
        {"broker metrics", "Show broker metrics"}
    ]).

%%-----------------------------------------------------------------------------
%% @doc Cluster with other nodes

cluster(["join", SNode]) ->
    case ekka:join(ekka_node:parse_name(SNode)) of
        ok ->
            emqx_ctl:print("Join the cluster successfully.~n"),
            %% FIXME: running status on the replicant immediately
            %% after join produces stale output
            mria_rlog:role() =:= core andalso
                cluster(["status"]),
            ok;
        ignore ->
            emqx_ctl:print("Ignore.~n");
        {error, Error} ->
            emqx_ctl:print("Failed to join the cluster: ~0p~n", [Error])
    end;
cluster(["leave"]) ->
    _ = maybe_disable_autocluster(),
    case ekka:leave() of
        ok ->
            emqx_ctl:print("Leave the cluster successfully.~n"),
            cluster(["status"]);
        {error, Error} ->
            emqx_ctl:print("Failed to leave the cluster: ~0p~n", [Error])
    end;
cluster(["force-leave", SNode]) ->
    Node = ekka_node:parse_name(SNode),
    case ekka:force_leave(Node) of
        ok ->
            case emqx_cluster_rpc:force_leave_clean(Node) of
                ok ->
                    emqx_ctl:print("Remove the node from cluster successfully.~n"),
                    cluster(["status"]);
                {error, Reason} ->
                    emqx_ctl:print(
                        "Failed to remove the node from cluster_rpc.~n~p~n",
                        [Reason]
                    )
            end;
        ignore ->
            emqx_ctl:print("Ignore.~n");
        {error, Error} ->
            emqx_ctl:print("Failed to remove the node from cluster: ~0p~n", [Error])
    end;
cluster(["status"]) ->
    emqx_ctl:print("Cluster status: ~p~n", [cluster_info()]);
cluster(["status", "--json"]) ->
    Info = sort_map_list_fields(cluster_info()),
    emqx_ctl:print("~ts~n", [emqx_logger_jsonfmt:best_effort_json(Info)]);
cluster(["discovery", "enable"]) ->
    enable_autocluster();
cluster(_) ->
    emqx_ctl:usage([
        {"cluster join <Node>", "Join the cluster"},
        {"cluster leave", "Leave the cluster"},
        {"cluster force-leave <Node>", "Force the node leave from cluster"},
        {"cluster status [--json]", "Cluster status"},
        {"cluster discovery enable", "Enable and run automatic cluster discovery (if configured)"}
    ]).

%% sort lists for deterministic output
sort_map_list_fields(Map) when is_map(Map) ->
    lists:foldl(
        fun(Field, Acc) ->
            sort_map_list_field(Field, Acc)
        end,
        Map,
        maps:keys(Map)
    ).

sort_map_list_field(Field, Map) ->
    case maps:get(Field, Map) of
        [_ | _] = L -> Map#{Field := lists:sort(L)};
        _ -> Map
    end.

enable_autocluster() ->
    ok = ekka:enable_autocluster(),
    _ = ekka:autocluster(emqx),
    emqx_ctl:print("Automatic cluster discovery enabled.~n").

maybe_disable_autocluster() ->
    case ekka:autocluster_enabled() of
        true ->
            ok = ekka:disable_autocluster(),
            emqx_ctl:print(
                "Automatic cluster discovery is disabled on this node: ~p to avoid"
                " re-joining the same cluster again, if the node is not stopped soon."
                " To enable it run: 'emqx ctl cluster discovery enable' or restart the node.~n",
                [node()]
            );
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% @doc Query clients

clients(["list"]) ->
    case ets:info(?CHAN_TAB, size) of
        0 -> emqx_ctl:print("No clients.~n");
        _ -> dump(?CHAN_TAB, client)
    end;
clients(["show", ClientId]) ->
    if_client(ClientId, fun print/1);
clients(["kick", ClientId]) ->
    ok = emqx_cm:kick_session(bin(ClientId)),
    emqx_ctl:print("ok~n");
clients(_) ->
    emqx_ctl:usage([
        {"clients list", "List all clients"},
        {"clients show <ClientId>", "Show a client"},
        {"clients kick <ClientId>", "Kick out a client"}
    ]).

if_client(ClientId, Fun) ->
    case ets:lookup(?CHAN_TAB, (bin(ClientId))) of
        [] -> emqx_ctl:print("Not Found.~n");
        [Channel] -> Fun({client, Channel})
    end.

%%--------------------------------------------------------------------
%% @doc Topics Command

topics(["list"]) ->
    Res =
        emqx_router:foldr_routes(
            fun(Route, Acc) -> [print({emqx_topic, Route}) | Acc] end,
            []
        ),
    case Res of
        [] -> emqx_ctl:print("No topics.~n");
        _ -> ok
    end;
topics(["show", Topic]) ->
    Routes = emqx_router:lookup_routes(Topic),
    [print({emqx_topic, Route}) || Route <- Routes];
topics(_) ->
    emqx_ctl:usage([
        {"topics list", "List all topics"},
        {"topics show <Topic>", "Show a topic"}
    ]).

subscriptions(["list"]) ->
    case ets:info(?SUBOPTION, size) of
        0 ->
            emqx_ctl:print("No subscriptions.~n");
        _ ->
            lists:foreach(
                fun(SubOption) ->
                    print({?SUBOPTION, SubOption})
                end,
                ets:tab2list(?SUBOPTION)
            )
    end;
subscriptions(["show", ClientId]) ->
    case ets:lookup(emqx_subid, bin(ClientId)) of
        [] ->
            emqx_ctl:print("Not Found.~n");
        [{_, Pid}] ->
            case ets:match_object(?SUBOPTION, {{'_', Pid}, '_'}) of
                [] -> emqx_ctl:print("Not Found.~n");
                SubOption -> [print({?SUBOPTION, Sub}) || Sub <- SubOption]
            end
    end;
subscriptions(["add", ClientId, Topic, QoS]) ->
    if_valid_qos(QoS, fun(IntQos) ->
        case ets:lookup(?CHAN_TAB, bin(ClientId)) of
            [] ->
                emqx_ctl:print("Error: Channel not found!");
            [{_, Pid}] ->
                {Topic1, Options} = emqx_topic:parse(bin(Topic)),
                Pid ! {subscribe, [{Topic1, Options#{qos => IntQos}}]},
                emqx_ctl:print("ok~n")
        end
    end);
subscriptions(["del", ClientId, Topic]) ->
    case ets:lookup(?CHAN_TAB, bin(ClientId)) of
        [] ->
            emqx_ctl:print("Error: Channel not found!");
        [{_, Pid}] ->
            Pid ! {unsubscribe, [emqx_topic:parse(bin(Topic))]},
            emqx_ctl:print("ok~n")
    end;
subscriptions(_) ->
    emqx_ctl:usage(
        [
            {"subscriptions list", "List all subscriptions"},
            {"subscriptions show <ClientId>", "Show subscriptions of a client"},
            {"subscriptions add <ClientId> <Topic> <QoS>", "Add a static subscription manually"},
            {"subscriptions del <ClientId> <Topic>", "Delete a static subscription manually"}
        ]
    ).

if_valid_qos(QoS, Fun) ->
    try list_to_integer(QoS) of
        Int when ?IS_QOS(Int) -> Fun(Int);
        _ -> emqx_ctl:print("QoS should be 0, 1, 2~n")
    catch
        _:_ ->
            emqx_ctl:print("QoS should be 0, 1, 2~n")
    end.

plugins(["list"]) ->
    emqx_plugins_cli:list(fun emqx_ctl:print/2);
plugins(["describe", NameVsn]) ->
    emqx_plugins_cli:describe(NameVsn, fun emqx_ctl:print/2);
plugins(["install", NameVsn]) ->
    emqx_plugins_cli:ensure_installed(NameVsn, fun emqx_ctl:print/2);
plugins(["uninstall", NameVsn]) ->
    emqx_plugins_cli:ensure_uninstalled(NameVsn, fun emqx_ctl:print/2);
plugins(["start", NameVsn]) ->
    emqx_plugins_cli:ensure_started(NameVsn, fun emqx_ctl:print/2);
plugins(["stop", NameVsn]) ->
    emqx_plugins_cli:ensure_stopped(NameVsn, fun emqx_ctl:print/2);
plugins(["restart", NameVsn]) ->
    emqx_plugins_cli:restart(NameVsn, fun emqx_ctl:print/2);
plugins(["disable", NameVsn]) ->
    emqx_plugins_cli:ensure_disabled(NameVsn, fun emqx_ctl:print/2);
plugins(["enable", NameVsn]) ->
    emqx_plugins_cli:ensure_enabled(NameVsn, no_move, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "front"]) ->
    emqx_plugins_cli:ensure_enabled(NameVsn, front, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "rear"]) ->
    emqx_plugins_cli:ensure_enabled(NameVsn, rear, fun emqx_ctl:print/2);
plugins(["enable", NameVsn, "before", Other]) ->
    emqx_plugins_cli:ensure_enabled(NameVsn, {before, Other}, fun emqx_ctl:print/2);
plugins(_) ->
    emqx_ctl:usage(
        [
            {"plugins <command> [Name-Vsn]", "e.g. 'start emqx_plugin_template-5.0-rc.1'"},
            {"plugins list", "List all installed plugins"},
            {"plugins describe  Name-Vsn", "Describe an installed plugins"},
            {"plugins install   Name-Vsn",
                "Install a plugin package placed\n"
                "in plugin's install_dir"},
            {"plugins uninstall Name-Vsn",
                "Uninstall a plugin. NOTE: it deletes\n"
                "all files in install_dir/Name-Vsn"},
            {"plugins start     Name-Vsn", "Start a plugin"},
            {"plugins stop      Name-Vsn", "Stop a plugin"},
            {"plugins restart   Name-Vsn", "Stop then start a plugin"},
            {"plugins disable   Name-Vsn", "Disable auto-boot"},
            {"plugins enable    Name-Vsn [Position]",
                "Enable auto-boot at Position in the boot list, where Position could be\n"
                "'front', 'rear', or 'before Other-Vsn' to specify a relative position.\n"
                "The Position parameter can be used to adjust the boot order.\n"
                "If no Position is given, an already configured plugin\n"
                "will stay at is old position; a newly plugin is appended to the rear\n"
                "e.g. plugins disable foo-0.1.0 front\n"
                "     plugins enable bar-0.2.0 before foo-0.1.0"}
        ]
    ).

%%--------------------------------------------------------------------
%% @doc vm command

vm([]) ->
    vm(["all"]);
vm(["all"]) ->
    [vm([Name]) || Name <- ["load", "memory", "process", "io", "ports"]];
vm(["load"]) ->
    [emqx_ctl:print("cpu/~-20s: ~w~n", [L, V]) || {L, V} <- emqx_vm:loads()];
vm(["memory"]) ->
    [emqx_ctl:print("memory/~-17s: ~w~n", [Cat, Val]) || {Cat, Val} <- erlang:memory()];
vm(["process"]) ->
    [
        emqx_ctl:print("process/~-16s: ~w~n", [Name, erlang:system_info(Key)])
     || {Name, Key} <- [{limit, process_limit}, {count, process_count}]
    ];
vm(["io"]) ->
    IoInfo = lists:usort(lists:flatten(erlang:system_info(check_io))),
    [
        emqx_ctl:print("io/~-21s: ~w~n", [Key, proplists:get_value(Key, IoInfo)])
     || Key <- [max_fds, active_fds]
    ];
vm(["ports"]) ->
    [
        emqx_ctl:print("ports/~-18s: ~w~n", [Name, erlang:system_info(Key)])
     || {Name, Key} <- [{count, port_count}, {limit, port_limit}]
    ];
vm(_) ->
    emqx_ctl:usage([
        {"vm all", "Show info of Erlang VM"},
        {"vm load", "Show load of Erlang VM"},
        {"vm memory", "Show memory of Erlang VM"},
        {"vm process", "Show process of Erlang VM"},
        {"vm io", "Show IO of Erlang VM"},
        {"vm ports", "Show Ports of Erlang VM"}
    ]).

%%--------------------------------------------------------------------
%% @doc mnesia Command

mnesia([]) ->
    mnesia:system_info();
mnesia(_) ->
    emqx_ctl:usage([{"mnesia", "Mnesia system info"}]).

%%--------------------------------------------------------------------
%% @doc Logger Command

log(["set-level", Level]) ->
    case emqx_utils:safe_to_existing_atom(Level) of
        {ok, Level1} ->
            case emqx_logger:set_log_level(Level1) of
                ok -> emqx_ctl:print("~ts~n", [Level]);
                Error -> emqx_ctl:print("[error] set overall log level failed: ~p~n", [Error])
            end;
        _ ->
            emqx_ctl:print("[error] invalid level: ~p~n", [Level])
    end;
log(["primary-level"]) ->
    Level = emqx_logger:get_primary_log_level(),
    emqx_ctl:print("~ts~n", [Level]);
log(["primary-level", Level]) ->
    case emqx_utils:safe_to_existing_atom(Level) of
        {ok, Level1} ->
            _ = emqx_logger:set_primary_log_level(Level1),
            ok;
        _ ->
            emqx_ctl:print("[error] invalid level: ~p~n", [Level])
    end,
    emqx_ctl:print("~ts~n", [emqx_logger:get_primary_log_level()]);
log(["handlers", "list"]) ->
    _ = [
        emqx_ctl:print(
            "LogHandler(id=~ts, level=~ts, destination=~ts, status=~ts)~n",
            [Id, Level, Dst, Status]
        )
     || #{
            id := Id,
            level := Level,
            dst := Dst,
            status := Status
        } <- emqx_logger:get_log_handlers()
    ],
    ok;
log(["handlers", "start", HandlerId]) ->
    case emqx_utils:safe_to_existing_atom(HandlerId) of
        {ok, HandlerId1} ->
            case emqx_logger:start_log_handler(HandlerId1) of
                ok ->
                    emqx_ctl:print("log handler ~ts started~n", [HandlerId]);
                {error, Reason} ->
                    emqx_ctl:print("[error] failed to start log handler ~ts: ~p~n", [
                        HandlerId, Reason
                    ])
            end;
        _ ->
            emqx_ctl:print("[error] invalid handler:~ts~n", [HandlerId])
    end;
log(["handlers", "stop", HandlerId]) ->
    case emqx_utils:safe_to_existing_atom(HandlerId) of
        {ok, HandlerId1} ->
            case emqx_logger:stop_log_handler(HandlerId1) of
                ok ->
                    emqx_ctl:print("log handler ~ts stopped~n", [HandlerId1]);
                {error, Reason} ->
                    emqx_ctl:print("[error] failed to stop log handler ~ts: ~p~n", [
                        HandlerId1, Reason
                    ])
            end;
        _ ->
            emqx_ctl:print("[error] invalid handler:~ts~n", [HandlerId])
    end;
log(["handlers", "set-level", HandlerId, Level]) ->
    case emqx_utils:safe_to_existing_atom(HandlerId) of
        {ok, HandlerId1} ->
            case emqx_utils:safe_to_existing_atom(Level) of
                {ok, Level1} ->
                    case emqx_logger:set_log_handler_level(HandlerId1, Level1) of
                        ok ->
                            #{level := NewLevel} = emqx_logger:get_log_handler(HandlerId1),
                            emqx_ctl:print("~ts~n", [NewLevel]);
                        {error, Error} ->
                            emqx_ctl:print("[error] ~p~n", [Error])
                    end;
                _ ->
                    emqx_ctl:print("[error] invalid level:~p~n", [Level])
            end;
        _ ->
            emqx_ctl:print("[error] invalid handler:~ts~n", [HandlerId])
    end;
log(_) ->
    emqx_ctl:usage(
        [
            {"log set-level <Level>", "Set the overall log level"},
            {"log primary-level", "Show the primary log level now"},
            {"log primary-level <Level>", "Set the primary log level"},
            {"log handlers list", "Show log handlers"},
            {"log handlers start <HandlerId>", "Start a log handler"},
            {"log handlers stop  <HandlerId>", "Stop a log handler"},
            {"log handlers set-level <HandlerId> <Level>", "Set log level of a log handler"}
        ]
    ).

%%--------------------------------------------------------------------
%% @doc Trace Command

trace(["list"]) ->
    case emqx_trace_handler:running() of
        [] ->
            emqx_ctl:print("Trace is empty~n", []);
        Traces ->
            lists:foreach(
                fun(Trace) ->
                    #{type := Type, filter := Filter, level := Level, dst := Dst} = Trace,
                    emqx_ctl:print("Trace(~s=~s, level=~s, destination=~0p)~n", [
                        Type, Filter, Level, Dst
                    ])
                end,
                Traces
            )
    end;
trace(["stop", Operation, Filter0]) ->
    case trace_type(Operation, Filter0, text) of
        {ok, Type, Filter, _} -> trace_off(Type, Filter);
        error -> trace([])
    end;
trace(["start", Operation, ClientId, LogFile]) ->
    trace(["start", Operation, ClientId, LogFile, "all"]);
trace(["start", Operation, Filter0, LogFile, Level]) ->
    trace(["start", Operation, Filter0, LogFile, Level, text]);
trace(["start", Operation, Filter0, LogFile, Level, Formatter0]) ->
    case trace_type(Operation, Filter0, Formatter0) of
        {ok, Type, Filter, Formatter} ->
            trace_on(
                name(Filter0),
                Type,
                Filter,
                list_to_existing_atom(Level),
                LogFile,
                Formatter
            );
        error ->
            trace([])
    end;
trace(_) ->
    emqx_ctl:usage([
        {"trace list", "List all traces started on local node"},
        {"trace start client <ClientId> <File> [<Level>] [<Formatter>]",
            "Traces for a client on local node (Formatter=text|json)"},
        {"trace stop  client <ClientId>", "Stop tracing for a client on local node"},
        {"trace start topic  <Topic>    <File> [<Level>] [<Formatter>]",
            "Traces for a topic on local node (Formatter=text|json)"},
        {"trace stop  topic  <Topic> ", "Stop tracing for a topic on local node"},
        {"trace start ip_address  <IP>    <File> [<Level>] [<Formatter>]",
            "Traces for a client ip on local node (Formatter=text|json)"},
        {"trace stop  ip_address  <IP> ", "Stop tracing for a client ip on local node"},
        {"trace start ruleid  <RuleID>    <File> [<Level>] [<Formatter>]",
            "Traces for a rule ID on local node (Formatter=text|json)"},
        {"trace stop  ruleid  <RuleID> ", "Stop tracing for a rule ID on local node"}
    ]).

trace_on(Name, Type, Filter, Level, LogFile, Formatter) ->
    case emqx_trace_handler:install(Name, Type, Filter, Level, LogFile, Formatter) of
        ok ->
            emqx_trace:check(),
            emqx_ctl:print("trace ~s ~s successfully~n", [Filter, Name]);
        {error, Error} ->
            emqx_ctl:print("[error] trace ~s ~s: ~p~n", [Filter, Name, Error])
    end.

trace_off(Type, Filter) ->
    ?TRACE("CLI", "trace_stopping", #{Type => Filter}),
    case emqx_trace_handler:uninstall(Type, name(Filter)) of
        ok ->
            emqx_trace:check(),
            emqx_ctl:print("stop tracing ~s ~s successfully~n", [Type, Filter]);
        {error, Error} ->
            emqx_ctl:print("[error] stop tracing ~s ~s: ~p~n", [Type, Filter, Error])
    end.

%%--------------------------------------------------------------------
%% @doc Trace Cluster Command
-define(DEFAULT_TRACE_DURATION, "1800").

traces(["list"]) ->
    {200, List} = emqx_mgmt_api_trace:trace(get, []),
    case List of
        [] ->
            emqx_ctl:print("Cluster Trace is empty~n", []);
        _ ->
            lists:foreach(
                fun(Trace) ->
                    #{
                        type := Type,
                        name := Name,
                        status := Status,
                        log_size := LogSize
                    } = Trace,
                    emqx_ctl:print(
                        "Trace(~s: ~s=~s, ~s, LogSize:~0p)~n",
                        [Name, Type, maps:get(Type, Trace), Status, LogSize]
                    )
                end,
                List
            )
    end,
    length(List);
traces(["stop", Name]) ->
    trace_cluster_off(Name);
traces(["delete", Name]) ->
    trace_cluster_del(Name);
traces(["start", Name, Operation, Filter]) ->
    traces(["start", Name, Operation, Filter, ?DEFAULT_TRACE_DURATION]);
traces(["start", Name, Operation, Filter, DurationS]) ->
    traces(["start", Name, Operation, Filter, DurationS, text]);
traces(["start", Name, Operation, Filter0, DurationS, Formatter0]) ->
    case trace_type(Operation, Filter0, Formatter0) of
        {ok, Type, Filter, Formatter} -> trace_cluster_on(Name, Type, Filter, DurationS, Formatter);
        error -> traces([])
    end;
traces(_) ->
    emqx_ctl:usage([
        {"traces list", "List all cluster traces started"},
        {"traces start <Name> client <ClientId> [<Duration>] [<Formatter>]",
            "Traces for a client in cluster (Formatter=text|json)"},
        {"traces start <Name> topic <Topic> [<Duration>] [<Formatter>]",
            "Traces for a topic in cluster (Formatter=text|json)"},
        {"traces start <Name> ruleid <RuleID> [<Duration>] [<Formatter>]",
            "Traces for a rule ID in cluster (Formatter=text|json)"},
        {"traces start <Name> ip_address <IPAddr> [<Duration>] [<Formatter>]",
            "Traces for a client IP in cluster\n"
            "Trace will start immediately on all nodes, including the core and replicant,\n"
            "and will end after <Duration> seconds. The default value for <Duration> is "
            ?DEFAULT_TRACE_DURATION
            " seconds. (Formatter=text|json)"},
        {"traces stop <Name>", "Stop trace in cluster"},
        {"traces delete <Name>", "Delete trace in cluster"}
    ]).

trace_cluster_on(Name, Type, Filter, DurationS0, Formatter) ->
    Now = emqx_trace:now_second(),
    DurationS = list_to_integer(DurationS0),
    Trace = #{
        name => bin(Name),
        type => Type,
        Type => bin(Filter),
        start_at => Now,
        end_at => Now + DurationS,
        formatter => Formatter
    },
    case emqx_trace:create(Trace) of
        {ok, _} ->
            emqx_ctl:print("cluster_trace ~p ~s ~s successfully~n", [Type, Filter, Name]);
        {error, Error} ->
            emqx_ctl:print(
                "[error] cluster_trace ~s ~s=~s ~p~n",
                [Name, Type, Filter, Error]
            )
    end.

trace_cluster_del(Name) ->
    case emqx_trace:delete(bin(Name)) of
        ok -> emqx_ctl:print("Del cluster_trace ~s successfully~n", [Name]);
        {error, Error} -> emqx_ctl:print("[error] Del cluster_trace ~s: ~p~n", [Name, Error])
    end.

trace_cluster_off(Name) ->
    case emqx_trace:update(bin(Name), false) of
        ok -> emqx_ctl:print("Stop cluster_trace ~s successfully~n", [Name]);
        {error, Error} -> emqx_ctl:print("[error] Stop cluster_trace ~s: ~p~n", [Name, Error])
    end.

trace_type(Op, Match, "text") -> trace_type(Op, Match, text);
trace_type(Op, Match, "json") -> trace_type(Op, Match, json);
trace_type("client", ClientId, Formatter) -> {ok, clientid, bin(ClientId), Formatter};
trace_type("topic", Topic, Formatter) -> {ok, topic, bin(Topic), Formatter};
trace_type("ip_address", IP, Formatter) -> {ok, ip_address, IP, Formatter};
trace_type(_, _, _) -> error.

%%--------------------------------------------------------------------
%% @doc Listeners Command

listeners([]) ->
    lists:foreach(
        fun({ID, Conf}) ->
            Bind = maps:get(bind, Conf),
            Enable = maps:get(enable, Conf),
            Acceptors = maps:get(acceptors, Conf),
            ProxyProtocol = maps:get(proxy_protocol, Conf, undefined),
            Running = maps:get(running, Conf),
            case Running of
                true ->
                    CurrentConns =
                        case emqx_listeners:current_conns(ID, Bind) of
                            {error, _} -> [];
                            CC -> [{current_conn, CC}]
                        end,
                    MaxConn =
                        case emqx_listeners:max_conns(ID, Bind) of
                            {error, _} -> [];
                            MC -> [{max_conns, MC}]
                        end,
                    ShutdownCount =
                        case emqx_listeners:shutdown_count(ID, Bind) of
                            {error, _} -> [];
                            SC -> [{shutdown_count, SC}]
                        end;
                false ->
                    CurrentConns = [],
                    MaxConn = [],
                    ShutdownCount = []
            end,
            Info =
                [
                    {listen_on, {string, emqx_listeners:format_bind(Bind)}},
                    {acceptors, Acceptors},
                    {proxy_protocol, ProxyProtocol},
                    {enbale, Enable},
                    {running, Running}
                ] ++ CurrentConns ++ MaxConn ++ ShutdownCount,
            emqx_ctl:print("~ts~n", [ID]),
            lists:foreach(fun indent_print/1, Info)
        end,
        emqx_listeners:list()
    );
listeners(["stop", ListenerId]) ->
    case emqx_utils:safe_to_existing_atom(ListenerId) of
        {ok, ListenerId1} ->
            case emqx_listeners:stop_listener(ListenerId1) of
                ok ->
                    emqx_ctl:print("Stop ~ts listener successfully.~n", [ListenerId]);
                {error, Error} ->
                    emqx_ctl:print("Failed to stop ~ts listener: ~0p~n", [ListenerId, Error])
            end;
        _ ->
            emqx_ctl:print("Invalid listener: ~0p~n", [ListenerId])
    end;
listeners(["start", ListenerId]) ->
    case emqx_utils:safe_to_existing_atom(ListenerId) of
        {ok, ListenerId1} ->
            case emqx_listeners:start_listener(ListenerId1) of
                ok ->
                    emqx_ctl:print("Started ~ts listener successfully.~n", [ListenerId]);
                {error, Error} ->
                    emqx_ctl:print("Failed to start ~ts listener: ~0p~n", [ListenerId, Error])
            end;
        _ ->
            emqx_ctl:print("Invalid listener: ~0p~n", [ListenerId])
    end;
listeners(["restart", ListenerId]) ->
    case emqx_utils:safe_to_existing_atom(ListenerId) of
        {ok, ListenerId1} ->
            case emqx_listeners:restart_listener(ListenerId1) of
                ok ->
                    emqx_ctl:print("Restarted ~ts listener successfully.~n", [ListenerId]);
                {error, Error} ->
                    emqx_ctl:print("Failed to restart ~ts listener: ~0p~n", [ListenerId, Error])
            end;
        _ ->
            emqx_ctl:print("Invalid listener: ~0p~n", [ListenerId])
    end;
listeners(["enable", ListenerId, Enable0]) ->
    maybe
        {ok, Enable, Action} ?=
            case Enable0 of
                "true" ->
                    {ok, true, start};
                "false" ->
                    {ok, false, stop};
                _ ->
                    {error, badarg}
            end,
        {ok, #{type := Type, name := Name}} ?= emqx_listeners:parse_listener_id(ListenerId),
        #{<<"enable">> := OldEnable} ?= RawConf = emqx_conf:get_raw(
            [listeners, Type, Name], {error, nout_found}
        ),
        {ok, AtomId} = emqx_utils:safe_to_existing_atom(ListenerId),
        ok ?=
            case Enable of
                OldEnable ->
                    %% `enable` and `running` may lose synchronization due to the start/stop commands
                    case Action of
                        start ->
                            emqx_listeners:start_listener(AtomId);
                        stop ->
                            emqx_listeners:stop_listener(AtomId)
                    end;
                _ ->
                    Conf = RawConf#{<<"enable">> := Enable},
                    case emqx_mgmt_listeners_conf:action(Type, Name, Action, Conf) of
                        {ok, _} ->
                            ok;
                        Error ->
                            Error
                    end
            end,
        emqx_ctl:print("Updated 'enable' to: '~0p' successfully.~n", [Enable])
    else
        {error, badarg} ->
            emqx_ctl:print("Invalid bool argument: ~0p~n", [Enable0]);
        {error, {invalid_listener_id, _Id}} ->
            emqx_ctl:print("Invalid listener: ~0p~n", [ListenerId]);
        {error, not_found} ->
            emqx_ctl:print("Not found listener: ~0p~n", [ListenerId]);
        {error, {already_started, _Pid}} ->
            emqx_ctl:print("Updated 'enable' to: '~0p' successfully.~n", [Enable0]);
        {error, Reason} ->
            emqx_ctl:print("Update listener: ~0p failed, Reason: ~0p~n", [
                ListenerId, Reason
            ])
    end;
listeners(_) ->
    emqx_ctl:usage([
        {"listeners", "List listeners"},
        {"listeners stop    <Identifier>", "Stop a listener"},
        {"listeners start   <Identifier>", "Start a listener"},
        {"listeners restart <Identifier>", "Restart a listener"},
        {"listeners enable <Identifier> <true/false>", "Enable or disable a listener"}
    ]).

%%--------------------------------------------------------------------
%% @doc authz Command

authz(["cache-clean", "node", Node]) ->
    Msg = io_lib:format("Authorization cache drain started on node ~ts", [Node]),
    with_log(fun() -> for_node(fun emqx_mgmt:clean_authz_cache_all/1, Node) end, Msg);
authz(["cache-clean", "all"]) ->
    Msg = "Authorization cache drain started on all nodes",
    with_log(fun emqx_mgmt:clean_authz_cache_all/0, Msg);
authz(["cache-clean", ClientId]) ->
    Msg = io_lib:format("Drain ~ts authz cache", [ClientId]),
    with_log(fun() -> emqx_mgmt:clean_authz_cache(iolist_to_binary(ClientId)) end, Msg);
authz(_) ->
    emqx_ctl:usage(
        [
            {"authz cache-clean all", "Clears authorization cache on all nodes"},
            {"authz cache-clean node <Node>", "Clears authorization cache on given node"},
            {"authz cache-clean <ClientId>", "Clears authorization cache for given client"}
        ]
    ).

pem_cache(["clean", "all"]) ->
    with_log(fun emqx_mgmt:clean_pem_cache_all/0, "PEM cache clean");
pem_cache(["clean", "node", Node]) ->
    Msg = io_lib:format("~ts PEM cache clean", [Node]),
    with_log(fun() -> for_node(fun emqx_mgmt:clean_pem_cache_all/1, Node) end, Msg);
pem_cache(_) ->
    emqx_ctl:usage([
        {"pem_cache clean all", "Clears x509 certificate cache on all nodes"},
        {"pem_cache clean node <Node>", "Clears x509 certificate cache on given node"}
    ]).

%%--------------------------------------------------------------------
%% @doc OLP (Overload Protection related)
olp(["status"]) ->
    S =
        case emqx_olp:is_overloaded() of
            true -> "overloaded";
            false -> "not overloaded"
        end,
    emqx_ctl:print("~p is ~s ~n", [node(), S]);
olp(["disable"]) ->
    Res = emqx_olp:disable(),
    emqx_ctl:print("Disable overload protetion ~p : ~p ~n", [node(), Res]);
olp(["enable"]) ->
    Res = emqx_olp:enable(),
    emqx_ctl:print("Enable overload protection ~p : ~p ~n", [node(), Res]);
olp(_) ->
    emqx_ctl:usage([
        {"olp status", "Return OLP status if system is overloaded"},
        {"olp enable", "Enable overload protection"},
        {"olp disable", "Disable overload protection"}
    ]).

%%--------------------------------------------------------------------
%% @doc data Command

data(["export" | Args]) ->
    maybe
        {ok, Opts} ?= parse_data_export_args(Args),
        {ok, #{filename := Filename}} ?= emqx_mgmt_data_backup:export(Opts),
        emqx_ctl:print("Data has been successfully exported to ~s.~n", [Filename])
    else
        {error, {unknown_root_keys, UnknownKeys}} ->
            Msg = iolist_to_binary([
                <<"Invalid root keys: ">>,
                lists:join(<<", ">>, UnknownKeys)
            ]),
            emqx_ctl:print(Msg);
        {error, {bad_table_sets, InvalidSetNames}} ->
            Msg = iolist_to_binary([
                <<"Invalid table sets: ">>,
                lists:join(<<", ">>, InvalidSetNames)
            ]),
            emqx_ctl:print(Msg);
        {error, Reason} ->
            Reason1 = emqx_mgmt_data_backup:format_error(Reason),
            emqx_ctl:print("[error] Data export failed, reason: ~p.~n", [Reason1])
    end;
data(["import", Filename]) ->
    case emqx_mgmt_data_backup:import(Filename, ?DATA_BACKUP_OPTS) of
        {ok, #{db_errors := DbErrs, config_errors := ConfErrs}} when
            map_size(DbErrs) =:= 0, map_size(ConfErrs) =:= 0
        ->
            emqx_ctl:print("Data has been imported successfully.~n");
        {ok, _} ->
            emqx_ctl:print(
                "Data has been imported, but some errors occurred, see the log above.~n"
            );
        {error, Reason} ->
            Reason1 = emqx_mgmt_data_backup:format_error(Reason),
            emqx_ctl:print("[error] Data import failed, reason: ~p.~n", [Reason1])
    end;
data(_) ->
    emqx_ctl:usage([
        {"data import <File>", "Import data from the specified tar archive file"},
        {
            "data export \\\n"
            "  [--root-keys key1,key2,key3] \\\n"
            "  [--table-sets set1,set2,set3]",
            "Export data"
        }
    ]).

parse_data_export_args(Args) ->
    maybe
        {ok, Collected} ?= collect_data_export_args(Args, #{}),
        emqx_mgmt_data_backup:parse_export_request(Collected)
    end.

collect_data_export_args([], Acc) ->
    {ok, Acc};
collect_data_export_args(["--root-keys", RootKeysJoined | Rest], Acc) ->
    RootKeysStr = string:tokens(RootKeysJoined, [$,]),
    RootKeys = lists:map(fun list_to_binary/1, RootKeysStr),
    collect_data_export_args(Rest, Acc#{<<"root_keys">> => RootKeys});
collect_data_export_args(["--table-sets", TableSetsJoined | Rest], Acc) ->
    TableSetsStr = string:tokens(TableSetsJoined, [$,]),
    TableSets = lists:map(fun list_to_binary/1, TableSetsStr),
    collect_data_export_args(Rest, Acc#{<<"table_sets">> => TableSets});
collect_data_export_args(Args, _Acc) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

%%--------------------------------------------------------------------
%% @doc Durable storage command

-if(?EMQX_RELEASE_EDITION == ee).

ds(CMD) ->
    case emqx_mgmt_api_ds:is_enabled() of
        true ->
            do_ds(CMD);
        false ->
            emqx_ctl:usage([{"ds", "Durable storage is disabled"}])
    end.

do_ds(["info"]) ->
    emqx_ds_replication_layer_meta:print_status(),
    ok;
do_ds(["set-replicas", DBStr | SitesStr]) ->
    case emqx_utils:safe_to_existing_atom(DBStr) of
        {ok, DB} ->
            Sites = lists:map(fun list_to_binary/1, SitesStr),
            case emqx_mgmt_api_ds:update_db_sites(DB, Sites, cli) of
                {ok, _} ->
                    emqx_ctl:print("ok~n");
                {error, Description} ->
                    emqx_ctl:warning("Unable to update replicas: ~s~n", [Description])
            end;
        {error, _} ->
            emqx_ctl:warning("Unknown durable storage")
    end;
do_ds(["set_replicas" | Args]) ->
    do_ds(["set-replicas" | Args]);
do_ds(["join", DBStr, Site]) ->
    case emqx_utils:safe_to_existing_atom(DBStr) of
        {ok, DB} ->
            case emqx_mgmt_api_ds:join(DB, list_to_binary(Site), cli) of
                {ok, unchanged} ->
                    emqx_ctl:print("unchanged~n");
                {ok, _} ->
                    emqx_ctl:print("ok~n");
                {error, Description} ->
                    emqx_ctl:warning("Unable to update replicas: ~s~n", [Description])
            end;
        {error, _} ->
            emqx_ctl:warning("Unknown durable storage~n")
    end;
do_ds(["leave", DBStr, Site]) ->
    case emqx_utils:safe_to_existing_atom(DBStr) of
        {ok, DB} ->
            case emqx_mgmt_api_ds:leave(DB, list_to_binary(Site), cli) of
                {ok, unchanged} ->
                    emqx_ctl:print("unchanged~n");
                {ok, _} ->
                    emqx_ctl:print("ok~n");
                {error, Description} ->
                    emqx_ctl:warning("Unable to update replicas: ~s~n", [Description])
            end;
        {error, _} ->
            emqx_ctl:warning("Unknown durable storage~n")
    end;
do_ds(["forget", Site]) ->
    case emqx_mgmt_api_ds:forget(list_to_binary(Site), cli) of
        ok ->
            emqx_ctl:print("ok~n");
        {error, Description} ->
            emqx_ctl:warning("Unable to forget site: ~s~n", [Description])
    end;
do_ds(_) ->
    emqx_ctl:usage([
        {"ds info", "Show overview of the embedded durable storage state"},
        {"ds set-replicas <storage> <site1> <site2> ...",
            "Change the replica set of the durable storage"},
        {"ds join <storage> <site>", "Add site to the replica set of the storage"},
        {"ds leave <storage> <site>", "Remove site from the replica set of the storage"},
        {"ds forget <site>", "Remove a site from the list of known sites"}
    ]).

-else.

ds(_CMD) ->
    emqx_ctl:usage([{"ds", "DS CLI is not available in this edition of EMQX"}]).

-endif.

%%--------------------------------------------------------------------
%% Dump ETS
%%--------------------------------------------------------------------

dump(Table, Tag) ->
    dump(Table, Tag, ets:first(Table), []).

dump(_Table, _, '$end_of_table', Result) ->
    lists:reverse(Result);
dump(Table, Tag, Key, Result) ->
    PrintValue = [print({Tag, Record}) || Record <- ets:lookup(Table, Key)],
    dump(Table, Tag, ets:next(Table, Key), [PrintValue | Result]).

print({_, []}) ->
    ok;
print({client, {ClientId, ChanPid}}) ->
    Attrs =
        case emqx_cm:get_chan_info(ClientId, ChanPid) of
            undefined -> #{};
            Attrs0 -> Attrs0
        end,
    Stats =
        case emqx_cm:get_chan_stats(ClientId, ChanPid) of
            undefined -> #{};
            Stats0 -> maps:from_list(Stats0)
        end,
    ClientInfo = maps:get(clientinfo, Attrs, #{}),
    ConnInfo = maps:get(conninfo, Attrs, #{}),
    Session = maps:get(session, Attrs, #{}),
    Connected =
        case maps:get(conn_state, Attrs) of
            connected -> true;
            _ -> false
        end,
    Info = lists:foldl(
        fun(Items, Acc) ->
            maps:merge(Items, Acc)
        end,
        #{connected => Connected},
        [
            maps:with(
                [
                    subscriptions_cnt,
                    inflight_cnt,
                    awaiting_rel_cnt,
                    mqueue_len,
                    mqueue_dropped,
                    send_msg
                ],
                Stats
            ),
            maps:with([clientid, username], ClientInfo),
            maps:with(
                [
                    peername,
                    clean_start,
                    keepalive,
                    expiry_interval,
                    connected_at,
                    disconnected_at
                ],
                ConnInfo
            ),
            maps:with([created_at], Session)
        ]
    ),
    InfoKeys =
        [
            clientid,
            username,
            peername,
            clean_start,
            keepalive,
            expiry_interval,
            subscriptions_cnt,
            inflight_cnt,
            awaiting_rel_cnt,
            send_msg,
            mqueue_len,
            mqueue_dropped,
            connected,
            created_at,
            connected_at
        ] ++
            case maps:is_key(disconnected_at, Info) of
                true -> [disconnected_at];
                false -> []
            end,
    Info1 = Info#{expiry_interval => maps:get(expiry_interval, Info) div 1000},
    emqx_ctl:print(
        "Client(~ts, username=~ts, peername=~ts, clean_start=~ts, "
        "keepalive=~w, session_expiry_interval=~w, subscriptions=~w, "
        "inflight=~w, awaiting_rel=~w, delivered_msgs=~w, enqueued_msgs=~w, "
        "dropped_msgs=~w, connected=~ts, created_at=~w, connected_at=~w" ++
            case maps:is_key(disconnected_at, Info1) of
                true -> ", disconnected_at=~w)~n";
                false -> ")~n"
            end,
        [format(K, maps:get(K, Info1)) || K <- InfoKeys]
    );
print({emqx_topic, #route{topic = Topic, dest = {_, Node}}}) ->
    emqx_ctl:print("~ts -> ~ts~n", [Topic, Node]);
print({emqx_topic, #route{topic = Topic, dest = Node}}) ->
    emqx_ctl:print("~ts -> ~ts~n", [Topic, Node]);
print({?SUBOPTION, {{Topic, Pid}, Options}}) when is_pid(Pid) ->
    SubId = maps:get(subid, Options),
    QoS = maps:get(qos, Options, 0),
    NL = maps:get(nl, Options, 0),
    RH = maps:get(rh, Options, 0),
    RAP = maps:get(rap, Options, 0),
    emqx_ctl:print("~ts -> topic:~ts qos:~p nl:~p rh:~p rap:~p~n", [SubId, Topic, QoS, NL, RH, RAP]);
print({exclusive, {exclusive_subscription, Topic, ClientId}}) ->
    emqx_ctl:print("topic:~ts -> ClientId:~ts~n", [Topic, ClientId]).

format(_, undefined) ->
    undefined;
format(peername, {IPAddr, Port}) ->
    IPStr = emqx_mgmt_util:ntoa(IPAddr),
    io_lib:format("~ts:~p", [IPStr, Port]);
format(_, Val) ->
    Val.

bin(S) -> iolist_to_binary(S).

indent_print({Key, {string, Val}}) ->
    emqx_ctl:print("  ~-16s: ~ts~n", [Key, Val]);
indent_print({Key, Val}) ->
    emqx_ctl:print("  ~-16s: ~w~n", [Key, Val]).

name(Filter) ->
    iolist_to_binary(["CLI-", Filter]).

for_node(Fun, Node) ->
    try list_to_existing_atom(Node) of
        NodeAtom ->
            Fun(NodeAtom)
    catch
        error:badarg ->
            {error, unknown_node}
    end.

with_log(Fun, Msg) ->
    Res = Fun(),
    case Res of
        ok ->
            emqx_ctl:print("~s OK~n", [Msg]);
        {error, Reason} ->
            emqx_ctl:print("~s FAILED~n~p~n", [Msg, Reason])
    end,
    Res.

cluster_info() ->
    RunningNodes = safe_call_mria(running_nodes, [], []),
    StoppedNodes = safe_call_mria(cluster_nodes, [stopped], []),
    #{
        running_nodes => RunningNodes,
        stopped_nodes => StoppedNodes
    }.

%% CLI starts before mria, so we should handle errors gracefully:
safe_call_mria(Fun, Args, OnFail) ->
    try
        apply(mria, Fun, Args)
    catch
        EC:Err:Stack ->
            ?SLOG(warning, #{
                msg => "call_to_mria_failed",
                call => {mria, Fun, Args},
                EC => Err,
                stacktrace => Stack
            }),
            OnFail
    end.
%%--------------------------------------------------------------------
%% @doc Exclusive topics
exclusive(["list"]) ->
    case ets:info(?EXCLUSIVE_TAB, size) of
        0 -> emqx_ctl:print("No topics.~n");
        _ -> dump(?EXCLUSIVE_TAB, exclusive)
    end;
exclusive(["delete", Topic0]) ->
    Topic = erlang:iolist_to_binary(Topic0),
    case emqx_exclusive_subscription:dirty_lookup_clientid(Topic) of
        undefined ->
            ok;
        ClientId ->
            case emqx_mgmt:unsubscribe(ClientId, Topic) of
                {unsubscribe, _} ->
                    ok;
                {error, channel_not_found} ->
                    emqx_exclusive_subscription:unsubscribe(Topic, #{is_exclusive => true})
            end
    end,
    emqx_ctl:print("ok~n");
exclusive(_) ->
    emqx_ctl:usage([
        {"exclusive list", "List all exclusive topics"},
        {"exclusive delete <Topic>", "Delete an exclusive topic"}
    ]).
