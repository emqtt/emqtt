%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_replication_layer_shard).

-include_lib("snabbkaffe/include/trace.hrl").

%% API:
-export([start_link/3]).

%% Static server configuration
-export([
    shard_servers/2,
    known_shard_servers/2,
    shard_server/3,
    local_server/2
]).

%% Dynamic server location API
-export([
    servers/3,
    shard_info/3
]).

%% Safe Process Command API
-export([
    process_command/3,
    try_servers/3
]).

%% Membership
-export([
    add_local_server/2,
    drop_local_server/2,
    remove_server/3,
    server_info/2
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type server() :: ra:server_id().

-type server_error() :: server_error(none()).
-type server_error(Reason) ::
    {timeout, server()}
    | {error, server(), Reason}
    | {error, servers_unreachable}.

-define(MEMBERSHIP_CHANGE_TIMEOUT, 30_000).
-define(MIN_BOOSTRAP_RETRY_TIMEOUT, 50).
-define(MAX_BOOSTRAP_RETRY_TIMEOUT, 1_000).

-define(PTERM(DB, SHARD, KEY), {?MODULE, DB, SHARD, KEY}).

%%

start_link(DB, Shard, Opts) ->
    gen_server:start_link(?MODULE, {DB, Shard, Opts}, []).

%% @doc Return a list of servers comprising a shard, according to the information
%% in the DB metadata storage.
%% Note that this indicates which servers _should_ be members of respective Ra
%% cluster, but may at times be out-of-sync with the actual Ra cluster membership.
-spec shard_servers(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> [server()].
shard_servers(DB, Shard) ->
    ReplicaSet = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
    shard_servers(DB, Shard, ReplicaSet).

shard_servers(DB, Shard, [Site | Rest]) ->
    case shard_server(DB, Shard, Site) of
        Server when is_tuple(Server) ->
            [Server | shard_servers(DB, Shard, Rest)];
        undefined ->
            shard_servers(DB, Shard, Rest)
    end;
shard_servers(_DB, _Shard, []) ->
    [].

%% @doc Return a list of servers comprising a shard, according to the information
%% in the DB metadata storage, but excluding any servers residing on nodes not
%% considered to be in the cluster.
known_shard_servers(DB, Shard) ->
    [Server || Server <- shard_servers(DB, Shard), is_server_known(Server)].

%% @doc Return a term identifying a server for the shard located on specified site.
-spec shard_server(
    emqx_ds:db(),
    emqx_ds_replication_layer:shard_id(),
    emqx_ds_replication_layer_meta:site()
) -> server() | undefined.
shard_server(DB, Shard, Site) ->
    case emqx_ds_replication_layer_meta:node(Site) of
        Node when Node =/= undefined ->
            {server_name(DB, Shard, Site), Node};
        undefined ->
            undefined
    end.

%% @doc Return a term identifying a local server for the shard.
-spec local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> server().
local_server(DB, Shard) ->
    {server_name(DB, Shard, local_site()), node()}.

cluster_name(DB, Shard) ->
    DBBin = atom_to_binary(DB),
    <<DBBin/binary, "_", Shard/binary>>.

server_name(DB, Shard, Site) ->
    %% NOTE
    %% Site is redundant as part of server name, keeping for backward / rolling upgrade
    %% compatibility.
    DBBin = atom_to_binary(DB),
    binary_to_atom(<<"ds_", DBBin/binary, Shard/binary, "_", Site/binary>>).

%%

%% @doc Return list of servers for the shard, taking into account runtime information
%% about leadership and cluster connectivity.
%% * `Order` is `leader_preferred`
%%    Result will contain the known leader first, then rest of shard servers. If unknown,
%%    order is unspecified. Use when request is meant to reach the leader, e.g. Ra
%%    command.
%% * `Order` is `local_preferred`
%%    Return list of servers, where the local replica (if exists) is the first element.
%%    Note: result is _NOT_ shuffled. This can be bad for the load balancing, but it
%%    makes results more deterministic. Caller that doesn't care about that can shuffle
%%    the results by itself.
-spec servers(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), Order) -> [server()] when
    Order :: leader_preferred | local_preferred | undefined.
servers(DB, Shard, leader_preferred) ->
    get_servers_leader_preferred(DB, Shard);
servers(DB, Shard, local_preferred) ->
    get_servers_local_preferred(DB, Shard);
servers(DB, Shard, _Order = undefined) ->
    get_shard_servers(DB, Shard).

get_servers_leader_preferred(DB, Shard) ->
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_leader(ClusterName) of
        Leader when Leader /= undefined ->
            Servers = ra_leaderboard:lookup_members(ClusterName),
            [Leader | lists:delete(Leader, Servers)];
        undefined ->
            get_online_servers(DB, Shard)
    end.

get_servers_local_preferred(DB, Shard) ->
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_members(ClusterName) of
        undefined ->
            Servers = get_online_servers(DB, Shard);
        Servers when is_list(Servers) ->
            ok
    end,
    case lists:keytake(node(), 2, Servers) of
        false ->
            Servers;
        {value, Local, Rest} ->
            [Local | Rest]
    end.

lookup_leader(DB, Shard) ->
    %% NOTE
    %% Does not block, but the result may be outdated or even unknown when there's
    %% no servers on the local node.
    ClusterName = get_cluster_name(DB, Shard),
    ra_leaderboard:lookup_leader(ClusterName).

get_online_servers(DB, Shard) ->
    filter_online(get_shard_servers(DB, Shard)).

filter_online(Servers) ->
    case lists:filter(fun is_server_online/1, Servers) of
        [] ->
            %% NOTE: Must return non-empty list.
            Servers;
        Online ->
            Online
    end.

is_server_online({_Name, Node}) ->
    Node == node() orelse lists:member(Node, nodes()).

is_server_known({_Name, Node}) ->
    mria:is_node_in_cluster(Node).

get_cluster_name(DB, Shard) ->
    memoize(fun cluster_name/2, [DB, Shard]).

get_local_server(DB, Shard) ->
    memoize(fun local_server/2, [DB, Shard]).

get_shard_servers(DB, Shard) ->
    maps:get(servers, emqx_ds_replication_shard_allocator:shard_meta(DB, Shard)).

local_site() ->
    emqx_ds_replication_layer_meta:this_site().

%%

-spec shard_info(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), _Info) -> _Value.
shard_info(DB, Shard, ready) ->
    get_shard_info(DB, Shard, ready, false).

%%

-spec process_command([server()], _Command, timeout()) ->
    {ok, _Result, _Leader :: server()} | server_error().
process_command(Servers, Command, Timeout) ->
    try_servers(Servers, fun ra:process_command/3, [Command, Timeout]).

-spec try_servers([server()], function(), [_Arg]) ->
    {ok, _Result, _Leader :: server()} | server_error(_Reason).
try_servers([Server | Rest], Fun, Args) ->
    case is_server_online(Server) andalso erlang:apply(Fun, [Server | Args]) of
        {ok, R, Leader} ->
            {ok, R, Leader};
        _Online = false ->
            ?tp(emqx_ds_replshard_try_next_servers, #{server => Server, reason => offline}),
            try_servers(Rest, Fun, Args);
        {error, Reason = noproc} ->
            ?tp(emqx_ds_replshard_try_next_servers, #{server => Server, reason => Reason}),
            try_servers(Rest, Fun, Args);
        {error, Reason} when Reason =:= nodedown orelse Reason =:= shutdown ->
            %% NOTE
            %% Conceptually, those error conditions basically mean the same as a plain
            %% timeout: "it's impossible to tell if operation has succeeded or not".
            ?tp(emqx_ds_replshard_try_servers_timeout, #{server => Server, reason => Reason}),
            {timeout, Server};
        {timeout, _} = Timeout ->
            ?tp(emqx_ds_replshard_try_servers_timeout, #{server => Server, reason => timeout}),
            Timeout;
        {error, Reason} ->
            {error, Server, Reason}
    end;
try_servers([], _Fun, _Args) ->
    {error, servers_unreachable}.

%%

%% @doc Add a local server to the shard cluster.
%% It's recommended to have the local server running before calling this function.
%% This function is idempotent.
-spec add_local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    ok | emqx_ds:error(_Reason).
add_local_server(DB, Shard) ->
    ShardServers = known_shard_servers(DB, Shard),
    add_local_server(DB, Shard, ShardServers).

add_local_server(DB, Shard, ShardServers = [_ | _]) ->
    %% NOTE
    %% Adding local server as "promotable" member to the cluster, which means
    %% that it won't affect quorum until it is promoted to a voter, which in
    %% turn happens when the server has caught up sufficiently with the log.
    %% We also rely on this "membership" to understand when the server's
    %% ready.
    LocalServer = local_server(DB, Shard),
    case server_info(uid, LocalServer) of
        UID when is_binary(UID) ->
            ServerRecord = #{
                id => LocalServer,
                membership => promotable,
                uid => UID
            };
        unknown ->
            ServerRecord = #{
                id => LocalServer,
                membership => voter
            }
    end,
    Timeout = ?MEMBERSHIP_CHANGE_TIMEOUT,
    case try_servers(ShardServers, fun ra:add_member/3, [ServerRecord, Timeout]) of
        {ok, _, _Leader} ->
            ok;
        {error, _Server, already_member} ->
            ok;
        Error ->
            {error, recoverable, Error}
    end;
add_local_server(_DB, _Shard, _ShardServers = []) ->
    %% NOTE
    %% No active servers to ask to accept us. The most likely situation when this
    %% happens is when all existing shard servers are owned by "lost" nodes, i.e.
    %% those nodes that has (abnormally) left the cluster.
    ok.

%% @doc Remove a local server from the shard cluster and clean up on-disk data.
%% It's required to have the local server running before calling this function.
%% This function is idempotent.
-spec drop_local_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    ok | emqx_ds:error(_Reason).
drop_local_server(DB, Shard) ->
    %% NOTE: Timeouts are ignored, it's a best effort attempt.
    _ = prep_stop_server(DB, Shard),
    LocalServer = local_server(DB, Shard),
    case remove_server(DB, Shard, LocalServer) of
        ok ->
            ra:force_delete_server(DB, LocalServer);
        {error, _, _Reason} = Error ->
            Error
    end.

%% @doc Remove a (remote) server from the shard cluster.
%% The server might not be running when calling this function, e.g. the node
%% might be offline. Because of this, on-disk data will not be cleaned up.
%% This function is idempotent.
-spec remove_server(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), server()) ->
    ok | emqx_ds:error(_Reason).
remove_server(DB, Shard, Server) ->
    ShardServers = known_shard_servers(DB, Shard),
    remove_server(Server, ShardServers).

remove_server(Server, ShardServers = [_ | _]) ->
    Timeout = ?MEMBERSHIP_CHANGE_TIMEOUT,
    case try_servers(ShardServers, fun ra:remove_member/3, [Server, Timeout]) of
        {ok, _, _Leader} ->
            ok;
        {error, _Server, not_member} ->
            ok;
        Error ->
            {error, recoverable, Error}
    end;
remove_server(_Server, _ShardServers = []) ->
    %% NOTE
    %% No active servers to ask to remove us.
    ok.

-spec server_info
    (readiness, server()) -> ready | {unready, _Details} | unknown;
    (leader, server()) -> server() | unknown;
    (uid, server()) -> _UID :: binary() | unknown.
server_info(readiness, Server) ->
    %% NOTE
    %% Server is ready if it's either the leader or a follower with voter "membership"
    %% status (meaning it was promoted after catching up with the log).
    case ra:members(Server) of
        {ok, _Servers, Server} ->
            ready;
        {ok, _Servers, Leader} ->
            member_info(readiness, Server, Leader);
        Error ->
            {unready, {leader_unavailable, Error}}
    end;
server_info(leader, Server) ->
    current_leader(Server);
server_info(uid, Server) ->
    maps:get(uid, ra_overview(Server), unknown).

member_info(readiness, Server, Leader) ->
    case ra:member_overview(Leader) of
        {ok, Overview = #{}, _Leader} ->
            Cluster = maps:get(cluster, Overview, #{}),
            member_readiness(maps:get(Server, Cluster, #{}));
        Error ->
            {unready, {leader_overview_unavailable, Error}}
    end.

current_leader(Server) ->
    %% NOTE: This call will block until the leader is known, or until the timeout.
    case ra:members(Server) of
        {ok, _Servers, Leader} ->
            Leader;
        _Error ->
            unknown
    end.

member_readiness(#{status := Status, voter_status := #{membership := Membership}}) ->
    case Status of
        normal when Membership =:= voter ->
            ready;
        _Other ->
            {unready, {catching_up, Status, Membership}}
    end;
member_readiness(#{}) ->
    unknown.

ra_overview(Server) ->
    case ra:member_overview(Server) of
        {ok, Overview, _Leader} ->
            Overview;
        _Error ->
            #{}
    end.

%%

-record(st, {
    db :: emqx_ds:db(),
    shard :: emqx_ds_replication_layer:shard_id(),
    server :: server(),
    bootstrapped :: boolean(),
    stage :: term()
}).

init({DB, Shard, Opts}) ->
    _ = process_flag(trap_exit, true),
    case start_server(DB, Shard, Opts) of
        {_New = true, Server} ->
            NextStage = trigger_election;
        {_New = false, Server} ->
            NextStage = wait_leader
    end,
    St = #st{
        db = DB,
        shard = Shard,
        server = Server,
        bootstrapped = false,
        stage = NextStage
    },
    {ok, St, {continue, bootstrap}}.

handle_continue(bootstrap, St = #st{bootstrapped = true}) ->
    {noreply, St};
handle_continue(bootstrap, St0 = #st{db = DB, shard = Shard, stage = Stage}) ->
    ?tp(emqx_ds_replshard_bootstrapping, #{db => DB, shard => Shard, stage => Stage}),
    case bootstrap(St0) of
        St = #st{bootstrapped = true} ->
            ?tp(emqx_ds_replshard_bootstrapped, #{db => DB, shard => Shard}),
            {noreply, St};
        St = #st{bootstrapped = false} ->
            {noreply, St, {continue, bootstrap}};
        {retry, Timeout, St} ->
            _TRef = erlang:start_timer(Timeout, self(), bootstrap),
            {noreply, St}
    end.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, bootstrap}, St) ->
    {noreply, St, {continue, bootstrap}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #st{db = DB, shard = Shard}) ->
    %% NOTE: Mark as not ready right away.
    ok = erase_shard_info(DB, Shard),
    %% NOTE: Timeouts are ignored, it's a best effort attempt.
    catch prep_stop_server(DB, Shard),
    LocalServer = get_local_server(DB, Shard),
    ok = ra:stop_server(DB, LocalServer).

%%

bootstrap(St = #st{stage = trigger_election, server = Server}) ->
    ok = trigger_election(Server),
    St#st{stage = wait_leader};
bootstrap(St = #st{stage = wait_leader, server = Server}) ->
    case current_leader(Server) of
        Leader = {_, _} ->
            St#st{stage = {wait_log, Leader}};
        unknown ->
            St
    end;
bootstrap(St = #st{stage = {wait_log, Leader}}) ->
    case ra_overview(Leader) of
        #{commit_index := RaftIdx} ->
            St#st{stage = {wait_log_index, RaftIdx}};
        #{} ->
            St#st{stage = wait_leader}
    end;
bootstrap(St = #st{stage = {wait_log_index, RaftIdx}, db = DB, shard = Shard, server = Server}) ->
    Overview = ra_overview(Server),
    case maps:get(last_applied, Overview, 0) of
        LastApplied when LastApplied >= RaftIdx ->
            ok = announce_shard_ready(DB, Shard),
            St#st{bootstrapped = true, stage = undefined};
        LastApplied ->
            %% NOTE
            %% Blunt estimate of time shard needs to catch up. If this proves to be too long in
            %% practice, it's could be augmented with handling `recover` -> `follower` Ra
            %% member state transition.
            Timeout = min(
                max(?MIN_BOOSTRAP_RETRY_TIMEOUT, RaftIdx - LastApplied),
                ?MAX_BOOSTRAP_RETRY_TIMEOUT
            ),
            {retry, Timeout, St}
    end.

%%

start_server(DB, Shard, #{replication_options := ReplicationOpts}) ->
    ClusterName = cluster_name(DB, Shard),
    LocalServer = local_server(DB, Shard),
    Servers = known_shard_servers(DB, Shard),
    MutableConfig = #{tick_timeout => 100},
    case ra:restart_server(DB, LocalServer, MutableConfig) of
        {error, name_not_registered} ->
            UID = server_uid(DB, Shard),
            Machine = {module, emqx_ds_replication_layer, #{db => DB, shard => Shard}},
            LogOpts = maps:with(
                [
                    snapshot_interval,
                    resend_window
                ],
                ReplicationOpts
            ),
            ok = ra:start_server(DB, MutableConfig#{
                id => LocalServer,
                uid => UID,
                cluster_name => ClusterName,
                initial_members => Servers,
                machine => Machine,
                log_init_args => LogOpts#{uid => UID}
            }),
            {_NewServer = true, LocalServer};
        ok ->
            {_NewServer = false, LocalServer};
        {error, {already_started, _}} ->
            {_NewServer = false, LocalServer}
    end.

trigger_election(Server) ->
    %% NOTE
    %% Triggering election is necessary when a new consensus group is being brought up.
    %% TODO
    %% It's probably a good idea to rebalance leaders across the cluster from time to
    %% time. There's `ra:transfer_leadership/2` for that.
    try ra:trigger_election(Server) of
        ok -> ok
    catch
        %% NOTE
        %% Tolerating exceptions because server might be occupied with log replay for
        %% a while.
        exit:{timeout, _} ->
            ?tp(emqx_ds_replshard_trigger_election, #{server => Server, error => timeout}),
            ok
    end.

announce_shard_ready(DB, Shard) ->
    set_shard_info(DB, Shard, ready, true).

server_uid(_DB, Shard) ->
    %% NOTE
    %% Each new "instance" of a server should have a unique identifier. Otherwise,
    %% if some server migrates to another node during rebalancing, and then comes
    %% back, `ra` will be very confused by it having the same UID as before.
    %% Keeping the shard ID as a prefix to make it easier to identify the server
    %% in the filesystem / logs / etc.
    Ts = integer_to_binary(erlang:system_time(microsecond)),
    <<Shard/binary, "_", Ts/binary>>.

%%

get_shard_info(DB, Shard, K, Default) ->
    persistent_term:get(?PTERM(DB, Shard, K), Default).

set_shard_info(DB, Shard, K, V) ->
    persistent_term:put(?PTERM(DB, Shard, K), V).

erase_shard_info(DB, Shard) ->
    lists:foreach(fun(K) -> erase_shard_info(DB, Shard, K) end, [
        ready
    ]).

erase_shard_info(DB, Shard, K) ->
    persistent_term:erase(?PTERM(DB, Shard, K)).

%%

prep_stop_server(DB, Shard) ->
    prep_stop_server(DB, Shard, 5_000).

prep_stop_server(DB, Shard, Timeout) ->
    LocalServer = get_local_server(DB, Shard),
    Candidates = lists:delete(LocalServer, shard_servers(DB, Shard)),
    case lookup_leader(DB, Shard) of
        LocalServer when Candidates =/= [] ->
            %% NOTE
            %% Trigger leadership transfer *and* force to wait until the new leader
            %% is elected and updated in the leaderboard. This should help to avoid
            %% edge cases where entries appended right before removal are duplicated
            %% due to client retries.
            %% TODO: Candidate may be offline.
            [Candidate | _] = Candidates,
            _ = ra:transfer_leadership(LocalServer, Candidate),
            wait_until(fun() -> lookup_leader(DB, Shard) == Candidate end, Timeout);
        _Another ->
            ok
    end.

%%

memoize(Fun, Args) ->
    %% NOTE: Assuming that the function is pure and never returns `undefined`.
    case persistent_term:get([Fun | Args], undefined) of
        undefined ->
            Result = erlang:apply(Fun, Args),
            _ = persistent_term:put([Fun | Args], Result),
            Result;
        Result ->
            Result
    end.

wait_until(Fun, Timeout) ->
    wait_until(Fun, Timeout, 100).

wait_until(Fun, Timeout, Sleep) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    loop_until(Fun, Deadline, Sleep).

loop_until(Fun, Deadline, Sleep) ->
    case Fun() of
        true ->
            ok;
        false ->
            case erlang:monotonic_time(millisecond) of
                Now when Now < Deadline ->
                    timer:sleep(Sleep),
                    loop_until(Fun, Deadline, Sleep);
                _ ->
                    timeout
            end
    end.
