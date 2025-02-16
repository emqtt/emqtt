%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Metadata storage for the builtin sharded database.
%%
%% Currently metadata is stored in mria; that's not ideal, but
%% eventually we'll replace it, so it's important not to leak
%% implementation details from this module.
-module(emqx_ds_replication_layer_meta).

-feature(maybe_expr, enable).
-compile(inline).

-behaviour(gen_server).

%% API:
-export([
    shards/1,
    my_shards/1,
    shard_info/2,
    allocate_shards/1,
    replica_set/2,
    sites/0,
    node/1,
    this_site/0,
    forget_site/1,
    print_status/0
]).

%% DB API:
-export([
    open_db/2,
    db_config/1,
    update_db_config/2,
    drop_db/1,
    dbs/0
]).

%% Site / shard allocation:
-export([
    join_db_site/2,
    leave_db_site/2,
    assign_db_sites/2,
    replica_set_transitions/2,
    claim_transition/3,
    update_replica_set/3,
    db_sites/1,
    target_set/2
]).

%% Subscriptions to changes:
-export([
    subscribe/2,
    unsubscribe/1
]).

%% gen_server
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    open_db_trans/2,
    allocate_shards_trans/1,
    assign_db_sites_trans/2,
    modify_db_sites_trans/2,
    claim_transition_trans/3,
    update_replica_set_trans/3,
    update_db_config_trans/2,
    drop_db_trans/1,
    claim_site_trans/2,
    forget_site_trans/1,
    n_shards/1
]).

%% Migrations:
-export([
    migrate_node_table/0,
    migrate_shard_table/0,
    migrate_node_table_trans/1,
    migrate_shard_table_trans/1
]).

-export_type([
    site/0,
    transition/0,
    subscription_event/0,
    update_cluster_result/0
]).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(SERVER, ?MODULE).

-define(RLOG_SHARD, emqx_ds_builtin_metadata_shard).
%% DS database metadata:
-define(META_TAB, emqx_ds_builtin_metadata_tab).
%% Mapping from Site to the actual Erlang node:
-define(NODE_TAB, emqx_ds_builtin_node_tab2).
-define(NODE_TAB_LEGACY, emqx_ds_builtin_node_tab).
%% Shard metadata:
-define(SHARD_TAB, emqx_ds_builtin_shard_tab2).
-define(SHARD_TAB_LEGACY, emqx_ds_builtin_shard_tab).
%% Membership transitions:
-define(TRANSITION_TAB, emqx_ds_builtin_trans_tab).

-record(?META_TAB, {
    db :: emqx_ds:db(),
    db_props :: emqx_ds_replication_layer:builtin_db_opts()
}).

-record(?NODE_TAB, {
    site :: site(),
    node :: node(),
    misc = #{} :: map()
}).

-record(?SHARD_TAB, {
    shard :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()},
    %% Sites that currently contain the data:
    replica_set :: [site()],
    %% Sites that should contain the data when the cluster is in the
    %% stable state (no nodes are being added or removed from it):
    target_set :: [site()] | undefined,
    misc = #{} :: map()
}).

-record(?TRANSITION_TAB, {
    shard :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()},
    transition :: transition(),
    misc = #{} :: map()
}).

%% Persistent ID of the node (independent from the IP/FQDN):
-type site() :: binary().

%% Membership transition of shard's replica set:
-type transition() :: {add | del, site()}.

-type update_cluster_result() ::
    {ok, unchanged | [site()]}
    | {error, {nonexistent_db, emqx_ds:db()}}
    | {error, {nonexistent_sites, [site()]}}
    | {error, {too_few_sites, [site()]}}
    | {error, _}.

%% Subject of the subscription:
-type subject() :: emqx_ds:db().

%% Event for the subscription:
-type subscription_event() ::
    {changed, {shard, emqx_ds:db(), emqx_ds_replication_layer:shard_id()}}.

%% Peristent term key:
-define(emqx_ds_builtin_site, emqx_ds_builtin_site).

%% Make Dialyzer happy
-define(NODE_PAT(),
    %% Equivalent of `#?NODE_TAB{_ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_')
).

-define(NODE_PAT(NODE),
    %% Equivalent of `#?NODE_TAB{node = NODE, _ = '_'}`:
    erlang:make_tuple(record_info(size, ?NODE_TAB), '_', [{#?NODE_TAB.node, NODE}])
).

-define(SHARD_PAT(SHARD),
    %% Equivalent of `#?SHARD_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?SHARD_TAB), '_', [{#?SHARD_TAB.shard, SHARD}])
).

-define(TRANSITION_PAT(SHARD),
    %% Equivalent of `#?TRANSITION_TAB{shard = SHARD, _ = '_'}`
    erlang:make_tuple(record_info(size, ?TRANSITION_TAB), '_', [{#?TRANSITION_TAB.shard, SHARD}])
).

%%================================================================================
%% API functions
%%================================================================================

-spec this_site() -> site().
this_site() ->
    persistent_term:get(?emqx_ds_builtin_site).

-spec n_shards(emqx_ds:db()) -> pos_integer().
n_shards(DB) ->
    [#?META_TAB{db_props = #{n_shards := NShards}}] = mnesia:dirty_read(?META_TAB, DB),
    NShards.

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
shards(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}} <- Recs].

-spec shard_info(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    #{replica_set := #{site() => #{status => up | down}}}
    | undefined.
shard_info(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [] ->
            undefined;
        [#?SHARD_TAB{replica_set = Replicas}] ->
            ReplicaSet = maps:from_list([
                begin
                    Status =
                        case mria:cluster_status(?MODULE:node(I)) of
                            running -> up;
                            stopped -> down;
                            false -> down
                        end,
                    ReplInfo = #{status => Status},
                    {I, ReplInfo}
                end
             || I <- Replicas
            ]),
            #{replica_set => ReplicaSet}
    end.

-spec my_shards(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
my_shards(DB) ->
    Site = this_site(),
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    [Shard || #?SHARD_TAB{shard = {_, Shard}, replica_set = RS} <- Recs, lists:member(Site, RS)].

allocate_shards(DB) ->
    case mria:transaction(?RLOG_SHARD, fun ?MODULE:allocate_shards_trans/1, [DB]) of
        {atomic, Shards} ->
            {ok, Shards};
        {aborted, {shards_already_allocated, Shards}} ->
            {ok, Shards};
        {aborted, {insufficient_sites_online, Needed, Sites}} ->
            {error, #{reason => insufficient_sites_online, needed => Needed, sites => Sites}}
    end.

-spec sites() -> [site()].
sites() ->
    [R#?NODE_TAB.site || R <- mnesia:dirty_match_object(?NODE_TAB, ?NODE_PAT())].

-spec node(site()) -> node() | undefined.
node(Site) ->
    case mnesia:dirty_read(?NODE_TAB, Site) of
        [#?NODE_TAB{node = Node}] ->
            Node;
        [] ->
            undefined
    end.

-spec forget_site(site()) -> ok | {error, _Reason}.
forget_site(Site) ->
    maybe
        [Record] ?= mnesia:dirty_read(?NODE_TAB, Site),
        false ?= node_status(Record#?NODE_TAB.node),
        %% Node is lost, proceed.
        transaction(fun ?MODULE:forget_site_trans/1, [Record])
    else
        [] ->
            {error, nonexistent_site};
        running ->
            {error, site_online};
        stopped ->
            %% Node is stopped, reject the request.
            %% If it's gone, it should leave the cluster first.
            {error, site_temporarily_offline}
    end.

%%===============================================================================

-spec print_status() -> ok.
print_status() ->
    %% TODO: Consistent view of state.
    Nodes = all_nodes(),
    Shards = all_shards(),
    Transitions = all_transitions(),
    print_status(Nodes, Shards, Transitions).

print_status(Nodes, Shards, Transitions) ->
    PendingTransitions = lists:filtermap(
        fun(Record = #?SHARD_TAB{shard = DBShard}) ->
            ClaimedTs = [T || T = #?TRANSITION_TAB{shard = S} <- Transitions, S == DBShard],
            case compute_transitions(Record, ClaimedTs) of
                [] -> false;
                ShardTransitions -> {true, {DBShard, ShardTransitions}}
            end
        end,
        Shards
    ),
    %% This site
    io:format("THIS SITE:~n"),
    try this_site() of
        Site -> io:format("~s~n", [Site])
    catch
        error:badarg ->
            io:format(
                "(!) UNCLAIMED~n"
                "(!) Likely this node's name is already known as another site in the cluster.~n"
                "(!) Please resolve conflicts manually.~n"
            )
    end,
    %% Sites information
    io:format("~nSITES:~n", []),
    print_table(
        ["Site", "Node", "Status"],
        [
            [Site, Node, format_node_status(node_status(Node))]
         || #?NODE_TAB{site = Site, node = Node} <- Nodes
        ]
    ),
    NodesLost = [Node || #?NODE_TAB{node = Node} <- Nodes, node_status(Node) == false],
    NodesLost =/= [] andalso
        io:format(
            "(!) ATTENTION~n"
            "(!) One or more sites are lost, replicas under their ownership are gone.~n"
            "(!) Availability may be compromised.~n"
            "(!) Please take actions to bring the cluster back to healthy state.~n"
        ),
    %% Shards information
    io:format("~nSHARDS:~n"),
    print_table(
        ["DB/Shard", "Replicas", "Transitions"],
        [
            [
                format_shard(DBShard),
                {group, [
                    {subcolumns, [
                        format_replicas(RS, Nodes), format_transitions(ShardTransitions)
                    ]}
                ]}
            ]
         || #?SHARD_TAB{shard = DBShard, replica_set = RS} <- Shards,
            ShardTransitions <- [proplists:get_value(DBShard, PendingTransitions, [])]
        ]
    ),
    TransitionsStuck = [
        DBShard
     || #?SHARD_TAB{shard = DBShard, replica_set = RS} <- Shards,
        RSLost <- [[Site || Site <- RS, site_status(Site, Nodes) == false]],
        length(RSLost) * 2 >= length(RS)
    ],
    TransitionsStuck =/= [] andalso
        io:format(
            "(!) ATTENTION~n"
            "(!) One or more shards have replica set where majority of replicas are gone.~n"
            "(!) Membership changes are compromised, pending transitions may never finish.~n"
            "(!) Please take necessary steps to deal with lost sites.~n"
            "(!) Prepare for the possibility of data loss.~n"
        ),
    ok.

format_shard({DB, Shard}) ->
    io_lib:format("~p/~s", [DB, Shard]).

format_replicas(RS, Nodes) ->
    [format_replica(R, Nodes) || R <- RS].

format_replica(Site, Nodes) ->
    [Site, format_node_marker(site_status(Site, Nodes))].

site_status(Site, Nodes) ->
    [Node] = [N || #?NODE_TAB{site = S, node = N} <- Nodes, S == Site],
    node_status(Node).

format_transitions(Transitions) ->
    [format_transition(T) || T <- Transitions].

format_transition({add, Site}) ->
    ["+ ", Site];
format_transition({del, Site}) ->
    ["- ", Site].

format_node_status(Status) ->
    case Status of
        running -> "    up";
        stopped -> "(x) down";
        false -> "(!) LOST"
    end.

format_node_marker(Status) ->
    case Status of
        running -> "";
        stopped -> " (x)";
        false -> " (!)"
    end.

node_status(Node) ->
    mria:cluster_status(Node).

print_table(Header, Rows) ->
    io:put_chars(emqx_utils_fmt:table(Header, Rows)).

%%===============================================================================
%% DB API
%%===============================================================================

-spec db_config(emqx_ds:db()) -> emqx_ds_replication_layer:builtin_db_opts() | #{}.
db_config(DB) ->
    case mnesia:dirty_read(?META_TAB, DB) of
        [#?META_TAB{db_props = Opts}] ->
            Opts;
        [] ->
            #{}
    end.

-spec open_db(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db(DB, DefaultOpts) ->
    transaction(fun ?MODULE:open_db_trans/2, [DB, DefaultOpts]).

-spec update_db_config(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts() | {error, nonexistent_db}.
update_db_config(DB, DefaultOpts) ->
    transaction(fun ?MODULE:update_db_config_trans/2, [DB, DefaultOpts]).

-spec drop_db(emqx_ds:db()) -> ok.
drop_db(DB) ->
    transaction(fun ?MODULE:drop_db_trans/1, [DB]).

-spec dbs() -> [emqx_ds:db()].
dbs() ->
    mnesia:dirty_all_keys(?META_TAB).

%%===============================================================================
%% Site / shard allocation API
%%===============================================================================

%% @doc Join a site to the set of sites the DB is replicated across.
-spec join_db_site(emqx_ds:db(), site()) -> update_cluster_result().
join_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{add, Site}]]).

%% @doc Make a site leave the set of sites the DB is replicated across.
-spec leave_db_site(emqx_ds:db(), site()) -> update_cluster_result().
leave_db_site(DB, Site) ->
    transaction(fun ?MODULE:modify_db_sites_trans/2, [DB, [{del, Site}]]).

%% @doc Assign a set of sites to the DB for replication.
-spec assign_db_sites(emqx_ds:db(), [site()]) -> update_cluster_result().
assign_db_sites(DB, Sites) ->
    transaction(fun ?MODULE:assign_db_sites_trans/2, [DB, Sites]).

%% @doc List the sites the DB is replicated across.
-spec db_sites(emqx_ds:db()) -> [site()].
db_sites(DB) ->
    Recs = mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'})),
    list_db_sites(Recs).

%% @doc List the sequence of transitions that should be conducted in order to
%% bring the set of replicas for a DB shard in line with the target set.
-spec replica_set_transitions(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [transition()] | undefined.
replica_set_transitions(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [Record] ->
            PendingTransitions = mnesia:dirty_read(?TRANSITION_TAB, {DB, Shard}),
            compute_transitions(Record, PendingTransitions);
        [] ->
            undefined
    end.

%% @doc Claim the intention to start the replica set transition for the given shard.
%% To be called before starting acting on transition, so that information about this
%% will not get lost. Once it finishes, call `update_replica_set/3`.
-spec claim_transition(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), transition()) ->
    ok | {error, {conflict, transition()} | {outdated, _Expected :: [transition()]}}.
claim_transition(DB, Shard, Trans) ->
    transaction(fun ?MODULE:claim_transition_trans/3, [DB, Shard, Trans]).

%% @doc Update the set of replication sites for a shard.
%% To be called after a `transition()` has been conducted successfully.
-spec update_replica_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), transition()) -> ok.
update_replica_set(DB, Shard, Trans) ->
    transaction(fun ?MODULE:update_replica_set_trans/3, [DB, Shard, Trans]).

%% @doc Get the current set of replication sites for a shard.
-spec replica_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [site()] | undefined.
replica_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{replica_set = ReplicaSet}] ->
            ReplicaSet;
        [] ->
            undefined
    end.

%% @doc Get the target set of replication sites for a DB shard.
%% Target set is updated every time the set of replication sites for the DB changes.
%% See `join_db_site/2`, `leave_db_site/2`, `assign_db_sites/2`.
-spec target_set(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) ->
    [site()] | undefined.
target_set(DB, Shard) ->
    case mnesia:dirty_read(?SHARD_TAB, {DB, Shard}) of
        [#?SHARD_TAB{target_set = TargetSet}] ->
            TargetSet;
        [] ->
            undefined
    end.

%%================================================================================

subscribe(Pid, Subject) ->
    gen_server:call(?SERVER, {subscribe, Pid, Subject}, infinity).

unsubscribe(Pid) ->
    gen_server:call(?SERVER, {unsubscribe, Pid}, infinity).

%%================================================================================
%% behavior callbacks
%%================================================================================

-record(s, {
    subs = #{} :: #{pid() => {subject(), _Monitor :: reference()}}
}).

init([]) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => [ds, meta]}),
    ok = ekka:monitor(membership),
    ensure_tables(),
    run_migrations(),
    ensure_site(),
    S = #s{},
    {ok, _Node} = mnesia:subscribe({table, ?SHARD_TAB, simple}),
    {ok, S}.

handle_call({subscribe, Pid, Subject}, _From, S) ->
    {reply, ok, handle_subscribe(Pid, Subject, S)};
handle_call({unsubscribe, Pid}, _From, S) ->
    {reply, ok, handle_unsubscribe(Pid, S)};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info({mnesia_table_event, {write, #?SHARD_TAB{shard = {DB, Shard}}, _}}, S) ->
    ok = notify_subscribers(DB, {shard, DB, Shard}, S),
    {noreply, S};
handle_info({'DOWN', _MRef, process, Pid, _Reason}, S) ->
    {noreply, handle_unsubscribe(Pid, S)};
handle_info({membership, {node, leaving, Node}}, S) ->
    forget_node(Node),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{}) ->
    persistent_term:erase(?emqx_ds_builtin_site),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec open_db_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
open_db_trans(DB, CreateOpts) ->
    case mnesia:wread({?META_TAB, DB}) of
        [] ->
            mnesia:write(#?META_TAB{db = DB, db_props = CreateOpts}),
            CreateOpts;
        [#?META_TAB{db_props = Opts}] ->
            case maps:merge(CreateOpts, Opts) of
                Opts ->
                    ok;
                UpdatedOpts ->
                    %% NOTE
                    %% Preserve any new options not yet present in the DB. This is
                    %% most likely because `Opts` is outdated, written by earlier
                    %% EMQX version.
                    mnesia:write(#?META_TAB{db = DB, db_props = UpdatedOpts})
            end,
            Opts
    end.

-spec allocate_shards_trans(emqx_ds:db()) -> [emqx_ds_replication_layer:shard_id()].
allocate_shards_trans(DB) ->
    Opts = #{n_shards := NShards, n_sites := NSites} = db_config_trans(DB),
    case mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write) of
        [] ->
            ok;
        Records ->
            ShardsAllocated = [Shard || #?SHARD_TAB{shard = {_DB, Shard}} <- Records],
            mnesia:abort({shards_already_allocated, ShardsAllocated})
    end,
    Nodes = mnesia:match_object(?NODE_TAB, ?NODE_PAT(), read),
    case length(Nodes) of
        N when N >= NSites ->
            ok;
        _ ->
            mnesia:abort({insufficient_sites_online, NSites, Nodes})
    end,
    Shards = gen_shards(NShards),
    Sites = [S || #?NODE_TAB{site = S} <- Nodes],
    Allocation = compute_allocation(Shards, Sites, Opts),
    lists:map(
        fun({Shard, ReplicaSet}) ->
            Record = #?SHARD_TAB{
                shard = {DB, Shard},
                replica_set = ReplicaSet
            },
            ok = mnesia:write(Record),
            Shard
        end,
        Allocation
    ).

-spec assign_db_sites_trans(emqx_ds:db(), [site()]) -> {ok, [site()]}.
assign_db_sites_trans(DB, Sites) ->
    Opts = db_config_trans(DB),
    case [S || S <- Sites, mnesia:read(?NODE_TAB, S, read) == []] of
        [] when length(Sites) == 0 ->
            mnesia:abort({too_few_sites, Sites});
        [] ->
            ok;
        NonexistentSites ->
            mnesia:abort({nonexistent_sites, NonexistentSites})
    end,
    %% TODO
    %% Optimize reallocation. The goals are:
    %% 1. Minimize the number of membership transitions.
    %% 2. Ensure that sites are responsible for roughly the same number of shards.
    Shards = db_shards_trans(DB),
    Reallocation = compute_allocation(Shards, Sites, Opts),
    ok = lists:foreach(
        fun({Record, ReplicaSet}) ->
            ok = mnesia:write(Record#?SHARD_TAB{target_set = ReplicaSet})
        end,
        Reallocation
    ),
    {ok, Sites}.

-spec modify_db_sites_trans(emqx_ds:db(), [transition()]) -> {ok, unchanged | [site()]}.
modify_db_sites_trans(DB, Modifications) ->
    Shards = db_shards_trans(DB),
    Sites0 = list_db_target_sites(Shards),
    Sites = lists:foldl(fun apply_transition/2, Sites0, Modifications),
    case Sites of
        Sites0 ->
            {ok, unchanged};
        _Changed ->
            assign_db_sites_trans(DB, Sites)
    end.

claim_transition_trans(DB, Shard, Trans) ->
    ShardRecord =
        case mnesia:read(?SHARD_TAB, {DB, Shard}, read) of
            [Record] ->
                Record;
            [] ->
                mnesia:abort({nonexistent_shard, {DB, Shard}})
        end,
    case shard_transition_trans(ShardRecord) of
        [#?TRANSITION_TAB{transition = Trans}] ->
            ok;
        [#?TRANSITION_TAB{transition = Conflict}] ->
            mnesia:abort({conflict, Conflict});
        [] ->
            case compute_transitions(ShardRecord) of
                [Trans | _] ->
                    mnesia:write(#?TRANSITION_TAB{shard = {DB, Shard}, transition = Trans});
                Expected ->
                    mnesia:abort({outdated, Expected})
            end
    end.

update_replica_set_trans(DB, Shard, Trans) ->
    case mnesia:read(?SHARD_TAB, {DB, Shard}, write) of
        [Record = #?SHARD_TAB{replica_set = ReplicaSet0, target_set = TargetSet0}] ->
            %% NOTE
            %% It's possible to complete a transition that's no longer planned. We
            %% should anticipate that we may stray _away_ from the target set.
            TargetSet1 = emqx_maybe:define(TargetSet0, ReplicaSet0),
            ReplicaSet = apply_transition(Trans, ReplicaSet0),
            case lists:usort(TargetSet1) of
                ReplicaSet ->
                    TargetSet = undefined;
                TS ->
                    TargetSet = TS
            end,
            %% NOTE: Not enforcing existence on that level, makes little sense.
            mnesia:delete_object(#?TRANSITION_TAB{shard = {DB, Shard}, transition = Trans}),
            mnesia:write(Record#?SHARD_TAB{replica_set = ReplicaSet, target_set = TargetSet});
        [] ->
            mnesia:abort({nonexistent_shard, {DB, Shard}})
    end.

-spec update_db_config_trans(emqx_ds:db(), emqx_ds_replication_layer:builtin_db_opts()) ->
    emqx_ds_replication_layer:builtin_db_opts().
update_db_config_trans(DB, UpdateOpts) ->
    Opts = db_config_trans(DB, write),
    %% Since this is an update and not a reopen,
    %% we should keep the shard number and replication factor
    %% and not create a new shard server
    ChangeableOpts = maps:without([n_shards, n_sites, replication_factor], UpdateOpts),
    EffectiveOpts = maps:merge(Opts, ChangeableOpts),
    ok = mnesia:write(#?META_TAB{
        db = DB,
        db_props = EffectiveOpts
    }),
    EffectiveOpts.

-spec db_config_trans(emqx_ds:db()) -> emqx_ds_replication_layer:builtin_db_opts().
db_config_trans(DB) ->
    db_config_trans(DB, read).

db_config_trans(DB, LockType) ->
    case mnesia:read(?META_TAB, DB, LockType) of
        [#?META_TAB{db_props = Config}] ->
            Config;
        [] ->
            mnesia:abort({nonexistent_db, DB})
    end.

db_shards_trans(DB) ->
    mnesia:match_object(?SHARD_TAB, ?SHARD_PAT({DB, '_'}), write).

shard_transition_trans(#?SHARD_TAB{shard = DBShard}) ->
    mnesia:read(?TRANSITION_TAB, DBShard, write).

-spec drop_db_trans(emqx_ds:db()) -> ok.
drop_db_trans(DB) ->
    mnesia:delete({?META_TAB, DB}),
    [mnesia:delete({?SHARD_TAB, Shard}) || Shard <- shards(DB)],
    ok.

-spec claim_site_trans(site(), node()) -> ok.
claim_site_trans(Site, Node) ->
    case node_sites(Node) of
        [] ->
            mnesia:write(#?NODE_TAB{site = Site, node = Node});
        [#?NODE_TAB{site = Site}] ->
            ok;
        Records ->
            ExistingSites = [S || #?NODE_TAB{site = S} <- Records],
            mnesia:abort({conflicting_node_site, ExistingSites})
    end.

-spec forget_site_trans(_Record :: tuple()) -> ok.
forget_site_trans(Record = #?NODE_TAB{site = Site}) ->
    %% Safeguards.
    DBs = mnesia:all_keys(?META_TAB),
    %% 1. Compute which DBs has this site as a replica for any of the shards.
    SiteDBs = lists:usort([DB || DB <- DBs, S <- list_db_sites(db_shards_trans(DB)), S == Site]),
    %% 2. Compute which DBs has this site in a membership transition, for any of the shards.
    SiteTargetDBs = lists:usort([
        DB
     || DB <- DBs,
        ShardRecord <- db_shards_trans(DB),
        {_, S} <- compute_transitions(ShardRecord, shard_transition_trans(ShardRecord)),
        S == Site
    ]),
    case SiteDBs of
        [] when SiteTargetDBs == [] ->
            Safeguard = ok;
        [_ | _] ->
            Safeguard = {member_of_replica_sets, SiteDBs};
        [] ->
            Safeguard = {member_of_target_sets, SiteTargetDBs}
    end,
    case Safeguard of
        ok ->
            mnesia:delete_object(?NODE_TAB, Record, write);
        _Otherwise ->
            mnesia:abort(Safeguard)
    end.

node_sites(Node) ->
    mnesia:dirty_match_object(?NODE_TAB, ?NODE_PAT(Node)).

all_nodes() ->
    mnesia:dirty_match_object(?NODE_TAB, ?NODE_PAT()).

all_shards() ->
    mnesia:dirty_match_object(?SHARD_TAB, ?SHARD_PAT('_')).

all_transitions() ->
    mnesia:dirty_match_object(?TRANSITION_TAB, ?TRANSITION_PAT('_')).

%%================================================================================
%% Internal functions
%%================================================================================

ensure_tables() ->
    ok = mria:create_table(?META_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?META_TAB},
        {attributes, record_info(fields, ?META_TAB)}
    ]),
    ok = mria:create_table(?NODE_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?NODE_TAB},
        {attributes, record_info(fields, ?NODE_TAB)}
    ]),
    ok = mria:create_table(?SHARD_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?SHARD_TAB},
        {attributes, record_info(fields, ?SHARD_TAB)}
    ]),
    ok = mria:create_table(?TRANSITION_TAB, [
        {rlog_shard, ?RLOG_SHARD},
        {type, bag},
        {storage, disc_copies},
        {record_name, ?TRANSITION_TAB},
        {attributes, record_info(fields, ?TRANSITION_TAB)}
    ]),
    ok = mria:wait_for_tables([?META_TAB, ?NODE_TAB, ?SHARD_TAB]).

run_migrations() ->
    run_migrations(emqx_release:version()).

run_migrations(_Version = "5.8." ++ _) ->
    run_migrations_e58().

ensure_site() ->
    Filename = filename:join(emqx_ds_storage_layer:base_dir(), "emqx_ds_builtin_site.eterm"),
    case file_read_term(Filename) of
        {ok, Entry} ->
            Site = migrate_site_id(Entry);
        {error, Error} when Error =:= enoent; Error =:= empty ->
            Site = binary:encode_hex(crypto:strong_rand_bytes(8)),
            logger:notice("Creating a new site with ID=~s", [Site]),
            ok = filelib:ensure_dir(Filename),
            {ok, FD} = file:open(Filename, [write]),
            io:format(FD, "~p.", [Site]),
            file:close(FD)
    end,
    case transaction(fun ?MODULE:claim_site_trans/2, [Site, node()]) of
        ok ->
            persistent_term:put(?emqx_ds_builtin_site, Site);
        {error, Reason} ->
            logger:error("Attempt to claim site with ID=~s failed: ~p", [Site, Reason])
    end.

file_read_term(Filename) ->
    %% NOTE
    %% This mess is needed because `file:consult/1` trips over binaries encoded as
    %% latin1, which 5.4.0 code could have produced with `io:format(FD, "~p.", [Site])`.
    maybe
        {ok, FD} ?= file:open(Filename, [read, {encoding, latin1}]),
        {ok, Term, _} ?= io:read(FD, '', _Line = 1),
        ok = file:close(FD),
        {ok, Term}
    else
        {error, Reason} ->
            {error, Reason};
        {error, Reason, _} ->
            {error, Reason};
        {eof, _} ->
            {error, empty}
    end.

migrate_site_id(Site) ->
    case re:run(Site, "^[0-9A-F]+$") of
        {match, _} ->
            Site;
        nomatch ->
            binary:encode_hex(Site)
    end.

forget_node(Node) ->
    Sites = node_sites(Node),
    Result = transaction(fun lists:map/2, [fun ?MODULE:forget_site_trans/1, Sites]),
    case Result of
        Ok when is_list(Ok) ->
            ok;
        {error, Reason} ->
            logger:error("Failed to forget leaving node ~p: ~p", [Node, Reason])
    end.

%% @doc Returns sorted list of sites shards are replicated across.
-spec list_db_sites([_Shard]) -> [site()].
list_db_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_sites/1, Shards).

-spec list_db_target_sites([_Shard]) -> [site()].
list_db_target_sites(Shards) ->
    flatmap_sorted_set(fun get_shard_target_sites/1, Shards).

-spec get_shard_sites(_Shard) -> [site()].
get_shard_sites(#?SHARD_TAB{replica_set = ReplicaSet}) ->
    ReplicaSet.

-spec get_shard_target_sites(_Shard) -> [site()].
get_shard_target_sites(#?SHARD_TAB{target_set = Sites}) when is_list(Sites) ->
    Sites;
get_shard_target_sites(#?SHARD_TAB{target_set = undefined} = Shard) ->
    get_shard_sites(Shard).

-spec compute_allocation([Shard], [Site], emqx_ds_replication_layer:builtin_db_opts()) ->
    [{Shard, [Site, ...]}].
compute_allocation(Shards, Sites, Opts) ->
    NSites = length(Sites),
    ReplicationFactor = maps:get(replication_factor, Opts),
    NReplicas = min(NSites, ReplicationFactor),
    ShardsSorted = lists:sort(Shards),
    SitesSorted = lists:sort(Sites),
    {Allocation, _} = lists:mapfoldl(
        fun(Shard, SSites) ->
            {ReplicaSet, _} = emqx_utils_stream:consume(NReplicas, SSites),
            {_, SRest} = emqx_utils_stream:consume(1, SSites),
            {{Shard, ReplicaSet}, SRest}
        end,
        emqx_utils_stream:repeat(emqx_utils_stream:list(SitesSorted)),
        ShardsSorted
    ),
    Allocation.

compute_transitions(Shard, []) ->
    compute_transitions(Shard);
compute_transitions(Shard, [#?TRANSITION_TAB{transition = Trans}]) ->
    [Trans | lists:delete(Trans, compute_transitions(Shard))].

compute_transitions(#?SHARD_TAB{target_set = TargetSet, replica_set = ReplicaSet}) ->
    do_compute_transitions(TargetSet, ReplicaSet).

do_compute_transitions(undefined, _ReplicaSet) ->
    [];
do_compute_transitions(TargetSet, ReplicaSet) ->
    Additions = TargetSet -- ReplicaSet,
    Deletions = ReplicaSet -- TargetSet,
    intersperse([{add, S} || S <- Additions], [{del, S} || S <- Deletions]).

%% @doc Apply a transition to a list of sites, preserving sort order.
-spec apply_transition(transition(), [site()]) -> [site()].
apply_transition({add, S}, Sites) ->
    lists:usort([S | Sites]);
apply_transition({del, S}, Sites) ->
    lists:delete(S, Sites).

gen_shards(NShards) ->
    [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)].

transaction(Fun, Args) ->
    case mria:transaction(?RLOG_SHARD, Fun, Args) of
        {atomic, Result} ->
            Result;
        {aborted, Reason} ->
            {error, Reason}
    end.

%%====================================================================

handle_subscribe(Pid, Subject, S = #s{subs = Subs0}) ->
    case maps:is_key(Pid, Subs0) of
        false ->
            MRef = erlang:monitor(process, Pid),
            Subs = Subs0#{Pid => {Subject, MRef}},
            S#s{subs = Subs};
        true ->
            S
    end.

handle_unsubscribe(Pid, S = #s{subs = Subs0}) ->
    case maps:take(Pid, Subs0) of
        {{_Subject, MRef}, Subs} ->
            _ = erlang:demonitor(MRef, [flush]),
            S#s{subs = Subs};
        error ->
            S
    end.

notify_subscribers(EventSubject, Event, #s{subs = Subs}) ->
    maps:foreach(
        fun(Pid, {Subject, _MRef}) ->
            Subject == EventSubject andalso
                erlang:send(Pid, {changed, Event})
        end,
        Subs
    ).

%%====================================================================
%% Migrations / 5.8 Release
%%====================================================================

run_migrations_e58() ->
    _ = migrate_node_table(),
    _ = migrate_shard_table(),
    ok.

migrate_node_table() ->
    Tab = ?NODE_TAB_LEGACY,
    migrate_node_table(Tab, table_info_safe(Tab)).

migrate_node_table(Tab, #{attributes := [_Site, _Node, _Misc]}) ->
    %% Table is present and looks migratable.
    ok = mria:wait_for_tables([Tab]),
    case transaction(fun ?MODULE:migrate_node_table_trans/1, [Tab]) of
        {migrated, [], []} ->
            ok;
        {migrated, Migrated, Dups} ->
            logger:notice("Table '~p' migrated ~p entries", [Tab, length(Migrated)]),
            Dups =/= [] andalso
                logger:warning("Table '~p' duplicated entries, skipped: ~p", [Tab, Dups]),
            {atomic, ok} = mria:clear_table(Tab);
        {error, Reason} ->
            logger:warning("Table '~p' unusable, migration skipped: ~p", [Tab, Reason])
    end;
migrate_node_table(_Tab, undefined) ->
    %% No legacy table exists.
    ok.

migrate_node_table_trans(Tab) ->
    %% NOTE
    %% This table could have been populated when running 5.4.0 release, but the
    %% representation of site IDs has changed in following versions. Legacy site IDs
    %% need to be passed through `migrate_site_id/1`, otherwise expectations of the
    %% existing code of those IDs to be "printable" will be broken.
    %% This should be no-op when running > 5.4.0 releases.
    Migstamp = mk_migstamp(),
    Records = mnesia:match_object(Tab, {Tab, '_', '_', '_'}, read),
    {Migrate, Dups} = unique_node_recs([migrate_node_rec(R) || R <- Records]),
    lists:foreach(
        fun(R) -> mnesia:write(?NODE_TAB, attach_migstamp(Migstamp, R), write) end,
        Migrate
    ),
    {migrated, Migrate, Dups}.

migrate_node_rec({?NODE_TAB_LEGACY, Site, Node, Misc}) ->
    #?NODE_TAB{site = migrate_site_id(Site), node = Node, misc = Misc}.

unique_node_recs(Records) ->
    %% NOTE
    %% Unlikely but possible that a 5.4.0 node could have assigned more than 1 Site ID
    %% to itself, because of occasional inability to read back Site ID with
    %% `file:consult/1. In this case it's impossible to tell in 100% of cases which one
    %% was the most recent, so let's just drop all of such node's records. It will
    %% insert the correct record by itself anyway, once upgraded to the recent release
    %% and restarted.
    Dups = Records -- lists:ukeysort(#?NODE_TAB.node, Records),
    DupNodes = [Node || #?NODE_TAB{node = Node} <- Dups],
    lists:partition(fun(R) -> not lists:member(R#?NODE_TAB.node, DupNodes) end, Records).

migrate_shard_table() ->
    Tab = ?SHARD_TAB_LEGACY,
    migrate_shard_table(Tab, table_info_safe(Tab)).

migrate_shard_table(Tab, #{attributes := [_Shard, _ReplicaSet, _TargetSet, _Misc]}) ->
    %% Table is present and looks migratable.
    ok = mria:wait_for_tables([Tab]),
    case transaction(fun ?MODULE:migrate_shard_table_trans/1, [Tab]) of
        {migrated, []} ->
            ok;
        {migrated, Migrated} ->
            logger:notice("Table '~p' migrated ~p entries", [Tab, length(Migrated)]),
            {atomic, ok} = mria:clear_table(Tab);
        {error, Reason} ->
            logger:warning("Table '~p' unusable, migration skipped: ~p", [Tab, Reason])
    end;
migrate_shard_table(Tab, #{attributes := _Incompatible}) ->
    %% Table is present and is incompatible.
    ok = mria:wait_for_tables([Tab]),
    case mnesia:table_info(Tab, size) of
        0 ->
            ok;
        Size ->
            logger:warning("Table '~p' has ~p legacy entries to be abandoned", [Size, Tab]),
            {atomic, ok} = mria:clear_table(Tab)
    end;
migrate_shard_table(_Tab, undefined) ->
    %% No legacy table exists.
    ok.

migrate_shard_table_trans(Tab) ->
    %% NOTE
    %% This table could have been instantiated with a different schema when running
    %% 5.4.0 release but most likely never populated, so it should be fine to abandon it.
    %% This table could also have been instantiated and populated when running 5.7.0
    %% release with the same schema, so we just have to migrate all the recoards verbatim.
    Migstamp = mk_migstamp(),
    Records = mnesia:match_object(Tab, {Tab, '_', '_', '_', '_'}, read),
    Migrate = [migrate_shard_rec(R) || R <- Records],
    lists:foreach(
        fun(R) -> mnesia:write(?SHARD_TAB, attach_migstamp(Migstamp, R), write) end,
        Migrate
    ),
    {migrated, Migrate}.

migrate_shard_rec({?SHARD_TAB_LEGACY, Shard, ReplicaSet, TargetSet, Misc}) ->
    #?SHARD_TAB{shard = Shard, replica_set = ReplicaSet, target_set = TargetSet, misc = Misc}.

mk_migstamp() ->
    %% NOTE: Piece of information describing when and how records were migrated.
    #{
        at => erlang:system_time(millisecond),
        on => emqx_release:version()
    }.

attach_migstamp(Migstamp, Node = #?NODE_TAB{misc = Misc}) ->
    Node#?NODE_TAB{misc = Misc#{migrated => Migstamp}};
attach_migstamp(Migstamp, Shard = #?SHARD_TAB{misc = Misc}) ->
    Shard#?SHARD_TAB{misc = Misc#{migrated => Migstamp}}.

table_info_safe(Tab) ->
    try mnesia:table_info(Tab, all) of
        Props ->
            maps:from_list(Props)
    catch
        exit:{aborted, {no_exists, Tab, _}} ->
            undefined
    end.

%%====================================================================

%% @doc Intersperse elements of two lists.
%% Example: intersperse([1, 2], [3, 4, 5]) -> [1, 3, 2, 4, 5].
-spec intersperse([X], [Y]) -> [X | Y].
intersperse(L1, []) ->
    L1;
intersperse([], L2) ->
    L2;
intersperse([H1 | T1], L2) ->
    [H1 | intersperse(L2, T1)].

%% @doc Map list into a list of sets and return union, as a sorted list.
-spec flatmap_sorted_set(fun((X) -> [Y]), [X]) -> [Y].
flatmap_sorted_set(Fun, L) ->
    ordsets:to_list(
        lists:foldl(
            fun(X, Acc) -> ordsets:union(ordsets:from_list(Fun(X)), Acc) end,
            ordsets:new(),
            L
        )
    ).
