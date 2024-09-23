%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_registry).

%% API
-export([
    start_link/0,
    child_spec/0
]).

-export([
    leader_wanted/3,
    start_elector/2
]).

-behaviour(supervisor).
-export([init/1]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        type => supervisor
    }.

-spec leader_wanted(
    emqx_ds_shared_sub_proto:agent(),
    emqx_ds_shared_sub_proto:agent_metadata(),
    emqx_persistent_session_ds:share_topic_filter()
) -> ok.
leader_wanted(Agent, AgentMetadata, ShareTopic) ->
    {ok, Pid} = ensure_elector_started(ShareTopic),
    emqx_ds_shared_sub_proto:agent_connect_leader(Pid, Agent, AgentMetadata, ShareTopic).

-spec ensure_elector_started(emqx_persistent_session_ds:share_topic_filter()) ->
    {ok, pid()}.
ensure_elector_started(ShareTopic) ->
    case start_elector(ShareTopic, _StartTime = emqx_message:timestamp_now()) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} when is_pid(Pid) ->
            {ok, Pid}
    end.

-spec start_elector(emqx_persistent_session_ds:share_topic_filter(), emqx_message:timestamp()) ->
    supervisor:startchild_ret().
start_elector(ShareTopic, StartTime) ->
    supervisor:start_child(?MODULE, #{
        id => ShareTopic,
        start => {emqx_ds_shared_sub_elector, start_link, [ShareTopic, StartTime]},
        restart => temporary,
        type => worker,
        shutdown => 5000
    }).

%%------------------------------------------------------------------------------
%% supervisor behaviour callbacks
%%------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.
