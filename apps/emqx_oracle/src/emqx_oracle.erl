%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_oracle).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(UNHEALTHY_TARGET_MSG,
    "Oracle table is invalid. Please check if the table exists in Oracle Database."
).

-define(DEFAULT_ROLE, normal).
-define(SYSDBA_ROLE, sysdba).

%%====================================================================
%% Exports
%%====================================================================

%% callbacks for behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

%% callbacks for ecpool
-export([connect/1, prepare_sql_to_conn/3, get_reconnect_callback_signature/1]).

%% Internal exports used to execute code with ecpool worker
-export([
    query/3,
    execute_batch/3,
    do_get_status/1
]).

-export([
    oracle_host_options/0
]).

-define(ORACLE_DEFAULT_PORT, 1521).
-define(SYNC_QUERY_MODE, no_handover).
-define(DEFAULT_POOL_SIZE, 8).
-define(OPT_TIMEOUT, 30000).
-define(MAX_CURSORS, 10).
-define(ORACLE_HOST_OPTIONS, #{
    default_port => ?ORACLE_DEFAULT_PORT
}).

-type prepares() :: #{atom() => binary()}.
-type params_tokens() :: #{atom() => list()}.

-type state() ::
    #{
        pool_name := binary(),
        prepare_sql := prepares(),
        params_tokens := params_tokens(),
        batch_params_tokens := params_tokens()
    }.

resource_type() -> oracle.

% As ecpool is not monitoring the worker's PID when doing a handover_async, the
% request can be lost if worker crashes. Thus, it's better to force requests to
% be sync for now.
callback_mode() -> always_sync.

-spec on_start(binary(), hocon:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        username := User
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_oracle_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),
    ?tp(oracle_bridge_started, #{instance_id => InstId, config => Config}),
    {ok, _} = application:ensure_all_started(ecpool),
    {ok, _} = application:ensure_all_started(jamdb_oracle),
    jamdb_oracle_conn:set_max_cursors_number(?MAX_CURSORS),

    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, oracle_host_options()),
    Sid = maps:get(sid, Config, ""),
    ServiceName =
        case maps:get(service_name, Config, undefined) of
            undefined -> undefined;
            ServiceName0 -> emqx_utils_conv:str(ServiceName0)
        end,
    Role = convert_role_to_integer(maps:get(role, Config, ?DEFAULT_ROLE)),
    Options = [
        {host, Host},
        {port, Port},
        {role, Role},
        {user, emqx_utils_conv:str(User)},
        {password, maps:get(password, Config, "")},
        {sid, emqx_utils_conv:str(Sid)},
        {service_name, ServiceName},
        {pool_size, maps:get(pool_size, Config, ?DEFAULT_POOL_SIZE)},
        {timeout, ?OPT_TIMEOUT},
        {app_name, "EMQX Data To Oracle Database Action"}
    ],
    PoolName = InstId,
    State = #{
        pool_name => PoolName,
        installed_channels => #{}
    },
    case emqx_resource_pool:start(InstId, ?MODULE, Options) of
        ok ->
            {ok, State};
        {error, Reason} ->
            ?tp(
                oracle_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, #{pool_name := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping_oracle_connector",
        connector => InstId
    }),
    ?tp(oracle_bridge_stopped, #{instance_id => InstId}),
    emqx_resource_pool:stop(PoolName).

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        pool_name := PoolName
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    {ok, ChannelState} = create_channel_state(ChannelId, PoolName, ChannelConfig),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

create_channel_state(
    ChannelId,
    PoolName,
    #{parameters := Conf} = _ChannelConfig
) ->
    State0 = parse_prepare_sql(ChannelId, Conf),
    State1 = init_prepare(PoolName, State0),
    {ok, State1}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = _State
) ->
    State = maps:get(ChannelId, Channels),
    case do_check_prepares(ChannelId, PoolName, State) of
        ok ->
            ?status_connected;
        {error, undefined_table} ->
            %% return new state indicating that we are connected but the target table is not created
            {?status_disconnected, {unhealthy_target, ?UNHEALTHY_TARGET_MSG}};
        {error, _Reason} ->
            %% do not log error, it is logged in prepare_sql_to_conn
            ?status_connecting
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_query(InstId, {TypeOrKey, NameOrSQL}, #{pool_name := _PoolName} = State) ->
    on_query(InstId, {TypeOrKey, NameOrSQL, []}, State);
on_query(
    InstId,
    {TypeOrKey, NameOrSQL, Params},
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = _ConnectorState
) ->
    State = maps:get(TypeOrKey, Channels, #{}),
    ?SLOG(debug, #{
        msg => "oracle_connector_received_sql_query",
        connector => InstId,
        type => TypeOrKey,
        sql => NameOrSQL,
        state => State
    }),
    Type = query,
    {NameOrSQL2, Data} = proc_sql_params(TypeOrKey, NameOrSQL, Params, State),
    Res = on_sql_query(InstId, TypeOrKey, PoolName, Type, ?SYNC_QUERY_MODE, NameOrSQL2, Data),
    handle_result(Res).

on_batch_query(
    InstId,
    BatchReq,
    #{
        pool_name := PoolName,
        installed_channels := Channels
    } = ConnectorState
) ->
    case BatchReq of
        [{Key, _} = Request | _] ->
            BinKey = to_bin(Key),
            State = maps:get(BinKey, Channels),
            #{
                params_tokens := Tokens,
                prepare_sql := Sts
            } = State,
            case maps:get(BinKey, Tokens, undefined) of
                undefined ->
                    Log = #{
                        connector => InstId,
                        first_request => Request,
                        state => State,
                        msg => "batch_prepare_not_implemented"
                    },
                    ?SLOG(error, Log),
                    {error, {unrecoverable_error, batch_prepare_not_implemented}};
                TokenList ->
                    {_, Datas} = lists:unzip(BatchReq),
                    Datas2 = [emqx_placeholder:proc_sql(TokenList, Data) || Data <- Datas],
                    St = maps:get(BinKey, Sts),
                    case
                        on_sql_query(
                            InstId, BinKey, PoolName, execute_batch, ?SYNC_QUERY_MODE, St, Datas2
                        )
                    of
                        {ok, Results} ->
                            handle_batch_result(Results, 0);
                        Result ->
                            Result
                    end
            end;
        _ ->
            Log = #{
                connector => InstId,
                request => BatchReq,
                state => ConnectorState,
                msg => "invalid_request"
            },
            ?SLOG(error, Log),
            {error, {unrecoverable_error, invalid_request}}
    end.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{
    params_tokens := ParamsTokens, prepare_sql := PrepareSql
}) ->
    Key = to_bin(TypeOrKey),
    case maps:get(Key, ParamsTokens, undefined) of
        undefined ->
            {SQLOrData, Params};
        Tokens ->
            case maps:get(Key, PrepareSql, undefined) of
                undefined ->
                    {SQLOrData, Params};
                Sql ->
                    {Sql, emqx_placeholder:proc_sql(Tokens, SQLOrData)}
            end
    end.

on_sql_query(InstId, ChannelID, PoolName, Type, ApplyMode, NameOrSQL, Data) ->
    emqx_trace:rendered_action_template(ChannelID, #{
        type => Type,
        apply_mode => ApplyMode,
        name_or_sql => NameOrSQL,
        data => #emqx_trace_format_func_data{function = fun trace_format_data/1, data = Data}
    }),
    case ecpool:pick_and_do(PoolName, {?MODULE, Type, [NameOrSQL, Data]}, ApplyMode) of
        {error, Reason} = Result ->
            ?tp(
                oracle_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "oracle_connector_do_sql_query_failed",
                connector => InstId,
                type => Type,
                sql => NameOrSQL,
                reason => Reason
            }),
            case Reason of
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                _ ->
                    Result
            end;
        Result ->
            ?tp(
                oracle_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

trace_format_data(Data0) ->
    %% In batch request, we get a two level list
    {'$array$', lists:map(fun insert_array_marker_if_list/1, Data0)}.

insert_array_marker_if_list(List) when is_list(List) ->
    {'$array$', List};
insert_array_marker_if_list(Item) ->
    Item.

on_get_status(_InstId, #{pool_name := Pool} = _State) ->
    case emqx_resource_pool:health_check_workers(Pool, fun ?MODULE:do_get_status/1) of
        true ->
            ?status_connected;
        false ->
            ?status_disconnected
    end.

do_get_status(Conn) ->
    ok == element(1, jamdb_oracle:sql_query(Conn, "select 1 from dual")).

do_check_prepares(
    _ChannelId,
    _PoolName,
    #{
        prepare_sql := {error, _Prepares}
    } = _State
) ->
    {error, undefined_table};
do_check_prepares(
    ChannelId,
    PoolName,
    State
) ->
    #{
        prepare_sql := #{ChannelId := SQL},
        params_tokens := #{ChannelId := Tokens}
    } = State,

    % it's already connected. Verify if target table still exists
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    lists:foldl(
        fun
            (WorkerPid, ok) ->
                case ecpool_worker:client(WorkerPid) of
                    {ok, Conn} ->
                        case check_if_table_exists(Conn, SQL, Tokens) of
                            {error, undefined_table} -> {error, undefined_table};
                            _ -> ok
                        end;
                    _ ->
                        ok
                end;
            (_, Acc) ->
                Acc
        end,
        ok,
        Workers
    ).

%% ===================================================================

oracle_host_options() ->
    ?ORACLE_HOST_OPTIONS.

connect(Opts) ->
    jamdb_oracle:start_link(Opts).

sql_query_to_str(SqlQuery) ->
    emqx_utils_conv:str(SqlQuery).

sql_params_to_str(Params) when is_list(Params) ->
    lists:map(
        fun
            (false) -> "0";
            (true) -> "1";
            (null) -> null;
            (Value) -> emqx_utils_conv:str(Value)
        end,
        Params
    ).

query(Conn, SQL, Params) ->
    Ret = jamdb_oracle:sql_query(Conn, {sql_query_to_str(SQL), sql_params_to_str(Params)}),
    ?tp(oracle_query, #{conn => Conn, sql => SQL, params => Params, result => Ret}),
    handle_result(Ret).

execute_batch(Conn, SQL, ParamsList) ->
    ParamsListStr = lists:map(fun sql_params_to_str/1, ParamsList),
    Ret = jamdb_oracle:sql_query(Conn, {batch, sql_query_to_str(SQL), ParamsListStr}),
    ?tp(oracle_batch_query, #{conn => Conn, sql => SQL, params => ParamsList, result => Ret}),
    handle_result(Ret).

parse_prepare_sql(ChannelId, Config) ->
    SQL =
        case maps:get(prepare_statement, Config, undefined) of
            undefined ->
                case maps:get(sql, Config, undefined) of
                    undefined -> #{};
                    Template -> #{ChannelId => Template}
                end;
            Any ->
                Any
        end,
    parse_prepare_sql(maps:to_list(SQL), #{}, #{}).

parse_prepare_sql([{Key, H} | T], Prepares, Tokens) ->
    {PrepareSQL, ParamsTokens} = emqx_placeholder:preproc_sql(H, ':n'),
    parse_prepare_sql(
        T, Prepares#{Key => PrepareSQL}, Tokens#{Key => ParamsTokens}
    );
parse_prepare_sql([], Prepares, Tokens) ->
    #{
        prepare_sql => Prepares,
        params_tokens => Tokens
    }.

init_prepare(PoolName, State = #{prepare_sql := Prepares, params_tokens := TokensMap}) ->
    case prepare_sql(Prepares, PoolName, TokensMap) of
        {ok, Sts} ->
            State#{prepare_sql := Sts};
        Error ->
            LogMeta = #{
                msg => <<"oracle_init_prepare_statement_failed">>, error => Error
            },
            ?SLOG(error, LogMeta),
            %% mark the prepare_sql as failed
            State#{prepare_sql => {error, Prepares}}
    end.

prepare_sql(Prepares, PoolName, TokensMap) when is_map(Prepares) ->
    prepare_sql(maps:to_list(Prepares), PoolName, TokensMap);
prepare_sql(Prepares, PoolName, TokensMap) ->
    case do_prepare_sql(Prepares, PoolName, TokensMap) of
        {ok, _Sts} = Ok ->
            %% prepare for reconnect
            ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Prepares]}),
            Ok;
        Error ->
            Error
    end.

do_prepare_sql(Prepares, PoolName, TokensMap) ->
    do_prepare_sql(ecpool:workers(PoolName), Prepares, PoolName, TokensMap, #{}).

do_prepare_sql([{_Name, Worker} | T], Prepares, PoolName, TokensMap, _LastSts) ->
    {ok, Conn} = ecpool_worker:client(Worker),
    case prepare_sql_to_conn(Conn, Prepares, TokensMap) of
        {ok, Sts} ->
            do_prepare_sql(T, Prepares, PoolName, TokensMap, Sts);
        Error ->
            Error
    end;
do_prepare_sql([], _Prepares, _PoolName, _TokensMap, LastSts) ->
    {ok, LastSts}.

prepare_sql_to_conn(Conn, Prepares, TokensMap) ->
    prepare_sql_to_conn(Conn, Prepares, TokensMap, #{}).

prepare_sql_to_conn(Conn, [], _TokensMap, Statements) when is_pid(Conn) -> {ok, Statements};
prepare_sql_to_conn(Conn, [{Key, SQL} | PrepareList], TokensMap, Statements) when is_pid(Conn) ->
    LogMeta = #{msg => "oracle_prepare_statement", name => Key, prepare_sql => SQL},
    Tokens = maps:get(Key, TokensMap, []),
    ?SLOG(info, LogMeta),
    case check_if_table_exists(Conn, SQL, Tokens) of
        ok ->
            ?SLOG(info, LogMeta#{result => success}),
            prepare_sql_to_conn(Conn, PrepareList, TokensMap, Statements#{Key => SQL});
        {error, undefined_table} = Error ->
            %% Target table is not created
            ?SLOG(error, LogMeta#{result => failed, reason => "table does not exist"}),
            ?tp(oracle_undefined_table, #{}),
            Error;
        Error ->
            Error
    end.

%% this callback accepts the arg list provided to
%% ecpool:add_reconnect_callback(PoolName, {?MODULE, prepare_sql_to_conn, [Templates]})
%% so ecpool_worker can de-duplicate the callbacks based on the signature.
get_reconnect_callback_signature([[{ChannelId, _Template}]]) ->
    ChannelId.

check_if_table_exists(Conn, SQL, Tokens0) ->
    % Discard nested tokens for checking if table exist. As payload here is defined as
    % a single string, it would fail if Token is, for instance, ${payload.msg}, causing
    % bridge probe to fail.
    Tokens = lists:map(
        fun
            ({var, [Token | _DiscardedDeepTokens]}) ->
                {var, [Token]};
            (Token) ->
                Token
        end,
        Tokens0
    ),
    {Event, _Headers} = emqx_rule_events:eventmsg_publish(
        emqx_message:make(<<"t/opic">>, "test query")
    ),
    SqlQuery = "begin " ++ binary_to_list(SQL) ++ "; rollback; end;",
    Params = emqx_placeholder:proc_sql(Tokens, Event),
    case jamdb_oracle:sql_query(Conn, {SqlQuery, Params}) of
        {ok, [{proc_result, 0, _Description}]} ->
            ok;
        {ok, [{proc_result, 942, _Description}]} ->
            %% Target table is not created
            {error, undefined_table};
        {ok, [{proc_result, _, Description}]} ->
            % only the last result is returned, so we need to check on description if it
            % contains the "Table doesn't exist" error as it can not be the last one.
            % (for instance, the ORA-06550 can be the result value when table does not exist)
            ErrorCodes =
                case re:run(Description, <<"(ORA-[0-9]+)">>, [global, {capture, first, binary}]) of
                    {match, OraCodes} -> OraCodes;
                    _ -> []
                end,
            OraMap = maps:from_keys([ErrorCode || [ErrorCode] <- ErrorCodes], true),
            case OraMap of
                _ when is_map_key(<<"ORA-00942">>, OraMap) ->
                    % ORA-00942: table or view does not exist
                    {error, undefined_table};
                _ when is_map_key(<<"ORA-00932">>, OraMap) ->
                    % ORA-00932: inconsistent datatypes
                    % There is a some type inconsistency with table definition but
                    % table does exist. Probably this inconsistency was caused by
                    % token discarding in this test query.
                    ok;
                _ when is_map_key(<<"ORA-01400">>, OraMap) ->
                    % ORA-01400: cannot insert NULL into (string)
                    % There is a some type inconsistency with table definition but
                    % table does exist. Probably this inconsistency was caused by
                    % token discarding in this test query.
                    ok;
                _ when is_map_key(<<"ORA-01013">>, OraMap) ->
                    % ORA-01013: user requested cancel of current operation
                    % This error is returned when the query is canceled by the user.
                    ok;
                _ when is_map_key(<<"ORA-12899">>, OraMap) ->
                    % ORA-12899: value too large for column
                    % This error is returned when the value is too large for the column.
                    ok;
                _ ->
                    {error, Description}
            end;
        Reason ->
            {error, Reason}
    end.

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom).

handle_result({error, {recoverable_error, _Error}} = Res) ->
    Res;
handle_result({error, {unrecoverable_error, _Error}} = Res) ->
    Res;
handle_result({error, disconnected}) ->
    {error, {recoverable_error, disconnected}};
handle_result({error, Error}) ->
    {error, {unrecoverable_error, Error}};
handle_result({error, socket, closed} = Error) ->
    {error, {recoverable_error, Error}};
handle_result({error, Type, Reason}) ->
    {error, {unrecoverable_error, {Type, Reason}}};
handle_result({ok, [{proc_result, RetCode, Reason}]}) when RetCode =/= 0 ->
    {error, {unrecoverable_error, {RetCode, Reason}}};
handle_result(Res) ->
    Res.

handle_batch_result([{affected_rows, RowCount} | Rest], Acc) ->
    handle_batch_result(Rest, Acc + RowCount);
handle_batch_result([{proc_result, RetCode, _Rows} | Rest], Acc) when RetCode =:= 0 ->
    handle_batch_result(Rest, Acc);
handle_batch_result([{proc_result, RetCode, Reason} | _Rest], _Acc) ->
    {error, {unrecoverable_error, {RetCode, Reason}}};
handle_batch_result([], Acc) ->
    {ok, Acc}.

convert_role_to_integer(?SYSDBA_ROLE) ->
    1;
convert_role_to_integer(_) ->
    0.
