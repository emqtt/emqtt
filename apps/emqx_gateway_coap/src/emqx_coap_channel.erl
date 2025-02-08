%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_coap_channel).

-behaviour(emqx_gateway_channel).

%% API
-export([
    info/1,
    info/2,
    stats/1,
    validator/4,
    metrics_inc/2,
    run_hooks/3,
    send_request/2
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_deliver/2,
    handle_timeout/3,
    terminate/2
]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export_type([channel/0]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/logger.hrl").

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% Connection Info
    conninfo :: emqx_types:conninfo(),
    %% Client Info
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: emqx_coap_session:session() | undefined,
    %% Keepalive
    keepalive :: emqx_keepalive:keepalive() | undefined,
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %% Connection mode
    connection_required :: boolean(),
    %% Connection State
    conn_state :: conn_state(),
    %% Session token to identity this connection
    token :: binary() | undefined
}).

-type channel() :: #channel{}.

-type conn_state() :: idle | connecting | connected | disconnected.

-type reply() ::
    {outgoing, coap_message()}
    | {outgoing, [coap_message()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(TOKEN_MAXIMUM, 4294967295).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session]).

-define(DEF_IDLE_SECONDS, 30).
-define(RAND_CLIENTID_BYTES, 16).

-import(emqx_coap_medium, [reply/2, reply/3, reply/4, iter/3, iter/4]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    emqx_utils:maybe_apply(fun emqx_coap_session:info/1, Session);
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{session = Session}) ->
    emqx_coap_session:stats(Session).

-spec init(map(), map()) -> channel().
init(
    ConnInfo = #{
        peername := {PeerHost, _} = PeerName,
        sockname := {_, SockPort}
    },
    #{ctx := Ctx} = Config
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Config, <<>>),
    EnableAuthn = maps:get(enable_authn, Config, true),
    ListenerId =
        case maps:get(listener, Config, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    ClientInfo = set_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => 'coap',
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => emqx_utils:rand_id(?RAND_CLIENTID_BYTES),
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            enable_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),
    Heartbeat = maps:get(heartbeat, Config, ?DEF_IDLE_SECONDS),
    #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        timers = #{},
        session = emqx_coap_session:new(),
        keepalive = emqx_keepalive:init(Heartbeat),
        connection_required = maps:get(connection_required, Config, false),
        conn_state = idle
    }.

validator(Action, Topic, Ctx, ClientInfo) ->
    emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, Topic).

-spec send_request(pid(), coap_message()) -> any().
send_request(Channel, Request) ->
    gen_server:send_request(Channel, {?FUNCTION_NAME, Request}).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(coap_message() | {frame_error, any()}, channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(Msg, Channel0) ->
    Channel = ensure_keepalive_timer(Channel0),
    case emqx_coap_message:is_request(Msg) of
        true ->
            check_auth_state(Msg, Channel);
        _ ->
            call_session(handle_response, Msg, Channel)
    end.

handle_frame_error(Reason, Channel) ->
    {shutdown, Reason, Channel}.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------
handle_deliver(
    Delivers,
    #channel{
        session = Session,
        ctx = Ctx
    } = Channel
) ->
    handle_result(emqx_coap_session:deliver(Delivers, Ctx, Session), Channel).

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(_, {keepalive, NewVal}, #channel{keepalive = KeepAlive} = Channel) ->
    case emqx_keepalive:check(NewVal, KeepAlive) of
        {ok, _} ->
            Channel2 = ensure_keepalive_timer(fun make_timer/4, Channel),
            {ok, Channel2};
        {error, timeout} ->
            {shutdown, timeout, ensure_disconnected(keepalive_timeout, Channel)}
    end;
handle_timeout(_, {transport, Msg}, Channel) ->
    call_session(timeout, Msg, Channel);
handle_timeout(_, disconnect, Channel) ->
    {shutdown, normal, Channel};
handle_timeout(_, connection_expire, Channel) ->
    {shutdown, expired, Channel};
handle_timeout(_, _, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), coap_message(), channel()}.
handle_call({send_request, Msg}, From, Channel) ->
    Result = call_session(handle_out, {{send_request, From}, Msg}, Channel),
    erlang:setelement(1, Result, noreply);
handle_call(
    {subscribe, Topic, SubOpts},
    _From,
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{
                    clientid := ClientId,
                    mountpoint := Mountpoint
                },
        session = Session
    }
) ->
    Token = maps:get(
        token,
        maps:get(sub_props, SubOpts, #{}),
        <<>>
    ),
    NSubOpts = maps:merge(
        emqx_gateway_utils:default_subopts(),
        SubOpts
    ),
    MountedTopic = emqx_mountpoint:mount(Mountpoint, Topic),
    _ = emqx_broker:subscribe(MountedTopic, ClientId, NSubOpts),

    _ = run_hooks(
        Ctx,
        'session.subscribed',
        [ClientInfo, MountedTopic, NSubOpts]
    ),
    %% modify session state
    SubReq = #{topic => Topic, token => Token, subopts => NSubOpts},
    TempMsg = #coap_message{type = non},
    %% FIXME: The subopts is not used for emqx_coap_session
    Result = emqx_coap_session:process_subscribe(
        SubReq, TempMsg, #{}, Session
    ),
    NSession = maps:get(session, Result),
    {reply, {ok, {MountedTopic, NSubOpts}}, [{event, updated}], Channel#channel{session = NSession}};
handle_call(
    {unsubscribe, Topic},
    _From,
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint},
        session = Session
    }
) ->
    MountedTopic = emqx_mountpoint:mount(Mountpoint, Topic),
    ok = emqx_broker:unsubscribe(MountedTopic),
    _ = run_hooks(
        Ctx,
        'session.unsubscribed',
        [ClientInfo, MountedTopic, #{}]
    ),

    %% modify session state
    UnSubReq = Topic,
    TempMsg = #coap_message{type = non},
    Result = emqx_coap_session:process_subscribe(
        UnSubReq, TempMsg, #{}, Session
    ),
    NSession = maps:get(session, Result),
    {reply, ok, [{event, updated}], Channel#channel{session = NSession}};
handle_call(subscriptions, _From, Channel = #channel{session = Session}) ->
    Subs = emqx_coap_session:info(subscriptions, Session),
    {reply, {ok, maps:to_list(Subs)}, Channel};
handle_call({check_token, ReqToken}, _From, Channel = #channel{token = Token}) ->
    {reply, ReqToken == Token, Channel};
handle_call(kick, _From, Channel) ->
    NChannel = ensure_disconnected(kicked, Channel),
    shutdown_and_reply(kicked, ok, NChannel);
handle_call(discard, _From, Channel) ->
    shutdown_and_reply(discarded, ok, Channel);
handle_call(Req, _From, Channel) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, Channel}.

%%--------------------------------------------------------------------
%% Handle Cast
%%--------------------------------------------------------------------

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(close, Channel) ->
    ?SLOG(info, #{msg => "close_connection"}),
    shutdown(normal, Channel);
handle_cast(inc_recv_pkt, Channel) ->
    _ = emqx_pd:inc_counter(recv_pkt, 1),
    {ok, Channel};
handle_cast(Req, Channel) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_info({subscribe, _AutoSubs}, Channel) ->
    {ok, Channel};
handle_info(Info, Channel) ->
    ?SLOG(warning, #{msg => "unexpected_info", info => Info}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------
terminate(Reason, #channel{
    clientinfo = ClientInfo,
    ctx = Ctx,
    session = Session
}) ->
    run_hooks(Ctx, 'session.terminated', [ClientInfo, Reason, Session]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
set_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
set_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

ensure_timer(Name, Time, Msg, #channel{timers = Timers} = Channel) ->
    case maps:get(Name, Timers, undefined) of
        undefined ->
            make_timer(Name, Time, Msg, Channel);
        _ ->
            Channel
    end.

make_timer(Name, Time, Msg, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

ensure_keepalive_timer(Channel) ->
    ensure_keepalive_timer(fun ensure_timer/4, Channel).

ensure_keepalive_timer(Fun, #channel{keepalive = KeepAlive} = Channel) ->
    Heartbeat = emqx_keepalive:info(check_interval, KeepAlive),
    Fun(keepalive, Heartbeat, keepalive, Channel).

check_auth_state(Msg, #channel{connection_required = false} = Channel) ->
    call_session(handle_request, Msg, Channel);
check_auth_state(Msg, #channel{connection_required = true} = Channel) ->
    case is_create_connection_request(Msg) of
        true ->
            call_session(handle_request, Msg, Channel);
        false ->
            URIQuery = emqx_coap_message:extract_uri_query(Msg),
            case maps:get(<<"token">>, URIQuery, undefined) of
                undefined ->
                    ?SLOG(debug, #{msg => "token_required_in_conn_mode", message => Msg});
                _ ->
                    check_token(Msg, Channel)
            end
    end.

is_create_connection_request(Msg = #coap_message{method = Method}) when
    is_atom(Method) andalso Method =/= undefined
->
    URIPath = emqx_coap_message:get_option(uri_path, Msg, []),
    case URIPath of
        [<<"mqtt">>, <<"connection">>] when Method == post ->
            true;
        _ ->
            false
    end;
is_create_connection_request(_Msg) ->
    false.

is_delete_connection_request(Msg = #coap_message{method = Method}) when
    is_atom(Method) andalso Method =/= undefined
->
    URIPath = emqx_coap_message:get_option(uri_path, Msg, []),
    case URIPath of
        [<<"mqtt">>, <<"connection">>] when Method == delete ->
            true;
        _ ->
            false
    end;
is_delete_connection_request(_Msg) ->
    false.

check_token(
    Msg,
    #channel{
        token = Token,
        clientinfo = ClientInfo
    } = Channel
) ->
    #{clientid := ClientId} = ClientInfo,
    case emqx_coap_message:extract_uri_query(Msg) of
        #{
            <<"clientid">> := ClientId,
            <<"token">> := Token
        } ->
            call_session(handle_request, Msg, Channel);
        _ ->
            %% This channel is create by this DELETE command, so here can safely close this channel
            case Token =:= undefined andalso is_delete_connection_request(Msg) of
                true ->
                    Reply = emqx_coap_message:piggyback({ok, deleted}, Msg),
                    {shutdown, normal, Reply, Channel};
                false ->
                    ErrMsg = <<"Missing token or clientid in connection mode">>,
                    Reply = emqx_coap_message:piggyback({error, bad_request}, ErrMsg, Msg),
                    {ok, {outgoing, Reply}, Channel}
            end
    end.

run_conn_hooks(
    Input,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Input, Channel}
    end.

enrich_conninfo(
    {Queries, _Msg},
    Channel = #channel{
        keepalive = KeepAlive,
        conninfo = ConnInfo
    }
) ->
    case Queries of
        #{<<"clientid">> := ClientId} ->
            %% in milliseconds
            IntervalMs = emqx_keepalive:info(check_interval, KeepAlive),
            %% in seconds
            InternalS = floor(IntervalMs / 1000),
            NConnInfo = ConnInfo#{
                clientid => ClientId,
                proto_name => <<"CoAP">>,
                proto_ver => <<"1">>,
                clean_start => true,
                keepalive => InternalS,
                expiry_interval => 0
            },
            {ok, Channel#channel{conninfo = NConnInfo}};
        _ ->
            {error, "clientid is required", Channel}
    end.

enrich_clientinfo(
    {Queries, Msg},
    Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo0}
) ->
    ClientInfo = ClientInfo0#{
        clientid => maps:get(clientid, ConnInfo),
        username => maps:get(<<"username">>, Queries, undefined),
        password => maps:get(<<"password">>, Queries, undefined)
    },
    {ok, NClientInfo} = fix_mountpoint(Msg, ClientInfo),
    {ok, Channel#channel{clientinfo = NClientInfo}}.

set_log_meta(_Input, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

auth_connect(
    _Input,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                username => Username,
                clientid => ClientId,
                reason => Reason
            }),
            {error, Reason}
    end.

fix_mountpoint(_Packet, #{mountpoint := <<>>} = ClientInfo) ->
    {ok, ClientInfo};
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

process_connect(
    #channel{
        ctx = Ctx,
        session = Session,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    } = Channel,
    Msg,
    Result,
    Iter
) ->
    %% inherit the old session
    SessFun = fun(_, _) -> Session end,
    case
        emqx_gateway_ctx:open_session(
            Ctx,
            true,
            ClientInfo,
            ConnInfo,
            SessFun,
            emqx_coap_session
        )
    of
        {ok, _Sess} ->
            %% FIXME: Token in cluster wide?
            RandVal = rand:uniform(?TOKEN_MAXIMUM),
            Token = erlang:list_to_binary(erlang:integer_to_list(RandVal)),
            NResult = Result#{events => [{event, connected}]},
            iter(
                Iter,
                reply({ok, created}, Token, Msg, NResult),
                Channel#channel{token = Token}
            );
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_open_session",
                clientid => maps:get(clientid, ClientInfo),
                reason => Reason
            }),
            iter(Iter, reply({error, bad_request}, Msg, Result), Channel)
    end.

schedule_connection_expire(Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}) ->
    case emqx_gateway_ctx:connection_expire_interval(Ctx, ClientInfo) of
        undefined ->
            Channel;
        Interval ->
            ensure_timer(connection_expire_timer, Interval, connection_expire, Channel)
    end.

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

metrics_inc(Name, Ctx) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    _ = run_hooks(Ctx, 'client.connack', [NConnInfo, connection_accepted, #{}]),
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    schedule_connection_expire(Channel#channel{conninfo = NConnInfo, conn_state = connected}).

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        conn_state = ConnState
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},

    case ConnState of
        connected ->
            ok = run_hooks(Ctx, 'client.disconnected', [ClientInfo, Reason, NConnInfo]);
        _ ->
            ok
    end,
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown_and_reply(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

%shutdown_and_reply(Reason, Reply, OutPkt, Channel) ->
%    {shutdown, Reason, Reply, OutPkt, Channel}.

%%--------------------------------------------------------------------
%% Call Chain
%%--------------------------------------------------------------------

call_session(Fun, Msg, #channel{session = Session} = Channel) ->
    Result = emqx_coap_session:Fun(Msg, Session),
    handle_result(Result, Channel).

handle_result(Result, Channel) ->
    iter(
        [
            session,
            fun process_session/4,
            proto,
            fun process_protocol/4,
            reply,
            fun process_reply/4,
            out,
            fun process_out/4,
            fun process_nothing/3
        ],
        Result,
        Channel
    ).

call_handler(
    request,
    Msg,
    Result,
    #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    } = Channel,
    Iter
) ->
    HandlerResult =
        case emqx_coap_message:get_option(uri_path, Msg) of
            [<<"ps">> | RestPath] ->
                emqx_coap_pubsub_handler:handle_request(RestPath, Msg, Ctx, ClientInfo);
            [<<"mqtt">> | RestPath] ->
                emqx_coap_mqtt_handler:handle_request(RestPath, Msg, Ctx, ClientInfo);
            _ ->
                reply({error, bad_request}, Msg)
        end,
    iter(
        [
            connection,
            fun process_connection/4,
            subscribe,
            fun process_subscribe/4
            | Iter
        ],
        maps:merge(Result, HandlerResult),
        Channel
    );
call_handler(response, {{send_request, From}, Response}, Result, Channel, Iter) ->
    gen_server:reply(From, Response),
    iter(Iter, Result, Channel);
call_handler(_, _, Result, Channel, Iter) ->
    iter(Iter, Result, Channel).

process_session(Session, Result, Channel, Iter) ->
    iter(Iter, Result, Channel#channel{session = Session}).

process_protocol({Type, Msg}, Result, Channel, Iter) ->
    call_handler(Type, Msg, Result, Channel, Iter).

%% leaf node
process_out(Outs, Result, Channel, _) ->
    Outs2 = lists:reverse(Outs),
    Outs3 =
        case maps:get(reply, Result, undefined) of
            undefined ->
                Outs2;
            Reply ->
                [Reply | Outs2]
        end,
    Events = maps:get(events, Result, []),
    {ok, [{outgoing, Outs3}] ++ Events, Channel}.

%% leaf node
process_nothing(_, _, Channel) ->
    {ok, Channel}.

process_connection(
    {open, Req},
    Result,
    Channel = #channel{conn_state = idle},
    Iter
) ->
    Queries = emqx_coap_message:extract_uri_query(Req),
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2,
                fun auth_connect/2
            ],
            {Queries, Req},
            Channel
        )
    of
        {ok, _Input, NChannel} ->
            process_connect(ensure_connected(NChannel), Req, Result, Iter);
        {error, ReasonCode, NChannel} ->
            ErrMsg = io_lib:format("Login Failed: ~ts", [ReasonCode]),
            Payload = iolist_to_binary(ErrMsg),
            Reply = emqx_coap_message:piggyback({error, bad_request}, Payload, Req),
            process_shutdown(Reply, Result, NChannel, Iter)
    end;
process_connection(
    {open, Req},
    Result,
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = #{clientid := ClientId}
    },
    Iter
) when
    ConnState == connected
->
    %% TODO should take over the session here
    Queries = emqx_coap_message:extract_uri_query(Req),
    ErrMsg0 =
        case Queries of
            #{<<"clientid">> := ClientId} ->
                "client has connected";
            #{<<"clientid">> := ReqClientId} ->
                ["channel has registered by: ", ReqClientId];
            _ ->
                "invalid queries"
        end,
    ErrMsg = io_lib:format("Bad Request: ~ts", [ErrMsg0]),
    Payload = iolist_to_binary(ErrMsg),
    iter(
        Iter,
        reply({error, bad_request}, Payload, Req, Result),
        Channel
    );
process_connection({close, Msg}, _, Channel, _) ->
    Queries = emqx_coap_message:extract_uri_query(Msg),
    case maps:get(<<"clientid">>, Queries, undefined) of
        undefined ->
            ok;
        ClientId ->
            %% XXX: A cluster-level connection shutdown needs to be performed here.
            %%
            %% due to the possibility that the current close request may be
            %% from a CoAP client from another IP + Port tuple
            emqx_gateway_cm:cast(coap, ClientId, close)
    end,
    Reply = emqx_coap_message:piggyback({ok, deleted}, Msg),
    NChannel = ensure_disconnected(normal, Channel),
    {shutdown, normal, Reply, NChannel}.

process_subscribe({Sub, Msg}, Result, #channel{session = Session} = Channel, Iter) ->
    Result2 = emqx_coap_session:process_subscribe(Sub, Msg, Result, Session),
    iter([session, fun process_session/4 | Iter], Result2, Channel).

%% leaf node
process_reply(Reply, Result, #channel{session = Session} = Channel, _) ->
    Session2 = emqx_coap_session:set_reply(Reply, Session),
    Outs = maps:get(out, Result, []),
    Outs2 = lists:reverse(Outs),
    Events = maps:get(events, Result, []),
    {ok, [{outgoing, [Reply | Outs2]}] ++ Events, Channel#channel{session = Session2}}.

%% leaf node
process_shutdown(Reply, _Result, Channel, _) ->
    %    Outs = maps:get(out, Result, []),
    %   Outs2 = lists:reverse(Outs),
    %  Events = maps:get(events, Result, []),
    {shutdown, normal, Reply, Channel}.
