%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Utils funcs for emqx-gateway
-module(emqx_gateway_utils).

-include("emqx_gateway.hrl").
-include_lib("emqx/include/logger.hrl").

-define(GATEWAYS, [
    emqx_gateway_coap,
    emqx_gateway_exproto,
    emqx_gateway_gbt32960,
    emqx_gateway_jt808,
    emqx_gateway_lwm2m,
    emqx_gateway_mqttsn,
    emqx_gateway_ocpp,
    emqx_gateway_stomp
]).

-export([
    childspec/2,
    childspec/3,
    childspec/4,
    supervisor_ret/1,
    find_sup_child/2
]).

-export([
    start_listeners/4,
    start_listener/4,
    stop_listeners/2,
    stop_listener/2
]).

-export([
    apply/2,
    parse_listenon/1,
    unix_ts_to_rfc3339/2,
    listener_id/3,
    parse_listener_id/1,
    is_running/2,
    global_chain/1,
    listener_chain/3,
    find_gateway_definitions/0,
    find_gateway_definition/1,
    plus_max_connections/2,
    random_clientid/1,
    check_gateway_edition/1
]).

-export([stringfy/1]).

-export([normalize_config/1]).

%% Common Envs
-export([
    active_n/1,
    ratelimit/1,
    frame_options/1,
    init_gc_state/1,
    stats_timer/1,
    idle_timeout/1,
    oom_policy/1
]).

-export([
    default_tcp_options/0,
    default_udp_options/0,
    default_subopts/0
]).

-import(emqx_listeners, [esockd_access_rules/1]).

-define(ACTIVE_N, 10).
-define(DEFAULT_IDLE_TIMEOUT, 30000).
-define(DEFAULT_GC_OPTS, #{count => 1000, bytes => 1024 * 1024}).
-define(DEFAULT_OOM_POLICY, #{
    max_heap_size => 4194304,
    max_mailbox_size => 32000
}).

-define(IS_ESOCKD_LISTENER(T),
    T == tcp orelse T == ssl orelse T == udp orelse T == dtls
).
-define(IS_COWBOY_LISTENER(T), T == ws orelse T == wss).

-elvis([{elvis_style, god_modules, disable}]).

-spec childspec(worker | supervisor, Mod :: atom()) ->
    supervisor:child_spec().
childspec(Type, Mod) ->
    childspec(Mod, Type, Mod, []).

-spec childspec(worker | supervisor, Mod :: atom(), Args :: list()) ->
    supervisor:child_spec().
childspec(Type, Mod, Args) ->
    childspec(Mod, Type, Mod, Args).

-spec childspec(atom(), worker | supervisor, Mod :: atom(), Args :: list()) ->
    supervisor:child_spec().
childspec(Id, Type, Mod, Args) ->
    #{
        id => Id,
        start => {Mod, start_link, Args},
        type => Type
    }.

-spec supervisor_ret(supervisor:startchild_ret()) ->
    {ok, pid()}
    | {error, supervisor:startchild_err()}.
supervisor_ret({ok, Pid, _Info}) ->
    {ok, Pid};
supervisor_ret({error, {Reason, Child}}) ->
    case element(1, Child) == child of
        true -> {error, Reason};
        _ -> {error, {Reason, Child}}
    end;
supervisor_ret(Ret) ->
    Ret.

-spec find_sup_child(Sup :: pid() | atom(), ChildId :: term()) ->
    false
    | {ok, pid()}.
find_sup_child(Sup, ChildId) ->
    case lists:keyfind(ChildId, 1, supervisor:which_children(Sup)) of
        false -> false;
        {_Id, Pid, _Type, _Mods} -> {ok, Pid}
    end.

%% @doc start listeners. close all listeners if someone failed
-spec start_listeners(
    Listeners :: list(),
    GwName :: atom(),
    Ctx :: map(),
    ModCfg
) ->
    {ok, [pid()]}
    | {error, term()}
when
    ModCfg :: #{
        frame_mod := atom(),
        chann_mod := atom(),
        connection_mod => atom(),
        esockd_proxy_opts => map()
    }.
start_listeners(Listeners, GwName, Ctx, ModCfg) ->
    start_listeners(Listeners, GwName, Ctx, ModCfg, []).

start_listeners([], _, _, _, Acc) ->
    {ok, lists:map(fun({listener, {_, Pid}}) -> Pid end, Acc)};
start_listeners([L | Ls], GwName, Ctx, ModCfg, Acc) ->
    case start_listener(GwName, Ctx, L, ModCfg) of
        {ok, {ListenerId, ListenOn, Pid}} ->
            NAcc = Acc ++ [{listener, {{ListenerId, ListenOn}, Pid}}],
            start_listeners(Ls, GwName, Ctx, ModCfg, NAcc);
        {error, Reason} ->
            lists:foreach(
                fun({listener, {{ListenerId, ListenOn}, _}}) ->
                    esockd:close({ListenerId, ListenOn})
                end,
                Acc
            ),
            {error, {Reason, L}}
    end.

-spec start_listener(
    GwName :: atom(),
    Ctx :: emqx_gateway_ctx:context(),
    Listener :: tuple(),
    ModCfg :: map()
) ->
    {ok, {ListenerId :: atom(), esockd:listen_on(), pid()}}
    | {error, term()}.
start_listener(
    GwName,
    Ctx,
    {Type, LisName, ListenOn, Cfg},
    ModCfg
) ->
    ListenOnStr = emqx_listeners:format_bind(ListenOn),
    ListenerId = emqx_gateway_utils:listener_id(GwName, Type, LisName),

    case
        start_listener(
            GwName,
            Ctx,
            Type,
            LisName,
            ListenOn,
            Cfg,
            ModCfg
        )
    of
        {ok, Pid} ->
            console_print(
                "Gateway ~ts:~ts:~ts on ~ts started.~n",
                [GwName, Type, LisName, ListenOnStr]
            ),
            {ok, {ListenerId, ListenOn, Pid}};
        {error, Reason} ->
            ?ELOG(
                "Gateway failed to start ~ts:~ts:~ts on ~ts: ~0p~n",
                [GwName, Type, LisName, ListenOnStr, Reason]
            ),
            emqx_gateway_utils:supervisor_ret({error, Reason})
    end.

start_listener(GwName, Ctx, Type, LisName, ListenOn, Confs, ModCfg) when
    ?IS_ESOCKD_LISTENER(Type)
->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    SocketOpts = merge_default(Type, esockd_opts(Type, Confs)),
    HighLevelCfgs0 = filter_out_low_level_opts(Type, Confs),
    HighLevelCfgs = maps:merge(
        HighLevelCfgs0,
        ModCfg#{
            ctx => Ctx,
            listener => {GwName, Type, LisName}
        }
    ),
    ConnMod = maps:get(connection_mod, ModCfg, emqx_gateway_conn),
    MFA = {ConnMod, start_link, [HighLevelCfgs]},
    do_start_listener(Type, Name, ListenOn, SocketOpts, MFA);
start_listener(GwName, Ctx, Type, LisName, ListenOn, Confs, ModCfg) when
    ?IS_COWBOY_LISTENER(Type)
->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    RanchOpts = ranch_opts(Type, ListenOn, Confs),
    HighLevelCfgs0 = filter_out_low_level_opts(Type, Confs),
    HighLevelCfgs = maps:merge(
        HighLevelCfgs0,
        ModCfg#{
            ctx => Ctx,
            listener => {GwName, Type, LisName}
        }
    ),
    WsOpts = ws_opts(Confs, HighLevelCfgs),
    case Type of
        ws -> cowboy:start_clear(Name, RanchOpts, WsOpts);
        wss -> cowboy:start_tls(Name, RanchOpts, WsOpts)
    end.

filter_out_low_level_opts(Type, RawCfg = #{gw_conf := Conf0}) when ?IS_ESOCKD_LISTENER(Type) ->
    EsockdKeys = [
        gw_conf,
        bind,
        acceptors,
        max_connections,
        max_conn_rate,
        tcp_options,
        ssl_options,
        udp_options,
        dtls_options
    ],
    Conf1 = maps:without(EsockdKeys, RawCfg),
    maps:merge(Conf0, Conf1);
filter_out_low_level_opts(Type, RawCfg = #{gw_conf := Conf0}) when ?IS_COWBOY_LISTENER(Type) ->
    CowboyKeys = [
        gw_conf,
        bind,
        acceptors,
        max_connections,
        max_conn_rate,
        tcp_options,
        ssl_options,
        udp_options,
        dtls_options
    ],
    Conf1 = maps:without(CowboyKeys, RawCfg),
    maps:merge(Conf0, Conf1).

merge_default(Udp, Options) ->
    {Key, Default} =
        case Udp of
            udp ->
                {udp_options, default_udp_options()};
            dtls ->
                {dtls_options, default_udp_options()};
            tcp ->
                {tcp_options, default_tcp_options()};
            ssl ->
                {tcp_options, default_tcp_options()}
        end,
    case lists:keytake(Key, 1, Options) of
        {value, {Key, TcpOpts}, Options1} ->
            [
                {Key, emqx_utils:merge_opts(Default, TcpOpts)}
                | Options1
            ];
        false ->
            [{Key, Default} | Options]
    end.

do_start_listener(Type, Name, ListenOn, SocketOpts, MFA) when
    Type == tcp;
    Type == ssl
->
    esockd:open(Name, ListenOn, SocketOpts, MFA);
do_start_listener(udp, Name, ListenOn, SocketOpts, MFA) ->
    esockd:open_udp(Name, ListenOn, SocketOpts, MFA);
do_start_listener(dtls, Name, ListenOn, SocketOpts, MFA) ->
    esockd:open_dtls(Name, ListenOn, SocketOpts, MFA).

-spec stop_listeners(GwName :: atom(), Listeners :: list()) -> ok.
stop_listeners(GwName, Listeners) ->
    lists:foreach(fun(L) -> stop_listener(GwName, L) end, Listeners).

-spec stop_listener(GwName :: atom(), Listener :: tuple()) -> ok.
stop_listener(GwName, {Type, LisName, ListenOn, Cfg}) ->
    StopRet = stop_listener(GwName, Type, LisName, ListenOn, Cfg),
    ListenOnStr = emqx_listeners:format_bind(ListenOn),
    case StopRet of
        ok ->
            console_print(
                "Gateway ~ts:~ts:~ts on ~ts stopped.~n",
                [GwName, Type, LisName, ListenOnStr]
            );
        {error, Reason} ->
            ?ELOG(
                "Failed to stop gateway ~ts:~ts:~ts on ~ts: ~0p~n",
                [GwName, Type, LisName, ListenOnStr, Reason]
            )
    end,
    StopRet.

stop_listener(GwName, Type, LisName, ListenOn, _Cfg) when
    Type == tcp;
    Type == ssl;
    Type == udp;
    Type == dtls
->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    esockd:close(Name, ListenOn);
stop_listener(GwName, Type, LisName, ListenOn, _Cfg) when
    Type == ws; Type == wss
->
    Name = emqx_gateway_utils:listener_id(GwName, Type, LisName),
    case cowboy:stop_listener(Name) of
        ok ->
            wait_listener_stopped(ListenOn);
        Error ->
            Error
    end.

wait_listener_stopped(ListenOn) ->
    % NOTE
    % `cowboy:stop_listener/1` will not close the listening socket explicitly,
    % it will be closed by the runtime system **only after** the process exits.
    Endpoint = maps:from_list(ip_port(ListenOn)),
    case
        gen_tcp:connect(
            maps:get(ip, Endpoint, loopback),
            maps:get(port, Endpoint),
            [{active, false}]
        )
    of
        {error, _EConnrefused} ->
            %% NOTE
            %% We should get `econnrefused` here because acceptors are already dead
            %% but don't want to crash if not, because this doesn't make any difference.
            ok;
        {ok, Socket} ->
            %% NOTE
            %% Tiny chance to get a connected socket here, when some other process
            %% concurrently binds to the same port.
            gen_tcp:close(Socket)
    end.

-ifndef(TEST).
console_print(Fmt, Args) -> ?ULOG(Fmt, Args).
-else.
console_print(_Fmt, _Args) -> ok.
-endif.

apply({M, F, A}, A2) when
    is_atom(M),
    is_atom(M),
    is_list(A),
    is_list(A2)
->
    erlang:apply(M, F, A ++ A2);
apply({F, A}, A2) when
    is_function(F),
    is_list(A),
    is_list(A2)
->
    erlang:apply(F, A ++ A2);
apply(F, A2) when
    is_function(F),
    is_list(A2)
->
    erlang:apply(F, A2).

parse_listenon(Port) when is_integer(Port) ->
    Port;
parse_listenon(IpPort) when is_tuple(IpPort) ->
    IpPort;
parse_listenon(Str) when is_binary(Str) ->
    parse_listenon(binary_to_list(Str));
parse_listenon(Str) when is_list(Str) ->
    try
        list_to_integer(Str)
    catch
        _:_ ->
            case emqx_schema:to_ip_port(Str) of
                {ok, R} -> R;
                {error, _} -> error({invalid_listenon_name, Str})
            end
    end.

listener_id(GwName, Type, LisName) ->
    binary_to_atom(
        <<(bin(GwName))/binary, ":", (bin(Type))/binary, ":", (bin(LisName))/binary>>
    ).

parse_listener_id(Id) when is_atom(Id) ->
    parse_listener_id(atom_to_binary(Id));
parse_listener_id(Id) ->
    try
        [GwName, Type, Name] = binary:split(bin(Id), <<":">>, [global]),
        {GwName, Type, Name}
    catch
        _:_ -> error({invalid_listener_id, Id})
    end.

is_running(ListenerId, #{<<"bind">> := ListenOn}) ->
    is_running(ListenerId, ListenOn);
is_running(ListenerId, ListenOn0) ->
    ListenOn = emqx_gateway_utils:parse_listenon(ListenOn0),
    try esockd:listener({ListenerId, ListenOn}) of
        Pid when is_pid(Pid) ->
            true
    catch
        _:_ ->
            false
    end.

%% same with emqx_authn_chains:global_chain/1
-spec global_chain(GatewayName :: atom()) -> atom().
global_chain('mqttsn') ->
    'mqtt-sn:global';
global_chain(coap) ->
    'coap:global';
global_chain(lwm2m) ->
    'lwm2m:global';
global_chain(stomp) ->
    'stomp:global';
global_chain(exproto) ->
    'exproto:global';
global_chain(_) ->
    'unknown:global'.

listener_chain(GwName, Type, LisName) ->
    listener_id(GwName, Type, LisName).

bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(L) when is_list(L); is_binary(L) ->
    iolist_to_binary(L).

unix_ts_to_rfc3339(Keys, Map) when is_list(Keys) ->
    lists:foldl(fun(K, Acc) -> unix_ts_to_rfc3339(K, Acc) end, Map, Keys);
unix_ts_to_rfc3339(Key, Map) ->
    case maps:get(Key, Map, undefined) of
        undefined ->
            Map;
        Ts ->
            Map#{
                Key => emqx_utils_calendar:epoch_to_rfc3339(Ts)
            }
    end.

-spec stringfy(term()) -> binary().
stringfy(T) when is_list(T); is_binary(T) ->
    iolist_to_binary(T);
stringfy(T) ->
    iolist_to_binary(io_lib:format("~0p", [T])).

-spec normalize_config(emqx_config:config()) ->
    list({
        Type :: udp | tcp | ssl | dtls,
        Name :: atom(),
        ListenOn :: esockd:listen_on(),
        RawCfg :: map()
    }).
normalize_config(RawConf) ->
    LisMap = maps:get(listeners, RawConf, #{}),
    Cfg0 = maps:without([listeners], RawConf),
    lists:append(
        maps:fold(
            fun(Type, Liss, AccIn1) ->
                Listeners =
                    maps:fold(
                        fun(Name, Confs, AccIn2) ->
                            ListenOn = maps:get(bind, Confs),
                            [{Type, Name, ListenOn, Confs#{gw_conf => Cfg0}} | AccIn2]
                        end,
                        [],
                        Liss
                    ),
                [Listeners | AccIn1]
            end,
            [],
            LisMap
        )
    ).

esockd_opts(Type, Opts0) when ?IS_ESOCKD_LISTENER(Type) ->
    Opts1 = maps:with(
        [
            acceptors,
            max_connections,
            max_conn_rate,
            proxy_protocol,
            proxy_protocol_timeout,
            health_check
        ],
        Opts0
    ),
    Opts2 = Opts1#{access_rules => esockd_access_rules(maps:get(access_rules, Opts0, []))},
    maps:to_list(
        case Type of
            tcp ->
                Opts2#{tcp_options => tcp_opts(Opts0)};
            ssl ->
                Opts2#{
                    tcp_options => tcp_opts(Opts0),
                    ssl_options => ssl_opts(ssl_options, Opts0)
                };
            udp ->
                Opts2#{udp_options => udp_opts(Opts0)};
            dtls ->
                UDPOpts = udp_opts(Opts0),
                DTLSOpts = ssl_opts(dtls_options, Opts0),
                Opts2#{
                    udp_options => UDPOpts,
                    dtls_options => DTLSOpts
                }
        end
    ).

tcp_opts(Opts) ->
    emqx_listeners:tcp_opts(Opts).

udp_opts(Opts) ->
    maps:to_list(
        maps:without(
            [active_n],
            maps:get(udp_options, Opts, #{})
        )
    ).

ssl_opts(Name, Opts) ->
    SSLOpts = maps:get(Name, Opts, #{}),
    emqx_utils:run_fold(
        [
            fun ensure_dtls_protocol/2,
            fun ssl_opts_crl_config/2,
            fun ssl_opts_drop_unsupported/2,
            fun ssl_partial_chain/2,
            fun ssl_verify_fun/2,
            fun ssl_server_opts/2
        ],
        SSLOpts,
        Name
    ).

ensure_dtls_protocol(SSLOpts, dtls_options) ->
    SSLOpts#{protocol => dtls};
ensure_dtls_protocol(SSLOpts, _) ->
    SSLOpts.

ssl_opts_crl_config(#{enable_crl_check := true} = SSLOpts, _Name) ->
    HTTPTimeout = emqx_config:get([crl_cache, http_timeout], timer:seconds(15)),
    NSSLOpts = maps:remove(enable_crl_check, SSLOpts),
    NSSLOpts#{
        %% `crl_check => true' doesn't work
        crl_check => peer,
        crl_cache => {emqx_ssl_crl_cache, {internal, [{http, HTTPTimeout}]}}
    };
ssl_opts_crl_config(SSLOpts, _Name) ->
    %% NOTE: Removing this because DTLS doesn't like any unknown options.
    maps:remove(enable_crl_check, SSLOpts).

ssl_opts_drop_unsupported(SSLOpts, _Name) ->
    %% TODO: Support OCSP stapling
    maps:without([ocsp], SSLOpts).

ssl_server_opts(SSLOpts, ssl_options) ->
    emqx_tls_lib:to_server_opts(tls, SSLOpts);
ssl_server_opts(SSLOpts, dtls_options) ->
    emqx_tls_lib:to_server_opts(dtls, SSLOpts).

ssl_partial_chain(SSLOpts, _Options) ->
    emqx_tls_lib:maybe_inject_ssl_fun(root_fun, SSLOpts).

ssl_verify_fun(SSLOpts, _Options) ->
    emqx_tls_lib:maybe_inject_ssl_fun(verify_fun, SSLOpts).

ranch_opts(Type, ListenOn, Opts) ->
    NumAcceptors = maps:get(acceptors, Opts, 4),
    MaxConnections = maps:get(max_connections, Opts, 1024),
    SocketOpts1 =
        case Type of
            wss ->
                tcp_opts(Opts) ++
                    proplists:delete(handshake_timeout, ssl_opts(ssl_options, Opts));
            ws ->
                tcp_opts(Opts)
        end,
    SocketOpts = ip_port(ListenOn) ++ proplists:delete(reuseaddr, SocketOpts1),
    #{
        num_acceptors => NumAcceptors,
        max_connections => MaxConnections,
        handshake_timeout => maps:get(handshake_timeout, Opts, 15000),
        socket_opts => SocketOpts
    }.

ws_opts(Opts, Conf) ->
    ConnMod = maps:get(connection_mod, Conf, emqx_gateway_conn),
    WsPaths = [
        {emqx_utils_maps:deep_get([websocket, path], Opts, "") ++ "/[...]", ConnMod, Conf}
    ],
    Dispatch = cowboy_router:compile([{'_', WsPaths}]),
    ProxyProto = maps:get(proxy_protocol, Opts, false),
    #{env => #{dispatch => Dispatch}, proxy_header => ProxyProto}.

ip_port(Port) when is_integer(Port) ->
    [{port, Port}];
ip_port({Addr, Port}) ->
    [{ip, Addr}, {port, Port}].

%%--------------------------------------------------------------------
%% Envs

active_n(Options) ->
    maps:get(active_n, Options, ?ACTIVE_N).

-spec idle_timeout(map()) -> pos_integer().
idle_timeout(Options) ->
    maps:get(idle_timeout, Options, ?DEFAULT_IDLE_TIMEOUT).

-spec ratelimit(map()) -> esockd_rate_limit:config() | undefined.
ratelimit(Options) ->
    maps:get(ratelimit, Options, undefined).

-spec frame_options(map()) -> map().
frame_options(Options) ->
    maps:get(frame, Options, #{}).

-spec init_gc_state(map()) -> emqx_gc:gc_state() | undefined.
init_gc_state(Options) ->
    emqx_utils:maybe_apply(fun emqx_gc:init/1, force_gc_policy(Options)).

-spec force_gc_policy(map()) -> emqx_gc:opts() | undefined.
force_gc_policy(Options) ->
    maps:get(force_gc_policy, Options, ?DEFAULT_GC_OPTS).

-spec oom_policy(map()) -> emqx_types:oom_policy().
oom_policy(Options) ->
    maps:get(force_shutdown_policy, Options, ?DEFAULT_OOM_POLICY).

-spec stats_timer(map()) -> undefined | disabled.
stats_timer(Options) ->
    case enable_stats(Options) of
        true -> undefined;
        false -> disabled
    end.

-spec enable_stats(map()) -> boolean().
enable_stats(Options) ->
    maps:get(enable_stats, Options, true).

%%--------------------------------------------------------------------
%% Envs2

default_tcp_options() ->
    [
        binary,
        {packet, raw},
        {reuseaddr, true},
        {nodelay, true},
        {backlog, 512}
    ].

default_udp_options() ->
    [].

default_subopts() ->
    %% Retain Handling
    #{
        rh => 1,
        %% Retain as Publish
        rap => 0,
        %% No Local
        nl => 0,
        %% QoS
        qos => 0,
        is_new => true
    }.

-spec find_gateway_definitions() -> list(gateway_def()).
find_gateway_definitions() ->
    read_pt_populate_if_missing(
        emqx_gateways,
        fun do_find_gateway_definitions/0
    ).

do_find_gateway_definitions() ->
    lists:flatmap(
        fun(App) ->
            lists:flatmap(fun gateways/1, find_attrs(App, gateway))
        end,
        ?GATEWAYS
    ).

read_pt_populate_if_missing(Key, Fn) ->
    case persistent_term:get(Key, no_value) of
        no_value ->
            Value = Fn(),
            _ = persistent_term:put(Key, {value, Value}),
            Value;
        {value, Value} ->
            Value
    end.

-spec find_gateway_definition(atom()) -> {ok, map()} | {error, term()}.
find_gateway_definition(Name) ->
    find_gateway_definition(Name, ?GATEWAYS).

-dialyzer({no_match, [find_gateway_definition/2]}).
find_gateway_definition(Name, [App | T]) ->
    Attrs = find_attrs(App, gateway),
    SearchFun = fun(#{name := GwName}) ->
        GwName =:= Name
    end,
    case lists:search(SearchFun, Attrs) of
        {value, Definition} ->
            case check_gateway_edition(Definition) of
                true ->
                    {ok, Definition};
                _ ->
                    {error, invalid_edition}
            end;
        false ->
            find_gateway_definition(Name, T)
    end;
find_gateway_definition(_Name, []) ->
    {error, not_found}.

-dialyzer({no_match, [gateways/1]}).
gateways(
    Definition = #{
        name := Name,
        callback_module := CbMod,
        config_schema_module := SchemaMod
    }
) when is_atom(Name), is_atom(CbMod), is_atom(SchemaMod) ->
    case check_gateway_edition(Definition) of
        true ->
            [Definition];
        _ ->
            []
    end.

-if(?EMQX_RELEASE_EDITION == ee).
check_gateway_edition(_Defination) ->
    true.
-else.
check_gateway_edition(Defination) ->
    ce == maps:get(edition, Defination, ce).
-endif.

find_attrs(AppMod, Def) ->
    [
        Attr
     || {Name, Attrs} <- module_attributes(AppMod),
        Name =:= Def,
        Attr <- Attrs
    ].

module_attributes(Module) ->
    try
        apply(Module, module_info, [attributes])
    catch
        error:undef -> []
    end.

-spec plus_max_connections(non_neg_integer() | infinity, non_neg_integer() | infinity) ->
    pos_integer() | infinity.
plus_max_connections(_, infinity) ->
    infinity;
plus_max_connections(infinity, _) ->
    infinity;
plus_max_connections(A, B) when is_integer(A) andalso is_integer(B) ->
    A + B.

random_clientid(GwName) when is_atom(GwName) ->
    iolist_to_binary([atom_to_list(GwName), "-", emqx_utils:gen_id()]).
