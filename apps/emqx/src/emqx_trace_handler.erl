%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_trace_handler).

-include("emqx.hrl").
-include("logger.hrl").

%% APIs
-export([ running/0
        , install/3
        , install/4
        , uninstall/1
        , uninstall/2
        ]).

%% For logger handler filters callbacks
-export([ filter_clientid/2
        , filter_topic/2
        , filter_ip_address/2
        ]).

-type tracer() :: #{
                    name := binary(),
                    type := clientid | topic | ip_address,
                    filter := emqx_types:clientid() | emqx_types:topic() | emqx_trace:ip_address()
                   }.

-define(FORMAT,
    {logger_formatter, #{
        template => [
            time, " [", level, "] ",
            {clientid,
                [{peername, [clientid, "@", peername, " "], [clientid, " "]}],
                [{peername, [peername, " "], []}]
            },
            msg, "\n"
        ],
        single_line => false,
        max_size => unlimited,
        depth => unlimited
    }}
).

-define(CONFIG(_LogFile_), #{
    type => halt,
    file => _LogFile_,
    max_no_bytes => 512 * 1024 * 1024,
    overload_kill_enable => true,
    overload_kill_mem_size => 50 * 1024 * 1024,
    overload_kill_qlen => 20000,
    %% disable restart
    overload_kill_restart_after => infinity
    }).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec install(Name :: binary() | list(),
              Type :: clientid | topic | ip_address,
              Filter ::emqx_types:clientid() | emqx_types:topic() | string(),
              Level :: logger:level() | all,
              LogFilePath :: string()) -> ok | {error, term()}.
install(Name, Type, Filter, Level, LogFile) ->
    Who = #{type => Type, filter => ensure_bin(Filter), name => ensure_bin(Name)},
    install(Who, Level, LogFile).

-spec install(Type :: clientid | topic | ip_address,
              Filter ::emqx_types:clientid() | emqx_types:topic() | string(),
              Level :: logger:level() | all,
              LogFilePath :: string()) -> ok | {error, term()}.
install(Type, Filter, Level, LogFile) ->
    install(Filter, Type, Filter, Level, LogFile).

-spec install(tracer(), logger:level() | all, string()) -> ok | {error, term()}.
install(Who, all, LogFile) ->
    install(Who, debug, LogFile);
install(Who, Level, LogFile) ->
    PrimaryLevel = emqx_logger:get_primary_log_level(),
    try logger:compare_levels(Level, PrimaryLevel) of
        lt ->
            {error,
                io_lib:format(
                    "Cannot trace at a log level (~s) "
                    "lower than the primary log level (~s)",
                    [Level, PrimaryLevel]
                )};
        _GtOrEq ->
            install_handler(Who, Level, LogFile)
    catch
        error:badarg ->
            {error, {invalid_log_level, Level}}
    end.

-spec uninstall(Type :: clientid | topic | ip_address,
                Name :: binary() | list()) -> ok | {error, term()}.
uninstall(Type, Name) ->
    HandlerId = handler_id(#{type => Type, name => ensure_bin(Name)}),
    uninstall(HandlerId).

-spec uninstall(HandlerId :: atom()) -> ok | {error, term()}.
uninstall(HandlerId) ->
    Res = logger:remove_handler(HandlerId),
    show_prompts(Res, HandlerId, "Stop trace"),
    Res.

%% @doc Return all running trace handlers information.
-spec running() ->
    [
        #{
            name => binary(),
            type => topic | clientid | ip_address,
            id => atom(),
            filter => emqx_types:topic() | emqx_types:clienetid() | emqx_trace:ip_address(),
            level => logger:level(),
            dst => file:filename() | console | unknown
        }
    ].
running() ->
    lists:foldl(fun filter_traces/2, [], emqx_logger:get_log_handlers(started)).

-spec filter_clientid(logger:log_event(), string()) -> logger:log_event() | ignore.
filter_clientid(#{meta := #{clientid := ClientId}} = Log, ClientId) -> Log;
filter_clientid(_Log, _ExpectId) -> ignore.

-spec filter_topic(logger:log_event(), string()) -> logger:log_event() | ignore.
filter_topic(#{meta := #{topic := Topic}} = Log, TopicFilter) ->
    case emqx_topic:match(Topic, TopicFilter) of
        true -> Log;
        false -> ignore
    end;
filter_topic(_Log, _ExpectId) -> ignore.

-spec filter_ip_address(logger:log_event(), string()) -> logger:log_event() | ignore.
filter_ip_address(#{meta := #{peername := Peername}} = Log, IP) ->
    case lists:prefix(IP, Peername) of
        true -> Log;
        false -> ignore
    end;
filter_ip_address(_Log, _ExpectId) -> ignore.

install_handler(Who, Level, LogFile) ->
    HandlerId = handler_id(Who),
    Config = #{
        level => Level,
        formatter => ?FORMAT,
        filter_default => stop,
        filters => filters(Who),
        config => ?CONFIG(LogFile)
    },
    Res = logger:add_handler(HandlerId, logger_disk_log_h, Config),
    show_prompts(Res, Who, "Start trace"),
    Res.

filters(#{type := clientid, filter := Filter}) ->
    [{clientid, {fun ?MODULE:filter_clientid/2, ensure_list(Filter)}}];
filters(#{type := topic, filter := Filter}) ->
    [{topic, {fun ?MODULE:filter_topic/2, ensure_bin(Filter)}}];
filters(#{type := ip_address, filter := Filter}) ->
    [{ip_address, {fun ?MODULE:filter_ip_address/2, ensure_list(Filter)}}].

filter_traces(#{id := Id, level := Level, dst := Dst, filters := Filters}, Acc) ->
    Init = #{id => Id, level => Level, dst => Dst},
    case Filters of
        [{topic, {_FilterFun, Filter}}] ->
            <<"trace_topic_", Name/binary>> = atom_to_binary(Id),
            [Init#{type => topic, filter => Filter, name => Name} | Acc];
        [{clientid, {_FilterFun, Filter}}] ->
            <<"trace_clientid_", Name/binary>> = atom_to_binary(Id),
            [Init#{type => clientid, filter => Filter, name => Name} | Acc];
        [{ip_address, {_FilterFun, Filter}}] ->
            <<"trace_ip_address_", Name/binary>> = atom_to_binary(Id),
            [Init#{type => ip_address, filter => Filter, name => Name} | Acc];
        _ ->
            Acc
    end.

handler_id(#{type := Type, name := Name}) ->
    binary_to_atom(<<"trace_", (atom_to_binary(Type))/binary, "_", Name/binary>>).

ensure_bin(List) when is_list(List) -> iolist_to_binary(List);
ensure_bin(Bin) when is_binary(Bin) -> Bin.

ensure_list(Bin) when is_binary(Bin) -> binary_to_list(Bin);
ensure_list(List) when is_list(List) -> List.

show_prompts(ok, Who, Msg) ->
    ?LOG(info, Msg ++ " ~p " ++ "successfully~n", [Who]);
show_prompts({error, Reason}, Who, Msg) ->
    ?LOG(error, Msg ++ " ~p " ++ "failed by ~p~n", [Who, Reason]).
