%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_slow_subs_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_management/include/emqx_mgmt.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v5").

-define(BASE_PATH, "api").
-define(NOW, erlang:system_time(millisecond)).

-define(CONF_DEFAULT, <<"""
emqx_slow_subs
{
 enable = true
 top_k_num = 5,
 expire_interval = 60000
 notice_interval = 0
 notice_qos = 0
 notice_batch_size = 3
}""">>).


all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_slow_subs_schema, ?CONF_DEFAULT),
    emqx_mgmt_api_test_util:init_suite([emqx_slow_subs]),
    {ok, _} = application:ensure_all_started(emqx_authn),
    Config.

end_per_suite(Config) ->
    application:stop(emqx_authn),
    emqx_mgmt_api_test_util:end_suite([emqx_slow_subs]),
    Config.

init_per_testcase(_, Config) ->
    application:ensure_all_started(emqx_slow_subs),
    timer:sleep(500),
    Config.

end_per_testcase(_, Config) ->
    application:stop(emqx_slow_subs),
    Config.

t_get_history(_) ->
    Now = ?NOW,
    Each = fun(I) ->
                   ClientId = erlang:list_to_binary(io_lib:format("test_~p", [I])),
                   ets:insert(?TOPK_TAB, #top_k{index = ?INDEX(I, ClientId),
                                                type = average,
                                                last_update_time = Now})
           end,

    lists:foreach(Each, lists:seq(1, 5)),

    {ok, Data} = request_api(get, api_path(["slow_subscriptions"]), "_page=1&_limit=10",
                             auth_header_()),
    #{<<"data">> := [First | _]} = emqx_json:decode(Data, [return_maps]),

    RFirst = #{<<"clientid">> => <<"test_5">>,
               <<"latency">> => 5,
               <<"type">> => <<"average">>,
               <<"last_update_time">> => Now},

    ?assertEqual(RFirst, First).

t_clear(_) ->
    ets:insert(?TOPK_TAB, #top_k{index = ?INDEX(1, <<"test">>),
                                 type = average,
                                 last_update_time = ?NOW}),

    {ok, _} = request_api(delete, api_path(["slow_subscriptions"]), [],
                          auth_header_()),

    ?assertEqual(0, ets:info(?TOPK_TAB, size)).

t_settting(_) ->
    Conf = emqx:get_config([emqx_slow_subs]),
    Conf2 = Conf#{threshold => 1000},
    {ok, Data} = request_api(put,
                             api_path(["slow_subscriptions", "settings"]),
                             [],
                             auth_header_(),
                             Conf2),

    Return = decode_json(Data),

    ?assertEqual(Conf2, Return),

    {ok, GetData} = request_api(get,
                                api_path(["slow_subscriptions", "settings"]),
                                [],
                                auth_header_()
                            ),

    GetReturn = decode_json(GetData),

    ?assertEqual(Conf2, GetReturn),

    ?assertEqual(1000,
                 emqx_message_latency_stats:get_threshold()).

decode_json(Data) ->
    BinJosn = emqx_json:decode(Data, [return_maps]),
    emqx_map_lib:unsafe_atom_key_map(BinJosn).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
          when Code =:= 200 orelse Code =:= 204 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).
