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

-module(emqx_coap_frame).

-behaviour(emqx_gateway_frame).

%% emqx_gateway_frame callbacks
-export([
    initial_parse_state/1,
    serialize_opts/0,
    serialize_pkt/2,
    parse/2,
    format/1,
    type/1,
    is_message/1
]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/types.hrl").

-define(VERSION, 1).

-define(OPTION_IF_MATCH, 1).
-define(OPTION_URI_HOST, 3).
-define(OPTION_ETAG, 4).
-define(OPTION_IF_NONE_MATCH, 5).
% draft-ietf-core-observe-16
-define(OPTION_OBSERVE, 6).
-define(OPTION_URI_PORT, 7).
-define(OPTION_LOCATION_PATH, 8).
-define(OPTION_URI_PATH, 11).
-define(OPTION_CONTENT_FORMAT, 12).
-define(OPTION_MAX_AGE, 14).
-define(OPTION_URI_QUERY, 15).
-define(OPTION_ACCEPT, 17).
-define(OPTION_LOCATION_QUERY, 20).
% draft-ietf-core-block-17
-define(OPTION_BLOCK2, 23).
-define(OPTION_BLOCK1, 27).
-define(OPTION_PROXY_URI, 35).
-define(OPTION_PROXY_SCHEME, 39).
-define(OPTION_SIZE1, 60).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec initial_parse_state(map()) -> emqx_gateway_frame:parse_state().
initial_parse_state(_) ->
    #{}.

-spec serialize_opts() -> emqx_gateway_frame:serialize_options().
serialize_opts() ->
    #{}.

%%--------------------------------------------------------------------
%% serialize_pkt
%%--------------------------------------------------------------------
%% empty message
serialize_pkt(#coap_message{type = Type, method = undefined, id = MsgId}, _Opts) ->
    <<?VERSION:2, (encode_type(Type)):2, 0:4, 0:3, 0:5, MsgId:16>>;
serialize_pkt(
    #coap_message{
        type = Type,
        method = Method,
        id = MsgId,
        token = Token,
        options = Options,
        payload = Payload
    },
    _Opts
) ->
    TKL = byte_size(Token),
    {Class, Code} = method_to_class_code(Method),
    Head =
        <<?VERSION:2, (encode_type(Type)):2, TKL:4, Class:3, Code:5, MsgId:16, Token:TKL/binary>>,
    FlatOpts = flatten_options(Options),
    encode_option_list(FlatOpts, 0, Head, Payload).

-spec encode_type(message_type()) -> 0..3.
encode_type(con) -> 0;
encode_type(non) -> 1;
encode_type(ack) -> 2;
encode_type(reset) -> 3.

flatten_options(Opts) ->
    flatten_options(maps:to_list(Opts), []).

flatten_options([{_OptId, undefined} | T], Acc) ->
    flatten_options(T, Acc);
flatten_options([{OptId, OptVal} | T], Acc) ->
    flatten_options(
        T,
        case is_repeatable_option(OptId) of
            false ->
                [encode_option(OptId, OptVal) | Acc];
            _ ->
                try_encode_repeatable(OptId, OptVal) ++ Acc
        end
    );
flatten_options([], Acc) ->
    %% sort by option id for calculate the deltas
    lists:keysort(1, Acc).

encode_option_list([{OptNum, OptVal} | OptionList], LastNum, Acc, Payload) ->
    NumDiff = OptNum - LastNum,
    {Delta, ExtNum} =
        if
            NumDiff >= 269 ->
                {14, <<(NumDiff - 269):16>>};
            OptNum - LastNum >= 13 ->
                {13, <<(NumDiff - 13)>>};
            true ->
                {NumDiff, <<>>}
        end,
    Binaryize = byte_size(OptVal),
    {Len, ExtLen} =
        if
            Binaryize >= 269 ->
                {14, <<(Binaryize - 269):16>>};
            Binaryize >= 13 ->
                {13, <<(Binaryize - 13)>>};
            true ->
                {Binaryize, <<>>}
        end,
    Acc2 = <<Acc/binary, Delta:4, Len:4, ExtNum/binary, ExtLen/binary, OptVal/binary>>,
    encode_option_list(OptionList, OptNum, Acc2, Payload);
encode_option_list([], _LastNum, Acc, <<>>) ->
    Acc;
encode_option_list([], _, Acc, Payload) ->
    <<Acc/binary, 16#FF, Payload/binary>>.

try_encode_repeatable(uri_query, Val) when is_map(Val) ->
    maps:fold(
        fun(K, V, Acc) ->
            [encode_option(uri_query, <<K/binary, $=, V/binary>>) | Acc]
        end,
        [],
        Val
    );
try_encode_repeatable(K, Val) ->
    lists:foldr(
        fun
            (undefined, Acc) ->
                Acc;
            (E, Acc) ->
                [encode_option(K, E) | Acc]
        end,
        [],
        Val
    ).

%% RFC 7252
encode_option(if_match, OptVal) ->
    {?OPTION_IF_MATCH, OptVal};
encode_option(uri_host, OptVal) ->
    {?OPTION_URI_HOST, OptVal};
encode_option(etag, OptVal) ->
    {?OPTION_ETAG, OptVal};
encode_option(if_none_match, true) ->
    {?OPTION_IF_NONE_MATCH, <<>>};
encode_option(uri_port, OptVal) ->
    {?OPTION_URI_PORT, binary:encode_unsigned(OptVal)};
encode_option(location_path, OptVal) ->
    {?OPTION_LOCATION_PATH, OptVal};
encode_option(uri_path, OptVal) ->
    {?OPTION_URI_PATH, OptVal};
encode_option(content_format, OptVal) when is_integer(OptVal) ->
    {?OPTION_CONTENT_FORMAT, binary:encode_unsigned(OptVal)};
encode_option(content_format, OptVal) ->
    Num = content_format_to_code(OptVal),
    {?OPTION_CONTENT_FORMAT, binary:encode_unsigned(Num)};
encode_option(max_age, OptVal) ->
    {?OPTION_MAX_AGE, binary:encode_unsigned(OptVal)};
encode_option(uri_query, OptVal) ->
    {?OPTION_URI_QUERY, OptVal};
encode_option('accept', OptVal) ->
    {?OPTION_ACCEPT, binary:encode_unsigned(OptVal)};
encode_option(location_query, OptVal) ->
    {?OPTION_LOCATION_QUERY, OptVal};
encode_option(proxy_uri, OptVal) ->
    {?OPTION_PROXY_URI, OptVal};
encode_option(proxy_scheme, OptVal) ->
    {?OPTION_PROXY_SCHEME, OptVal};
encode_option(size1, OptVal) ->
    {?OPTION_SIZE1, binary:encode_unsigned(OptVal)};
encode_option(observe, OptVal) ->
    {?OPTION_OBSERVE, binary:encode_unsigned(OptVal)};
encode_option(block2, OptVal) ->
    {?OPTION_BLOCK2, encode_block(OptVal)};
encode_option(block1, OptVal) ->
    {?OPTION_BLOCK1, encode_block(OptVal)};
%% unknown opton
encode_option(Option, Value) ->
    erlang:throw({bad_option, Option, Value}).

encode_block({Num, More, Size}) ->
    encode_block1(
        Num,
        if
            More -> 1;
            true -> 0
        end,
        trunc(math:log2(Size)) - 4
    ).

encode_block1(Num, M, SizEx) when Num < 16 ->
    <<Num:4, M:1, SizEx:3>>;
encode_block1(Num, M, SizEx) when Num < 4096 ->
    <<Num:12, M:1, SizEx:3>>;
encode_block1(Num, M, SizEx) ->
    <<Num:28, M:1, SizEx:3>>.

-spec content_format_to_code(binary()) -> non_neg_integer().
content_format_to_code(<<"text/plain">>) -> 0;
content_format_to_code(<<"application/link-format">>) -> 40;
content_format_to_code(<<"application/xml">>) -> 41;
content_format_to_code(<<"application/octet-stream">>) -> 42;
content_format_to_code(<<"application/exi">>) -> 47;
content_format_to_code(<<"application/json">>) -> 50;
content_format_to_code(<<"application/cbor">>) -> 60;
content_format_to_code(<<"application/vnd.oma.lwm2m+tlv">>) -> 11542;
content_format_to_code(<<"application/vnd.oma.lwm2m+json">>) -> 11543;
%% use octet-stream as default
content_format_to_code(_) -> 42.

method_to_class_code(get) -> {0, 01};
method_to_class_code(post) -> {0, 02};
method_to_class_code(put) -> {0, 03};
method_to_class_code(delete) -> {0, 04};
method_to_class_code({ok, created}) -> {2, 01};
method_to_class_code({ok, deleted}) -> {2, 02};
method_to_class_code({ok, valid}) -> {2, 03};
method_to_class_code({ok, changed}) -> {2, 04};
method_to_class_code({ok, content}) -> {2, 05};
method_to_class_code({ok, nocontent}) -> {2, 07};
method_to_class_code({ok, continue}) -> {2, 31};
method_to_class_code({error, bad_request}) -> {4, 00};
method_to_class_code({error, unauthorized}) -> {4, 01};
method_to_class_code({error, bad_option}) -> {4, 02};
method_to_class_code({error, forbidden}) -> {4, 03};
method_to_class_code({error, not_found}) -> {4, 04};
method_to_class_code({error, method_not_allowed}) -> {4, 05};
method_to_class_code({error, not_acceptable}) -> {4, 06};
method_to_class_code({error, request_entity_incomplete}) -> {4, 08};
method_to_class_code({error, precondition_failed}) -> {4, 12};
method_to_class_code({error, request_entity_too_large}) -> {4, 13};
method_to_class_code({error, unsupported_content_format}) -> {4, 15};
method_to_class_code({error, internal_server_error}) -> {5, 00};
method_to_class_code({error, not_implemented}) -> {5, 01};
method_to_class_code({error, bad_gateway}) -> {5, 02};
method_to_class_code({error, service_unavailable}) -> {5, 03};
method_to_class_code({error, gateway_timeout}) -> {5, 04};
method_to_class_code({error, proxying_not_supported}) -> {5, 05};
method_to_class_code(Method) -> erlang:throw({bad_method, Method}).

%%--------------------------------------------------------------------
%% parse
%%--------------------------------------------------------------------

-spec parse(binary(), emqx_gateway_frame:parse_state()) ->
    emqx_gateway_frame:parse_result().
parse(<<?VERSION:2, Type:2, 0:4, 0:3, 0:5, MsgId:16>>, ParseState) ->
    {ok,
        #coap_message{
            type = decode_type(Type),
            id = MsgId
        },
        <<>>, ParseState};
parse(
    <<?VERSION:2, Type:2, TKL:4, Class:3, Code:5, MsgId:16, Token:TKL/binary, Tail/binary>>,
    ParseState
) ->
    {Options, Payload} = decode_option_list(Tail),
    Options2 = maps:fold(
        fun(K, V, Acc) ->
            Acc#{K => get_option_val(K, V)}
        end,
        #{},
        Options
    ),
    {ok,
        #coap_message{
            type = decode_type(Type),
            method = class_code_to_method({Class, Code}),
            id = MsgId,
            token = Token,
            options = Options2,
            payload = Payload
        },
        <<>>, ParseState}.

get_option_val(uri_query, V) ->
    KVList = lists:foldl(
        fun(E, Acc) ->
            case re:split(E, "=") of
                [Key, Val] ->
                    [{Key, Val} | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        V
    ),
    maps:from_list(KVList);
get_option_val(K, V) ->
    case is_repeatable_option(K) of
        true ->
            lists:reverse(V);
        _ ->
            V
    end.

-spec decode_type(X) -> message_type() when
    X :: 0..3.
decode_type(0) -> con;
decode_type(1) -> non;
decode_type(2) -> ack;
decode_type(3) -> reset.

-spec decode_option_list(binary()) -> {message_options(), binary()}.
decode_option_list(Bin) ->
    decode_option_list(Bin, 0, #{}).

decode_option_list(<<>>, _OptNum, OptMap) ->
    {OptMap, <<>>};
decode_option_list(<<16#FF, Payload/binary>>, _OptNum, OptMap) ->
    {OptMap, Payload};
decode_option_list(<<Delta:4, Len:4, Bin/binary>>, OptNum, OptMap) ->
    case Delta of
        Any when Any < 13 ->
            decode_option_len(Bin, OptNum + Delta, Len, OptMap);
        13 ->
            <<ExtOptNum, NewBin/binary>> = Bin,
            decode_option_len(NewBin, OptNum + ExtOptNum + 13, Len, OptMap);
        14 ->
            <<ExtOptNum:16, NewBin/binary>> = Bin,
            decode_option_len(NewBin, OptNum + ExtOptNum + 269, Len, OptMap)
    end.

decode_option_len(<<Bin/binary>>, OptNum, Len, OptMap) ->
    case Len of
        Any when Any < 13 ->
            decode_option_value(Bin, OptNum, Len, OptMap);
        13 ->
            <<ExtOptLen, NewBin/binary>> = Bin,
            decode_option_value(NewBin, OptNum, ExtOptLen + 13, OptMap);
        14 ->
            <<ExtOptLen:16, NewBin/binary>> = Bin,
            decode_option_value(NewBin, OptNum, ExtOptLen + 269, OptMap)
    end.

decode_option_value(<<Bin/binary>>, OptNum, OptLen, OptMap) ->
    case Bin of
        <<OptVal:OptLen/binary, NewBin/binary>> ->
            decode_option_list(NewBin, OptNum, append_option(OptNum, OptVal, OptMap));
        <<>> ->
            decode_option_list(<<>>, OptNum, append_option(OptNum, <<>>, OptMap))
    end.

append_option(OptNum, RawOptVal, OptMap) ->
    {OptId, OptVal} = decode_option(OptNum, RawOptVal),
    case is_repeatable_option(OptId) of
        false ->
            OptMap#{OptId => OptVal};
        _ ->
            case maps:get(OptId, OptMap, undefined) of
                undefined ->
                    OptMap#{OptId => [OptVal]};
                OptVals ->
                    OptMap#{OptId => [OptVal | OptVals]}
            end
    end.

%% RFC 7252
decode_option(?OPTION_IF_MATCH, OptVal) ->
    {if_match, OptVal};
decode_option(?OPTION_URI_HOST, OptVal) ->
    {uri_host, OptVal};
decode_option(?OPTION_ETAG, OptVal) ->
    {etag, OptVal};
decode_option(?OPTION_IF_NONE_MATCH, <<>>) ->
    {if_none_match, true};
decode_option(?OPTION_URI_PORT, OptVal) ->
    {uri_port, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_LOCATION_PATH, OptVal) ->
    {location_path, OptVal};
decode_option(?OPTION_URI_PATH, OptVal) ->
    {uri_path, OptVal};
decode_option(?OPTION_CONTENT_FORMAT, OptVal) ->
    Num = binary:decode_unsigned(OptVal),
    {content_format, content_code_to_format(Num)};
decode_option(?OPTION_MAX_AGE, OptVal) ->
    {max_age, binary:decode_unsigned(OptVal)};
decode_option(?OPTION_URI_QUERY, OptVal) ->
    {uri_query, OptVal};
decode_option(?OPTION_ACCEPT, OptVal) ->
    {'accept', binary:decode_unsigned(OptVal)};
decode_option(?OPTION_LOCATION_QUERY, OptVal) ->
    {location_query, OptVal};
decode_option(?OPTION_PROXY_URI, OptVal) ->
    {proxy_uri, OptVal};
decode_option(?OPTION_PROXY_SCHEME, OptVal) ->
    {proxy_scheme, OptVal};
decode_option(?OPTION_SIZE1, OptVal) ->
    {size1, binary:decode_unsigned(OptVal)};
%% draft-ietf-core-observe-16
decode_option(?OPTION_OBSERVE, OptVal) ->
    {observe, binary:decode_unsigned(OptVal)};
%% draft-ietf-core-block-17
decode_option(?OPTION_BLOCK2, OptVal) ->
    {block2, decode_block(OptVal)};
decode_option(?OPTION_BLOCK1, OptVal) ->
    {block1, decode_block(OptVal)};
%% unknown option
decode_option(OptNum, OptVal) ->
    {OptNum, OptVal}.

decode_block(<<Num:4, M:1, SizEx:3>>) -> decode_block1(Num, M, SizEx);
decode_block(<<Num:12, M:1, SizEx:3>>) -> decode_block1(Num, M, SizEx);
decode_block(<<Num:28, M:1, SizEx:3>>) -> decode_block1(Num, M, SizEx).

decode_block1(Num, M, SizEx) ->
    {Num, M =/= 0, trunc(math:pow(2, SizEx + 4))}.

-spec content_code_to_format(non_neg_integer()) -> binary().
content_code_to_format(0) -> <<"text/plain">>;
content_code_to_format(40) -> <<"application/link-format">>;
content_code_to_format(41) -> <<"application/xml">>;
content_code_to_format(42) -> <<"application/octet-stream">>;
content_code_to_format(47) -> <<"application/exi">>;
content_code_to_format(50) -> <<"application/json">>;
content_code_to_format(60) -> <<"application/cbor">>;
content_code_to_format(11542) -> <<"application/vnd.oma.lwm2m+tlv">>;
content_code_to_format(11543) -> <<"application/vnd.oma.lwm2m+json">>;
%% use octet as default
content_code_to_format(_) -> <<"application/octet-stream">>.

%% RFC 7252
%% atom indicate a request
class_code_to_method({0, 01}) -> get;
class_code_to_method({0, 02}) -> post;
class_code_to_method({0, 03}) -> put;
class_code_to_method({0, 04}) -> delete;
%% success is a tuple {ok, ...}
class_code_to_method({2, 01}) -> {ok, created};
class_code_to_method({2, 02}) -> {ok, deleted};
class_code_to_method({2, 03}) -> {ok, valid};
class_code_to_method({2, 04}) -> {ok, changed};
class_code_to_method({2, 05}) -> {ok, content};
class_code_to_method({2, 07}) -> {ok, nocontent};
% block
class_code_to_method({2, 31}) -> {ok, continue};
%% error is a tuple {error, ...}
class_code_to_method({4, 00}) -> {error, bad_request};
class_code_to_method({4, 01}) -> {error, unauthorized};
class_code_to_method({4, 02}) -> {error, bad_option};
class_code_to_method({4, 03}) -> {error, forbidden};
class_code_to_method({4, 04}) -> {error, not_found};
class_code_to_method({4, 05}) -> {error, method_not_allowed};
class_code_to_method({4, 06}) -> {error, not_acceptable};
% block
class_code_to_method({4, 08}) -> {error, request_entity_incomplete};
class_code_to_method({4, 12}) -> {error, precondition_failed};
class_code_to_method({4, 13}) -> {error, request_entity_too_large};
class_code_to_method({4, 15}) -> {error, unsupported_content_format};
class_code_to_method({5, 00}) -> {error, internal_server_error};
class_code_to_method({5, 01}) -> {error, not_implemented};
class_code_to_method({5, 02}) -> {error, bad_gateway};
class_code_to_method({5, 03}) -> {error, service_unavailable};
class_code_to_method({5, 04}) -> {error, gateway_timeout};
class_code_to_method({5, 05}) -> {error, proxying_not_supported};
class_code_to_method(_) -> undefined.

format(Msg) ->
    io_lib:format("~0p", [Msg]).

type(_) ->
    coap.

is_message(#coap_message{}) ->
    true;
is_message(_) ->
    false.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
-spec is_repeatable_option(message_option_name()) -> boolean().
is_repeatable_option(if_match) -> true;
is_repeatable_option(etag) -> true;
is_repeatable_option(location_path) -> true;
is_repeatable_option(uri_path) -> true;
is_repeatable_option(uri_query) -> true;
is_repeatable_option(location_query) -> true;
is_repeatable_option(_) -> false.
