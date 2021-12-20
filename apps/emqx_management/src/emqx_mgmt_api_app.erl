%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_app).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([api_key/2, api_key_by_name/2]).
-export([validate_name/1]).

namespace() -> "api_key".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/api_key", "/api_key/:name"].


schema("/api_key") ->
    #{
        'operationId' => api_key,
        get => #{
            description => "Return api_key list",
            responses => #{
                200 => delete([api_secret], fields(app))
            }
        },
        post => #{
            description => "Create new api_key",
            'requestBody' => delete([created_at, api_key, api_secret], fields(app)),
            responses => #{
                200 => hoconsc:ref(app)
            }
        }
    };
schema("/api_key/:name") ->
    #{
        'operationId' => api_key_by_name,
        get => #{
            description => "Return the specific api_key",
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => delete([api_secret], fields(app))
            }
        },
        put => #{
            description => "Update the specific api_key",
            parameters => [hoconsc:ref(name)],
            'requestBody' => delete([created_at, api_key, api_secret, name], fields(app)),
            responses => #{
                200 => delete([api_secret], fields(app))
            }
        },
        delete => #{
            description => "Delete the specific api_key",
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => <<"Delete successfully">>
            }
        }
    }.

fields(app) ->
    [
        {name, hoconsc:mk(binary(),
            #{desc => "Unique and format by [a-zA-Z0-9-_]",
                validator => fun ?MODULE:validate_name/1,
                example => <<"EMQX-API-KEY-1">>})},
        {api_key, hoconsc:mk(binary(),
            #{desc => """TODO:uses HMAC-SHA256 for signing.""",
                example => <<"a4697a5c75a769f6">>})},
        {api_secret, hoconsc:mk(binary(),
            #{desc => """An API secret is a simple encrypted string that identifies"""
            """an application without any principal."""
            """They are useful for accessing public data anonymously,"""
            """and are used to associate API requests.""",
                example => <<"MzAyMjk3ODMwMDk0NjIzOTUxNjcwNzQ0NzQ3MTE2NDYyMDI">>})},
        {expired_at, hoconsc:mk(emqx_schema:rfc3339_system_time(),
            #{desc => "No longer valid datetime",
                example => <<"2021-12-05T02:01:34.186Z">>,
                nullable => true
            })},
        {created_at, hoconsc:mk(emqx_schema:rfc3339_system_time(),
            #{desc => "ApiKey create datetime",
                example => <<"2021-12-01T00:00:00.000Z">>
            })},
        {desc, hoconsc:mk(emqx_schema:unicode_binary(),
            #{example => <<"Note">>, nullable => true})},
        {enable, hoconsc:mk(boolean(), #{desc => "Enable/Disable", nullable => true})}
    ];
fields(name) ->
    [{name, hoconsc:mk(binary(),
        #{
            desc => <<"[a-zA-Z0-9-_]">>,
            example => <<"EMQX-API-KEY-1">>,
            in => path,
            validator => fun ?MODULE:validate_name/1
        })}
    ].

-define(NAME_RE, "^[A-Za-z]+[A-Za-z0-9-_]*$").

validate_name(Name) ->
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, ?NAME_RE) of
                nomatch -> {error, "Name should be " ?NAME_RE};
                _ -> ok
            end;
        false -> {error, "Name Length must =< 256"}
    end.

delete(Keys, Fields) ->
    lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end, Fields, Keys).

api_key(get, _) ->
    {200, [format(App) || App <- emqx_mgmt_auth:list()]};
api_key(post, #{body := App}) ->
    #{
        <<"name">> := Name,
        <<"desc">> := Desc0,
        <<"expired_at">> := ExpiredAt,
        <<"enable">> := Enable
    } = App,
    Desc = unicode:characters_to_binary(Desc0, unicode),
    case emqx_mgmt_auth:create(Name, Enable, ExpiredAt, Desc) of
        {ok, NewApp} -> {200, format(NewApp)};
        {error, Reason} -> {400, Reason}
    end.

api_key_by_name(get, #{bindings := #{name := Name}}) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, App} -> {200, format(App)};
        {error, not_found} -> {404, <<"NOT_FOUND">>}
    end;
api_key_by_name(delete, #{bindings := #{name := Name}}) ->
    case emqx_mgmt_auth:delete(Name) of
        {ok, _} -> {204};
        {error, not_found} -> {404, <<"NOT_FOUND">>}
    end;
api_key_by_name(put, #{bindings := #{name := Name}, body := Body}) ->
    Enable = maps:get(<<"enable">>, Body, undefined),
    ExpiredAt = maps:get(<<"expired_at">>, Body, undefined),
    Desc = maps:get(<<"desc">>, Body, undefined),
    case emqx_mgmt_auth:update(Name, Enable, ExpiredAt, Desc) of
        {ok, App} -> {200, format(App)};
        {error, not_found} -> {404, <<"NOT_FOUND">>}
    end.

format(App = #{expired_at := ExpiredAt, created_at := CreateAt}) ->
    App#{
        expired_at => list_to_binary(calendar:system_time_to_rfc3339(ExpiredAt)),
        created_at => list_to_binary(calendar:system_time_to_rfc3339(CreateAt))
    }.
