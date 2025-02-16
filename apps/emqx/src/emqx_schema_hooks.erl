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

-module(emqx_schema_hooks).

-type hookpoint() :: atom().

-callback injected_fields() ->
    #{hookpoint() => [hocon_schema:field()]}.
-callback injected_fields(term()) ->
    #{hookpoint() => [hocon_schema:field()]}.
-callback injected_values() ->
    #{hookpoint() => term()}.
-callback injected_values(term()) ->
    #{hookpoint() => term()}.
-optional_callbacks([injected_fields/0, injected_fields/1, injected_values/0, injected_values/1]).

-export_type([hookpoint/0]).

-define(HOOKPOINT_APPEND_PT_KEY(POINT_NAME), {?MODULE, fields, POINT_NAME}).
-define(HOOKPOINT_SET_PT_KEY(POINT_NAME), {?MODULE, value, POINT_NAME}).

-export([
    list_injection_point/1,
    list_injection_point/2,
    value_injection_point/1,
    value_injection_point/2,
    inject_from_modules/1
]).

%% for tests
-export([
    erase_injections/0,
    any_injections/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec list_injection_point(hookpoint()) -> [hocon_schema:field()].
list_injection_point(PointName) ->
    list_injection_point(PointName, []).

-spec list_injection_point(hookpoint(), [hocon_schema:field()]) -> [hocon_schema:field()].
list_injection_point(PointName, Default) ->
    persistent_term:get(?HOOKPOINT_APPEND_PT_KEY(PointName), Default).

-spec value_injection_point(hookpoint()) -> {ok, term()} | error.
value_injection_point(PointName) ->
    try persistent_term:get(?HOOKPOINT_SET_PT_KEY(PointName)) of
        Val -> {ok, Val}
    catch
        error:badarg -> error
    end.

-spec value_injection_point(hookpoint(), term()) -> term().
value_injection_point(PointName, Default) ->
    persistent_term:get(?HOOKPOINT_SET_PT_KEY(PointName), Default).

-spec erase_injections() -> ok.
erase_injections() ->
    lists:foreach(
        fun
            ({?HOOKPOINT_APPEND_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            ({?HOOKPOINT_SET_PT_KEY(_) = Key, _}) ->
                persistent_term:erase(Key);
            (_) ->
                ok
        end,
        persistent_term:get()
    ).

-spec any_injections() -> boolean().
any_injections() ->
    lists:any(
        fun
            ({?HOOKPOINT_APPEND_PT_KEY(_), _}) ->
                true;
            ({?HOOKPOINT_SET_PT_KEY(_), _}) ->
                true;
            (_) ->
                false
        end,
        persistent_term:get()
    ).

-spec inject_from_modules([module() | {module(), term()}]) -> ok.
inject_from_modules(Modules) ->
    ListInjections =
        lists:foldl(
            fun append_module_list_injections/2,
            #{},
            Modules
        ),
    ok = inject_list_fields(maps:to_list(ListInjections)),
    ValueInjections =
        lists:foldl(
            fun append_module_value_injections/2,
            #{},
            Modules
        ),
    ok = inject_value_fields(ValueInjections).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

append_module_list_injections(Module, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_fields, [], #{}),
    append_module_list_injections(Injections, AllInjections);
append_module_list_injections({Module, Options}, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_fields, [Options], #{}),
    append_module_list_injections(Injections, AllInjections);
append_module_list_injections(ModuleInjections, AllInjections) when is_map(ModuleInjections) ->
    maps:fold(
        fun(PointName, Fields, Acc) ->
            maps:update_with(
                PointName,
                fun(Fields0) ->
                    Fields0 ++ Fields
                end,
                Fields,
                Acc
            )
        end,
        AllInjections,
        ModuleInjections
    ).

append_module_value_injections(Module, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_values, [], #{}),
    append_module_value_injections(Injections, AllInjections);
append_module_value_injections({Module, Options}, AllInjections) when is_atom(Module) ->
    Injections = call_if_defined(Module, injected_values, [Options], #{}),
    append_module_value_injections(Injections, AllInjections);
append_module_value_injections(#{} = Injections, AllInjections) ->
    maps:merge(AllInjections, Injections).

inject_list_fields([]) ->
    ok;
inject_list_fields([{PointName, Fields} | Rest]) ->
    case any_list_injections(PointName) of
        true ->
            inject_list_fields(Rest);
        false ->
            ok = inject_list_fields(PointName, Fields),
            inject_list_fields(Rest)
    end.

inject_list_fields(PointName, Fields) ->
    Key = ?HOOKPOINT_APPEND_PT_KEY(PointName),
    persistent_term:put(Key, Fields).

inject_value(PointName, Value) ->
    Key = ?HOOKPOINT_SET_PT_KEY(PointName),
    persistent_term:put(Key, Value).

any_list_injections(PointName) ->
    persistent_term:get(?HOOKPOINT_APPEND_PT_KEY(PointName), undefined) =/= undefined.

inject_value_fields(Injections) ->
    maps:foreach(fun inject_value/2, Injections).

call_if_defined(Module, Function, Args, Default) ->
    %% Ensure module is loaded, especially when called from nodetool
    _ = Module:module_info(),
    case erlang:function_exported(Module, Function, length(Args)) of
        true ->
            apply(Module, Function, Args);
        false ->
            Default
    end.
