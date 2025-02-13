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

-module(emqx_limiter_container).

%% API
-export([create_by_names/3, create/1, check/2]).

-type index() :: pos_integer().

%% @doc
%% a container is a collection of groups of limiters.
%% Each group
%% * has a name
%% * contains a list of `emqx_limiter` implementations.
%%
%% We frequently update the limiters belonging to different groups in the container.
%% That is why we use double indexing: we keep each limiter under an individual index key.
%% This allows to avoid data churn on constant recreation of limiter lists.
-type container() ::
    undefined
    | #{
        emqx_limiter:limiter_name() => [index()],
        index() => emqx_limiter:limiter()
    }.

-export_type([container/0]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------

-spec create_by_names(
    [emqx_limiter:limiter_name()],
    _Config :: emqx_maybe:t(map()),
    emqx_limiter:zone()
) -> container().
create_by_names(Names, undefined, Zone) ->
    create_by_names(Names, #{}, Zone);
create_by_names(Names, Cfg, Zone) ->
    do_create_by_names(Names, Cfg, Zone, []).

-spec create([{emqx_limiter:limiter_name(), [emqx_limiter:limiter()]}]) -> container().
create(Groups) ->
    do_create(Groups, 1, #{}).

-spec check([{emqx_limiter:limiter_name(), non_neg_integer()}], container()) ->
    {boolean(), container()}.
check(_Needs, undefined) ->
    {true, undefined};
check(Needs, Container) ->
    do_check(Needs, Container, []).

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------

%% @doc
%% For each limiter name, we try to create
%% * a private limiter (belonging to the current process only) from the given config
%% * a shared limiter from the zone config
do_create_by_names([Name | Names], Cfg, Zone, Acc) ->
    Private =
        case emqx_limiter:get_config(Name, Cfg) of
            undefined ->
                [];
            LimiterCfg ->
                [emqx_limiter_private:create(LimiterCfg)]
        end,
    Limiters =
        case emqx_limiter_bucket_registry:find_bucket(Zone, Name) of
            {ok, Ref} ->
                [emqx_limiter_shared:create(Ref) | Private];
            undefined ->
                Private
        end,
    do_create_by_names(Names, Cfg, Zone, [{Name, Limiters} | Acc]);
do_create_by_names([], _Cfg, _Zone, Acc) ->
    create(Acc).

do_create([{Name, Limiters} | Groups], Seq, Acc) ->
    do_assign_limiters(Limiters, Name, Groups, Seq, [], Acc);
do_create([], 1, _Acc) ->
    undefined;
do_create([], _Seq, Acc) ->
    Acc.

do_assign_limiters([Limiter | Limiters], Name, Groups, Seq, Indexes, Acc) ->
    do_assign_limiters(Limiters, Name, Groups, Seq + 1, [Seq | Indexes], Acc#{Seq => Limiter});
do_assign_limiters([], Name, Groups, Seq, Indexes, Acc) ->
    do_create(Groups, Seq, Acc#{Name => Indexes}).

do_check([{Name, Need} | Needs], Container0, Consumed0) ->
    case maps:find(Name, Container0) of
        error ->
            do_check(Needs, Container0, Consumed0);
        {ok, Indexes} ->
            case do_check_limiters(Need, Indexes, Container0, Consumed0) of
                {true, Container, Consumed} ->
                    do_check(Needs, Container, Consumed);
                {false, Container, Consumed} ->
                    {false, do_restore(Consumed, Container)}
            end
    end;
do_check([], Container, _Consumed) ->
    {true, Container}.

do_check_limiters(Need, [Index | Indexes], Container, Consumed) ->
    Limiter0 = maps:get(Index, Container),
    case emqx_limiter:check(Need, Limiter0) of
        {true, Limiter} ->
            do_check_limiters(
                Need,
                Indexes,
                Container#{Index := Limiter},
                [{Index, Need} | Consumed]
            );
        {false, Limiter} ->
            {false, Container#{Index := Limiter}, Consumed}
    end;
do_check_limiters(_, [], Container, Consumed) ->
    {true, Container, Consumed}.

do_restore([{Index, Need} | ConsumedRest], Container) ->
    Limiter0 = maps:get(Index, Container),
    Limiter = emqx_limiter:restore(Need, Limiter0),
    do_restore(ConsumedRest, Container#{Index := Limiter});
do_restore([], Container) ->
    Container.
