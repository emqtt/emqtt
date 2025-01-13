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
create_by_names(Names, Cfg, Zone) ->
    do_create_by_names(Names, Cfg, Zone, []).

-spec create([{emqx_limiter:limter_name(), [emqx_limiter:limiter()]}]) -> container().
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
do_create_by_names([Name | Names], Cfg, Zone, Acc) ->
    Private =
        case emqx_limiter:get_cfg(Name, Cfg) of
            undefined ->
                [];
            LimiterCfg ->
                [emqx_limiter_private:create(LimiterCfg)]
        end,
    Limiters =
        case emqx_limiter_manager:find_bucket(Zone, Name) of
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

do_check([{Name, Need} | Needs], Container, Acc) ->
    case maps:find(Name, Container) of
        error ->
            do_check(Needs, Container, Acc);
        {ok, Indexes} ->
            case do_check_limiters(Need, Indexes, Container, Acc) of
                {true, Container2, Acc2} ->
                    do_check(Needs, Container2, Acc2);
                {false, Container2, Acc2} ->
                    {false, do_restore(Acc2, Container2)}
            end
    end;
do_check([], Container, _Acc) ->
    {true, Container}.

do_check_limiters(Need, [Index | Indexes], Container, Acc) ->
    Limiter = maps:get(Index, Container),
    case emqx_limiter:check(Need, Limiter) of
        {true, Limiter2} ->
            do_check_limiters(
                Need,
                Indexes,
                Container#{Index := Limiter2},
                [{Index, Need} | Acc]
            );
        {false, Limiter2} ->
            {false, Container#{Index := Limiter2}, Acc}
    end;
do_check_limiters(_, [], Container, Acc) ->
    {true, Container, Acc}.

do_restore([{Index, Consumed} | Indexes], Container) ->
    Limiter = maps:get(Index, Container),
    Limiter2 = emqx_limiter:restore(Limiter, Consumed),
    do_restore(Indexes, Container#{Index := Limiter2});
do_restore([], Container) ->
    Container.
