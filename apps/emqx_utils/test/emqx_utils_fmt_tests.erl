%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_fmt_tests).

-include_lib("eunit/include/eunit.hrl").

mixed_table_test() ->
    ?assertEqual(
        ".---------.---------.----------------.\n"
        ": 1st     : 2nd     : Third [number] :\n"
        ":---------:---------:----------------:\n"
        ": Apple   : Cider   :            4.5 :\n"
        ": Reddish : Ale     :           6ish :\n"
        ": España  : Cerveza :              4 :\n"
        "`---------`---------`----------------`\n",
        unicode:characters_to_list(
            emqx_utils_fmt:table(["1st", "2nd", "Third [number]"], [
                ["Apple", "Cider", 4.5],
                ["Reddish", "Ale", '6ish'],
                ["España", "Cerveza", 4]
            ])
        )
    ).

subrows_table_test() ->
    ?assertEqual(
        ".---------.---------.-------.--------.\n"
        ": 1st     : 2nd     : Third : Fourth :\n"
        ":---------:---------:-------:--------:\n"
        ": Apple   : Cider   : -     :      - :\n"
        ": Reddish : Ale     : I     :      6 :\n"
        ":         :         : II    :    6.7 :\n"
        ": España  : Cerveza : I     :      3 :\n"
        ":         :         : Neo   :    3.1 :\n"
        ":         :         :       :    3.2 :\n"
        "`---------`---------`-------`--------`\n",
        unicode:characters_to_list(
            emqx_utils_fmt:table(["1st", "2nd", "Third", "Fourth"], [
                ["Apple", "Cider", {subrows, [[]]}],
                ["Reddish", "Ale", {subrows, [["I", 6], ["II", 6.7]]}],
                ["España", "Cerveza", {subrows, [["I", 3], ["Neo", {subcolumns, [[3.1, 3.2]]}]]}]
            ])
        )
    ).

subgroup_table_test() ->
    ?assertEqual(
        ".---------.---------.-------.--------.\n"
        ": 1st     : 2nd     : Third : Fourth :\n"
        ":---------:---------:-------:--------:\n"
        ":-Apple---:-Cider---:-------:--------:\n"
        ":         :         : -     :      - :\n"
        ":-Reddish-:-Ale-----:-------:--------:\n"
        ":         :         : I     :    6.1 :\n"
        ":         :         : II    :    6.7 :\n"
        ":-España--:---------:-------:--------:\n"
        ":         :-Cerveza-:-------:--------:\n"
        ":         :         : I     :      3 :\n"
        ":         :         : Neo   :    3.1 :\n"
        ":         :         :       :    3.2 :\n"
        "`---------`---------`-------`--------`\n",
        unicode:characters_to_list(
            emqx_utils_fmt:table(["1st", "2nd", "Third", "Fourth"], [
                ["Apple", "Cider", {group, [[]]}],
                ["Reddish", "Ale", {group, [{subcolumns, [["I", "II"], [6.1, 6.7]]}]}],
                [
                    "España",
                    {group, [
                        "Cerveza",
                        {group, [{subrows, [["I", 3], ["Neo", {subcolumns, [[3.1, 3.2]]}]]}]}
                    ]}
                ]
            ])
        )
    ).
