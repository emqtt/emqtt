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

-module(emqx_utils_fmt).

-compile(export_all).
-compile(nowarn_export_all).

-export([
    table/2,
    table/3
]).

%%

-type header_cell() :: unicode:chardata() | {unicode:chardata(), column_props()}.
-type table_cell() :: table_cell_simple() | table_sub_structure().
-type table_cell_simple() :: unicode:chardata() | atom() | number().
-type table_sub_structure() ::
    {subrows, [[table_cell()]]}
    | {subcolumns, [[table_cell_simple()]]}
    | {group, [[table_cell()]]}.

-type table_props() :: #{_ => _}.
-type column_props() :: #{align => left | right}.
-type cell_props() :: #{padc => char(), delimiter => string(), _ => _}.

-type formatted_cell() :: unicode:chardata() | {unicode:chardata(), cell_props()}.

-define(table_header_props, #{padc => $-, delimiter => "."}).
-define(table_headcut_props, #{padc => $-}).
-define(group_header_props, #{padc => $-}).
-define(table_footer_props, #{padc => $-, delimiter => "`"}).

%%

%% @doc Format a set of rows accompanied by header into a printable, ASCII-delimited
%% table.
-spec table(Header :: [header_cell()], Rows :: [[table_cell()]]) ->
    unicode:chardata().
table(Header, Rows) ->
    table(Header, Rows, #{}).

-spec table(Header :: [header_cell()], Rows :: [[table_cell()]], table_props()) ->
    unicode:chardata().
table(Header, Rows, _Props) ->
    {FmtHeader, Columns0} = table_columns(Header),
    {FmtRows, Columns} = table_rows(Rows, Columns0),
    [
        table_tabulate_row([], Columns, ?table_header_props),
        table_tabulate_row(FmtHeader, Columns, #{}),
        table_tabulate_row([], Columns, ?table_headcut_props),
        [table_tabulate_row(FmtRow, Columns, #{}) || FmtRow <- FmtRows],
        table_tabulate_row([], Columns, ?table_footer_props)
    ].

%%

table_columns(Header) ->
    lists:unzip([table_column(C) || C <- Header]).

table_column({Title, Props}) ->
    Fmt = table_format_title(Title),
    Width = fmt_width(Fmt),
    Column = maps:merge(#{width => 1}, Props),
    {Fmt, table_column_resize(Width, Column)};
table_column(Title) ->
    table_column({Title, #{}}).

table_rows(Rows, Columns) ->
    lists:mapfoldl(fun table_row/2, Columns, Rows).

table_row(CellsIn, ColumnsIn) ->
    case table_row_expand(CellsIn) of
        {_Structure, RowsGroup} ->
            {FmtRows, Columns} = table_rows(RowsGroup, ColumnsIn),
            {{FmtRows}, Columns};
        Cells when is_list(Cells) ->
            lists:unzip(table_cells(Cells, ColumnsIn))
    end.

%% Expand complex row structures into potentially grouped together list of rows,
%% i.e. lists of lists of cells.
table_row_expand([{subrows, SubRowsIn}]) ->
    SubRows = [table_row_expand(SubRow) || SubRow <- SubRowsIn],
    {sub, table_flatten_rows(SubRows)};
table_row_expand([{subcolumns, SubColumns}]) ->
    SubRows = transpose(SubColumns, ""),
    {sub, SubRows};
table_row_expand([{group, Group}]) ->
    GroupRows = table_row_expand(Group),
    GroupHeader = [{repeat, {"", ?group_header_props}}],
    {group, [GroupHeader | table_flatten_rows(GroupRows)]};
table_row_expand([Cell | CellsIn]) when
    not is_tuple(Cell);
    element(1, Cell) =/= subrows,
    element(1, Cell) =/= subcolumns,
    element(1, Cell) =/= group
->
    case table_row_expand(CellsIn) of
        {sub, CellsSub} ->
            {sub, table_cells_expand(Cell, CellsSub)};
        {group, CellsGrouped} ->
            {group, table_cells_group(Cell, CellsGrouped)};
        Cells ->
            [Cell | Cells]
    end;
table_row_expand([]) ->
    [].

table_flatten_rows({_Section, Rows}) ->
    table_flatten_rows(Rows);
table_flatten_rows([{_Section, Rows} | Rest]) ->
    Rows ++ table_flatten_rows(Rest);
table_flatten_rows([Row | Rest]) ->
    [Row | table_flatten_rows(Rest)];
table_flatten_rows([]) ->
    [].

table_cells_group(GroupCell, [GroupHeader | SubRows]) ->
    SubRowsExpanded = [["" | Cells] || Cells <- SubRows],
    [[{GroupCell, ?group_header_props} | GroupHeader] | SubRowsExpanded].

table_cells_expand(LeftCell, [SubRow1 | SubRowsRest]) ->
    SubRow1Expanded = [LeftCell | SubRow1],
    SubRowsExpanded = [["" | SubCells] || SubCells <- SubRowsRest],
    [SubRow1Expanded | SubRowsExpanded];
table_cells_expand(LeftCell, []) ->
    [LeftCell].

%% 1. Formats table cells into text.
%% 2. Expands "repeated cells" into plain list of cells.
%% 3. Updates column props, for example resize and specify alignment if column
%%    cells contain numbers.
table_cells([{repeat, Cell}] = Cells, Columns = [_ | _]) ->
    table_cells([Cell | Cells], Columns);
table_cells([{repeat, _}], []) ->
    [];
table_cells([Cell | Cells], [Column | Columns]) ->
    Fmt = table_format_cell(Cell),
    [{Fmt, table_column_update(Fmt, Column)} | table_cells(Cells, Columns)];
table_cells([], [Column | Columns]) ->
    [{"-", Column} | table_cells([], Columns)];
table_cells([], []) ->
    [].

table_column_update(Fmt, Column0) ->
    Column1 = table_column_resize(fmt_width(Fmt), Column0),
    Column2 = table_column_realign(fmt_align(Fmt), Column1),
    Column2.

table_column_resize(Width, Column = #{width := W0}) ->
    Column#{width := max(Width, W0)}.

table_column_realign(default, Column) ->
    Column;
table_column_realign(left, Column) ->
    Column;
table_column_realign(right, Column) ->
    Column#{align => right}.

table_format_title(Title) ->
    Title.

%% Format cell into text + attach computed props if any.
table_format_cell(A) when is_atom(A) ->
    erlang:atom_to_binary(A);
table_format_cell(N) when is_integer(N) ->
    Fmt = integer_to_binary(N),
    {Fmt, #{align => right}};
table_format_cell(N) when is_number(N) ->
    Fmt = float_to_binary(N, [short]),
    {Fmt, #{align => right}};
table_format_cell({X, Props}) ->
    case table_format_cell(X) of
        {Fmt, CellProps} -> {Fmt, maps:merge(CellProps, Props)};
        Fmt -> {Fmt, Props}
    end;
table_format_cell(Text) ->
    Text.

-spec table_tabulate_row(Row | {[Row]}, [column_props()], cell_props()) ->
    unicode:chardata()
when
    Row :: [formatted_cell()].
table_tabulate_row({Rows}, Columns, Overrides) ->
    [table_tabulate_row(Fmts, Columns, Overrides) || Fmts <- Rows];
table_tabulate_row(Fmts, Columns, Overrides) ->
    Start = table_tabulate_row_start(Columns, Overrides),
    table_tabulate_row_end([Start | table_tabulate_cells(Fmts, Columns, Overrides)]).

table_tabulate_row_start([Column | _], Overrides) ->
    Delim = get_prop(delimiter, Column, Overrides, ":"),
    Delim.

table_tabulate_row_end(Acc) ->
    [Acc, $\n].

table_tabulate_cells([Fmt | Rest], [Column | Columns], Overrides) ->
    PadC = get_fmt_prop(padc, Fmt, Column, Overrides, $\s),
    Padding = get_fmt_prop(padding, Fmt, Column, Overrides, 1),
    Delim = get_prop(delimiter, Column, Overrides, ":"),
    Align = get_fmt_prop(align, Fmt, Column, Overrides, left),
    Width = maps:get(width, Column),
    case Align of
        left -> Dir = trailing;
        right -> Dir = leading
    end,
    Text = fmt_text(Fmt),
    PadS = lists:duplicate(Padding, PadC),
    Line = [PadS, string:pad(Text, Width, Dir, PadC), PadS, Delim],
    Line ++ table_tabulate_cells(Rest, Columns, Overrides);
table_tabulate_cells([], [], _Overrides) ->
    [];
table_tabulate_cells([], Columns, Overrides) ->
    table_tabulate_cells([<<>>], Columns, Overrides).

get_fmt_prop(Name, {_Fmt, Props1}, Props2, Props3, Default) ->
    case Props1 of
        #{Name := V} -> V;
        #{} -> get_prop(Name, Props2, Props3, Default)
    end;
get_fmt_prop(Name, _Fmt, Props2, Props3, Default) ->
    get_prop(Name, Props2, Props3, Default).

get_prop(Name, Props1, Props2, Default) ->
    case Props1 of
        #{Name := V} -> V;
        #{} -> get_prop(Name, Props2, Default)
    end.

get_prop(Name, Props, Default) ->
    maps:get(Name, Props, Default).

fmt_text({Fmt, _Props}) ->
    Fmt;
fmt_text(Fmt) ->
    Fmt.

fmt_align({_Fmt, Props}) ->
    maps:get(align, Props, left);
fmt_align(_Fmt) ->
    left.

fmt_width({Fmt, _Props}) ->
    fmt_width(Fmt);
fmt_width(Fmt) ->
    string:length(Fmt).

%% @doc Transpose list of lists into list of list of their respective elements.
transpose(Ls, Pad) ->
    case lists:any(fun(L) -> L =/= [] end, Ls) of
        true ->
            Heads = [transpose_head(L, Pad) || L <- Ls],
            Tails = [transpose_tail(L) || L <- Ls],
            [Heads | transpose(Tails, Pad)];
        false ->
            []
    end.

transpose_head([H | _], _Pad) -> H;
transpose_head([], Pad) -> Pad.

transpose_tail([_ | T]) -> T;
transpose_tail([]) -> [].
