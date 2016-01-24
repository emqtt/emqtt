%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc Internal Header File
%%%
%%%-----------------------------------------------------------------------------

-define(GPROC_POOL(JoinOrLeave, Pool, I),
        (begin
            case JoinOrLeave of
                join  -> gproc_pool:connect_worker(Pool, {Pool, Id});
                leave -> gproc_pool:disconnect_worker(Pool, {Pool, I})
            end
        end)).

-define(record_to_proplist(Def, Rec),
        lists:zip(record_info(fields, Def),
                  tl(tuple_to_list(Rec)))).

-define(record_to_proplist(Def, Rec, Fields),
    [{K, V} || {K, V} <- ?record_to_proplist(Def, Rec),
                         lists:member(K, Fields)]).

-define(UNEXPECTED_REQ(Req, State),
        (begin
            lager:error("Unexpected Request: ~p", [Req]),
            {reply, {error, unexpected_request}, State}
        end)).

-define(UNEXPECTED_MSG(Msg, State),
        (begin
            lager:error("Unexpected Message: ~p", [Msg]),
            {noreply, State}
        end)).

-define(UNEXPECTED_INFO(Info, State),
        (begin
            lager:error("Unexpected Info: ~p", [Info]),
            {noreply, State}
        end)).

