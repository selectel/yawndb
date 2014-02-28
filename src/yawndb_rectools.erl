-module(yawndb_rectools).
-include_lib("yawndb.hrl").
-compile({parse_transform, exprecs}).
-export([f1/0, f2/0, f3/0]).

-export_records([rule, conveyor, p_conveyor]).

f1() ->
    {new,'#new-rule'([])}.

f2() ->
    {new,'#new-conveyor'([])}.

f3() ->
    {new, '#new-p_conveyor'([])}.
