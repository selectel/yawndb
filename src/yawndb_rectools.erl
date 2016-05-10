-module(yawndb_rectools).
-include_lib("yawndb.hrl").
-compile({parse_transform, exprecs}).

-export_records([rule, conveyor, p_conveyor]).

