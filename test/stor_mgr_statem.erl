-module(stor_mgr_statem).
-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").

-import(helpers, [start_yawndb/1, stop_yawndb/0]).

-export([initial_state/0, command/1, prop_main/0, postcondition/3,
         precondition/2, next_state/3]).

%% This should be modified if yawndb.yml changes
-define(VALID_PREFIXES, ["stats", "load", "response"]).
-define(INVALID_PREFIXES, ["fake", "and", "wrong"]).
-define(PREFIXES, ?INVALID_PREFIXES++?VALID_PREFIXES).

-define(SERVER, yawndb_stor_mgr).

-record(state, {paths = []}).


initial_state() ->
    #state{}.

command(S) ->
    frequency(
      [
       {2, {call, ?SERVER, new_path, [path()]}},
       {2, {call, ?SERVER, get_paths, []}},
       {2, {call, ?SERVER, get_status, []}},
       {1, {call, ?SERVER, delete_path, [path()]}},
       {1, {call, ?SERVER, delete_path, [path(S#state.paths)]}}
]).

next_state(S, _, {call, _, new_path, [Path]}) ->
    case is_valid_new_path(Path, S#state.paths) of
        false -> S;
        true ->
            S#state{paths = [Path|S#state.paths]}
    end;
next_state(S, _, {call, _, delete_path, [Path]}) ->
    S#state{paths = lists:delete(Path, S#state.paths)};
next_state(State, _, _) ->
    % Assume that other commands does not change state
    State.

postcondition(S, {call, _, new_path, [Path]}, Result) ->
    case {is_valid_new_path(Path, S#state.paths), Result} of
        {true, {ok, _}} -> true;
        {false, {error, _}} -> true;
        _ -> false
    end;
postcondition(S, {call, _, delete_path, [Path]}, Result) ->
    case {lists:member(Path, S#state.paths), Result} of
        {true, ok} -> true;
        {false, failed} -> true;
        _ -> false
    end;
postcondition(S, {call, _, get_paths, []}, Result) ->
    {ok, Paths} = Result,
    ordsets:from_list(S#state.paths) ==
        ordsets:from_list(Paths);
postcondition(S, {call, _, get_status, []}, Result) ->
    % Actually, paths_count is the only variable in status
    % that we can check.
    {ok, {Vars}} = Result,
    Count = length(S#state.paths),
    proplists:get_value(<<"paths_count">>, Vars) == 
        Count;
postcondition(_, _, _) ->
    true.

precondition(_, _) ->
    true.

%% Properties

path() ->
    ?LET(Pref, elements(?PREFIXES),
         ?LET(Tail, path_tail(2, 32),
              list_to_binary([Pref, "-", Tail]))).
path([]) ->
    path();
path(Paths) ->
    elements(Paths).

path_tail(LengthMin, LengthMax) ->
    ?LET(L, integer(LengthMin, LengthMax),
         vector(L, choose($a, $z))).

%% Helper functions

is_valid_new_path(Path, Paths) ->
    (not lists:member(Path, Paths))
        andalso
        lists:member(
          binary_to_list(yawndb_stor_path:get_prefix(Path)),
          ?VALID_PREFIXES).

prop_main() ->
    ?FORALL(Cmds, commands(?MODULE),
      ?TRAPEXIT(
         begin
             application:start(yawndb),
             {History,State,Result} = run_commands(?MODULE, Cmds),
             application:stop(yawndb),
             timer:sleep(100),
             ?WHENFAIL(
                proper_report:report(Cmds, History, State, Result),
                aggregate(command_names(Cmds), Result =:= ok))
         end)).
