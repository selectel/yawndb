%%% @doc Interface module for YAWNDB storage
-module(yawndb_stor_mgr).
-behaviour(gen_server).
-include_lib("stdlib/include/qlc.hrl").
-include_lib("yawndb.hrl").

-define(MAX_PATH_SIZE, 1024).

%% API
-export([start_link/0]).
-export([write/3, slice/4, last/3, new_path/3, new_path/1,
         delete_path/1, kill_path_handler/1,
         get_paths/0, get_rules/1, get_status/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([rule_name/0, path/0,
              timestamp/0, pathpid/0]).

-define(SERVER, ?MODULE).
-record(state, {}).

%%% API
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%% @doc Writes path-time-value triplet to storage
-spec write(path(), timestamp(), {atom(), pos_integer()}) -> ok | badarg.
write(Path, Time, Value) ->
    try
        case (size(Path) =< ?MAX_PATH_SIZE andalso
              binary:match(Path, <<"/">>) =:= nomatch) of
            false ->
                lager:notice("got bad path to write: ~p", [Path]),
                throw({error, badarg});
            true -> ok
        end,

        case gproc:where({n, l, {path, Path}}) of
            undefined ->
                new_path(Path, Time, Value);
            PathPid ->
                yawndb_stor_path:write(PathPid, Time, Value)
        end
    catch
        throw:{error, _}=Err ->
            lager:notice("caught an error: ~p", [Err]),
            Err
    end.

%% @doc Returns data for schema/path/from/to values
-spec slice(rule_name() | binary(), path(),
           timestamp(), timestamp()) -> {ok, [{timestamp(), integer()}]} |
                                        {error, path_not_found} |
                                        {error, slice_too_big} |
                                        {error, rule_not_found}.
slice(RuleNameBin, P, F, T) when is_binary(RuleNameBin) ->
    case catch erlang:binary_to_existing_atom(RuleNameBin, latin1) of
        {'EXIT', {badarg, _}} -> {error, rule_not_found};
        RuleName -> slice(RuleName, P, F, T)
    end;
slice(RuleName, Path, FromTS, ToTS) when is_atom(RuleName) ->
    case gproc:where({n, l, {path, Path}}) of
        undefined -> {error, path_not_found};
        PathPid ->
            yawndb_stor_path:slice(PathPid, RuleName, FromTS, ToTS)
    end.

%% @doc Returns last N elements of data
-spec last(rule_name() | binary(), path(),
           pos_integer()) -> {ok, [{timestamp(), integer()}]} |
                             {error, path_not_found} |
                             {error, slice_too_big} |
                             {error, rule_not_found}.
last(RuleNameBin, P, N) when is_binary(RuleNameBin) ->
    case catch erlang:binary_to_existing_atom(RuleNameBin, latin1) of
        {'EXIT', {badarg, _}} -> {error, rule_not_found};
        RuleName -> last(RuleName, P, N)
    end;
last(RuleName, Path, N) when is_atom(RuleName) ->
    case gproc:where({n, l, {path, Path}}) of
        undefined -> {error, path_not_found};
        PathPid ->
            yawndb_stor_path:last(PathPid, RuleName, N)
    end.


%% @doc Creates new path and initializes it with given data
%% we can't parallelize this procedure because of race conditions on
%% creation of path - what will happen if 3 users want to create new
%% path handler simultaneously?
-spec new_path(path(),
               timestamp() | none,
               pos_integer() | none) -> {ok, pid()} | {error, atom()}.
new_path(Path, Time, Value) ->
    ProcsNow = erlang:system_info(process_count),
    ProcsMax = erlang:system_info(process_limit),
    case (ProcsNow / ProcsMax) < 0.9 of
        true ->
            case true of
                true ->
                    gen_server:call(?SERVER, {new_path, Path, Time, Value},
                                    20000);
                false ->
                    {error, memory_limit}
            end;
        false ->
            lager:error("Process limit is too close, "
                        "new path wasn't created; now: ~p max: ~p",
                        [ProcsNow, ProcsMax]),
            {error, process_limit}
    end.

%% @doc Just creates new path
-spec new_path(path()) -> {ok, pid()} | {error, atom()}.
new_path(Path) -> new_path(Path, none, none).

%% @doc Deletes path. Work is mostly done asynchroniously.
-spec delete_path(path()) -> ok | {error, atom()}.
delete_path(Path) ->
    gen_server:call(?SERVER, {delete_path, Path}, 10000).

-spec kill_path_handler(path()) -> ok | {error, path_not_found}.
kill_path_handler(Path) ->
    case gproc:where({n, l, {path, Path}}) of
        undefined ->
            gen_server:cast(?SERVER, {path_delete_failed, Path}),
            {error, path_not_found};
        PathPid ->
            supervisor:terminate_child(yawndb_stor_pathsup, PathPid),
            % Race condition could appear here! gproc path record can
            % be removed LONG TIME AFTER path procces dies, so we could get
            % not yet removed record when doing get_paths/0.
            gen_server:cast(?SERVER, {path_deleted, Path})
    end.

%% @doc Returns all registered pthes
-spec get_paths() -> {ok, [path()]}.
get_paths() ->
    gen_server:call(?SERVER, {get_paths}).

%% @doc Returns current status of storage subsystem
-spec get_status() -> {ok, {[{binary(), binary() | integer()}]}}.
get_status() ->
    ReadRpm = gproc:lookup_local_aggr_counter(read_rpm),
    WriteRpm = gproc:lookup_local_aggr_counter(write_rpm),
    ProcsNow = erlang:system_info(process_count),
    ProcsMax = erlang:system_info(process_limit),
    PathsCount = gproc:lookup_local_aggr_counter(path_counter),
    DirtyPathsCount = gproc:lookup_local_aggr_counter(dirty_counter),
    Result = [{<<"read_rpm">>, ReadRpm},
              {<<"write_rpm">>, WriteRpm},
              {<<"read_rps">>, round(ReadRpm / 60)},
              {<<"write_rps">>, round(WriteRpm / 60)},
              {<<"processes_now">>, ProcsNow},
              {<<"processes_max">>, ProcsMax},
              {<<"paths_count">>, PathsCount},
              {<<"dirty_paths_count">>, DirtyPathsCount}],
    {ok, {Result}}.

%% @doc Returns all schemas for path
-spec get_rules(path()) -> {ok, [rule_name()]} |
                           {error, path_not_found}.
get_rules(Path) ->
    case gproc:where({n, l, {path, Path}}) of
        undefined -> {error, path_not_found};
        PathPid -> {ok, yawndb_stor_path:get_rules(PathPid)}
    end.

%%% gen_server callbacks
init([]) ->
    gproc:reg({a, l, read_rpm}),
    gproc:reg({a, l, write_rpm}),
    gproc:reg({a, l, path_counter}),
    gproc:reg({a, l, dirty_counter}),
    ets:new(delete_requests,
            [bag, named_table, private]),
    {ok, #state{}}.

handle_call({new_path, Path, Time, Value}, _From, State) ->
    Reply = try
                %% should check this again to prevent race conditions
                orthrow(undefined == gproc:where({n, l, {path, Path}}),
                        process_exists),
                {ok, PathPid} = yawndb_stor_path:new(Path),
                %% check if we need to actually write something
                orthrow(Time /= none andalso Value /= none,
                        {no_time_value, PathPid}),
                yawndb_stor_path:write(PathPid, Time, Value)
            catch
                error:{badmatch, {error, no_prefix}} ->
                    lager:notice("no prefix for path ~p", [Path]),
                    {error, no_prefix};
                error:{badmatch, Error} ->
                    lager:warning("failed to create PathHandler "
                                  "for path ~p: ~p", [Path, Error]),
                    {error, Error};
                throw:process_exists ->
                    lager:warning("process already exists"),
                    {error, process_exists};
                throw:{no_time_value, Pid} ->
                    {ok, Pid}
            end,
    {reply, Reply, State};
handle_call({get_paths}, _From, State) ->
    Q = qlc:q([P || {{_, _, {path, P}}, _, _} <- gproc:table(names)]),
    {reply, {ok, qlc:eval(Q)}, State};
handle_call({delete_path, Path}, From, State) ->
    erlang:send_after(10000, self(), {remove_delete_reqs, Path}),
    ets:insert(delete_requests, {Path, From}),
    yawndb_stor_flusher:delete_path(Path),
    {noreply, State};
handle_call(Request, _From, State) ->
    Reply = ok,
    lager:warning("got unhandled call: ~p", [Request]),
    {reply, Reply, State}.

handle_cast({path_deleted, Path}, State) ->
    answer_to_delete_reqs(Path, ok),
    {noreply, State};
handle_cast({path_delete_failed, Path}, State) ->
    answer_to_delete_reqs(Path, failed),
    {noreply, State};
handle_cast(Msg, State) ->
    lager:warning("got unhandled cast: ~p", [Msg]),
    {noreply, State}.

handle_info({remove_delete_reqs, Path}, State) ->
    ets:delete(delete_requests, Path),
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("got unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions

answer_to_delete_reqs(Path, Result) ->
    [gen_server:reply(Req, Result)
     || {_, Req} <- ets:lookup(delete_requests, Path)],
    ets:delete(delete_requests, Path).

-spec orthrow(boolean(), any()) -> ok.
orthrow(true, _) -> ok;
orthrow(false, X) -> throw(X).
