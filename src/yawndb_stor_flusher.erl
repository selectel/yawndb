%%% @doc flushes YAWNDB data to disk
-module(yawndb_stor_flusher).
-behaviour(gen_server).
-include_lib("stdlib/include/qlc.hrl").

-include("yawndb.hrl").

%% API
-export([start_link/0,
         prepare_shutdown/0,
         start_flushing/0,
         check_data/1,
         get_paths/0,
         delete_path/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type bitcask_ref() :: any().

-record(state, {enabled          :: boolean(),
                flush_period     :: pos_integer(),
                flush_thresold   :: pos_integer(),
                shutting_down    :: boolean(),
                dump_started     :: any(),
                bitcask_ref      :: bitcask_ref()}).

%%% API
-spec start_link() -> term().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec start_flushing() -> ok.
start_flushing() ->
    {ok, FlushPeriod} = application:get_env(flush_period),
    {ok, Enabled} = application:get_env(flush_enabled),
    ets:insert(flusher_started, {started, true}),
    case Enabled of
        true  -> erlang:send_after(FlushPeriod, ?SERVER, start_flush), ok;
        false -> ok
    end.

-spec prepare_shutdown() -> done.
prepare_shutdown() ->
    gen_server:call(?SERVER, prepare_shutdown, infinity).

-spec check_data(binary()) -> {has_data, [binary()]} | no_data.
check_data(Path) ->
    gen_server:call(?SERVER, {check_data, Path}, infinity).

-spec get_paths() -> [binary()].
get_paths() ->
    gen_server:call(?SERVER, get_paths, infinity).

-spec delete_path(binary()) -> ok.
delete_path(Path) ->
    gen_server:cast(?SERVER, {delete_path, Path}).

%%% gen_server callbacks
init([]) ->
    {ok, Enabled} = application:get_env(flush_enabled),
    {ok, FlushPeriodS} = application:get_env(flush_period),
    FlushPeriod = FlushPeriodS * 1000,
    {ok, FlushLateThresold} = application:get_env(flush_late_threshold),
    case catch ets:new(flusher_started, [named_table, public]) of
        {'EXIT', {badarg, _}} ->
            [{started, Started}] = ets:lookup(flusher_started, started),
            case Started of
                true ->
                    lager:info("restarting flushing process after "
                               "flusher revival"),
                    erlang:send_after(FlushPeriod, ?SERVER, start_flush);
                _    -> ok
            end;
        _ -> ets:give_away(flusher_started, whereis(yawndb_dummy), ok)
    end,

    {ok, DataDir} = application:get_env(flush_dir),
    BitcaskRef = bitcask:open(DataDir, [read_write]),

    {ok, #state{enabled          = Enabled,
                flush_period     = FlushPeriod,
                flush_thresold   = FlushLateThresold,
                shutting_down    = false,
                bitcask_ref      = BitcaskRef}}.

handle_call(get_paths, _From, #state{bitcask_ref=BitcaskRef}=State) ->
    Paths = bitcask:list_keys(BitcaskRef),
    {reply, Paths, State};
handle_call({check_data, Path}, _From,
            #state{bitcask_ref=BitcaskRef}=State) ->
    Reply = case bitcask:get(BitcaskRef, Path) of
                {ok, Data} -> {has_data, binary_to_term(Data)};
                not_found  -> no_data
            end,
    {reply, Reply, State};
handle_call(Req, _From, #state{enabled=false}=State) ->
    lager:info("disabled, got req ~p, ignoring", [Req]),
    {reply, ok, State};
handle_call(prepare_shutdown, _From, #state{bitcask_ref=BitcaskRef}=State) ->
    Start = erlang:now(),
    lager:info("starting dump before shutdown"),
    DirtyPids = get_dirty_pids(),
    dump_all(BitcaskRef, DirtyPids),
    Diff = timer:now_diff(erlang:now(), Start) / 1000000,
    lager:info("dump finished, ~.2f s", [Diff]),
    {reply, done, State#state{shutting_down=true}};
handle_call(Request, _From, State) ->
    lager:warning("unhandled request: ~p", [Request]),
    {reply, ok, State}.

handle_cast({delete_path, Path}, #state{bitcask_ref=BitcaskRef}=State) ->
    case bitcask:get(BitcaskRef, Path) of
        {ok, _} -> bitcask:delete(BitcaskRef, Path);
        not_found -> ok
    end,
    case yawndb_stor_mgr:kill_path_handler(Path) of
        ok -> lager:info("path handler ~p killed fine", [Path]), ok;
        {error, path_not_found} -> ok;
        Other -> lager:warning("path handler killing failed ~p", [Other])
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    lager:warning("unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, #state{enabled=false}=State) ->
    lager:info("flusher disabled, got info ~p, ignoring", [Info]),
    {noreply, State};
handle_info(start_flush, #state{shutting_down=false,
                                bitcask_ref=BitcaskRef}=State) ->
    DumpStarted = erlang:now(),
    lager:info("starting dump"),
    DirtyPids = get_dirty_pids(),
    dump_all(BitcaskRef, DirtyPids),
    Diff = timer:now_diff(erlang:now(), DumpStarted) / 1000000,
    ToNextFlush = max(State#state.flush_period - Diff * 1000, 0),
    lager:info("dump finished, ~.2f s, next in ~.2f s",
               [Diff, ToNextFlush / 1000]),
    case Diff >= (State#state.flush_thresold
                  * State#state.flush_period) of
        true -> lager:warning("dump took too much time!");
        false -> ok
    end,
    erlang:send_after(round(ToNextFlush), self(), start_flush),
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions
-spec dump_all(bitcask_ref(), [pid()]) -> ok.
dump_all(BitcaskRef, DirtyPids) ->
    {ok, DataDir} = application:get_env(flush_dir),

    lager:info("dirty paths count: ~p", [length(DirtyPids)]),
    [dump(BitcaskRef, Pid) || Pid <- DirtyPids],
    lager:info("syncing..."),
    bitcask:sync(BitcaskRef),
    case bitcask:needs_merge(BitcaskRef) of
        {true, Files} ->
            lager:info("merging..."),
            bitcask_merge_worker:merge(DataDir, [], Files);
        false ->
            ok
    end.

-spec dump(bitcask_ref(), pid()) -> ok.
dump(BitcaskRef, Pid) ->
    {gproc, GprocInfo} = gproc:info(Pid, gproc),
    [Path] = [X || {{n, l, {path, X}}, undefined} <- GprocInfo],
    try
        {ok, {Ref, Binaries}} = yawndb_stor_path:persist_conveyors(Pid),
        bitcask:put(BitcaskRef, Path, term_to_binary(Binaries)),
        yawndb_stor_path:ack_persistence(Pid, Ref)
    catch
        exit:{timeout, {gen_server, call, [_, persist_conveyors, _]}} ->
            lager:error("timeout when trying to persist conveyours "
                        "from path ~s", [Path])
    end,
    ok.

-spec get_dirty_pids() -> [pid()].
get_dirty_pids() ->
    Tbl = gproc:table(props),
    Q = qlc:q([{Path, Pid} || {{_, _, {dirty, Path}}, Pid, true} <- Tbl]),
    PathPids = qlc:eval(Q),
    SortedPathPids = lists:sort(fun ({Path1, _}, {Path2, _}) ->
                                        Path1 =< Path2 end,
                                PathPids),
    [P || {_, P} <- SortedPathPids].
