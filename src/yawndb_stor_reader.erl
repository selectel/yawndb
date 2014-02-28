%%% @doc reads YAWNDB data from disk - at least part of them
%%       so we can make sure that path that we are creating
%%       isn't present yet
-module(yawndb_stor_reader).
-behaviour(gen_server).
-include_lib("stdlib/include/qlc.hrl").

-include("yawndb.hrl").

%% API
-export([start_link/0,
         check_data/1,
         get_paths/0]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type bitcask_ref() :: any().

-record(state, {bitcask_ref :: bitcask_ref()}).

%%% API
-spec start_link() -> term().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec check_data(binary()) -> {has_data, [binary()]} | no_data.
check_data(Path) ->
    gen_server:call(?SERVER, {check_data, Path}, infinity).

-spec get_paths() -> [binary()].
get_paths() ->
    gen_server:call(?SERVER, get_paths, infinity).

%%% gen_server callbacks
init([]) ->
    {ok, DataDir} = application:get_env(flush_dir),
    BitcaskRef = bitcask:open(DataDir, []),
    {ok, #state{bitcask_ref = BitcaskRef}}.

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
handle_call(Request, _From, State) ->
    lager:warning("unhandled request: ~p", [Request]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:warning("unhandled message: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:warning("unhandled info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions
