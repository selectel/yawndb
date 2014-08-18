%%% @doc Path controller for YAWNDB storage
-module(yawndb_stor_path).
-behaviour(gen_server).
-include_lib("yawndb.hrl").

%% API
-export([start_link/1]).
-export([new/1,
         write/3,
         slice/4, last/3,
         persist_conveyors/1, ack_persistence/2,
         get_rules/1,
         get_prefix/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
%% functions for testing
-export([init_conveyor/1, update_conveyor/3, read_conveyor/3]).

-export_type([maybe_value/0]).

-define(SERVER, ?MODULE).
-define(NO_REQ_TIMEOUT, 20*1000).
-define(NO_REQ_COUNT, 3).
-define(PRE_HIBERNATE_TIMEOUT, 60*1000).
-define(M, 1000000).

-type value()       :: {int, ecirca:value()} |
                       {atom, pos_integer()}.
-type maybe_value() :: ecirca:maybe_value().

-record(state, {path           :: path(),
                conveyors      :: [#conveyor{}],
                max_slice      :: pos_integer(),
                read_mavg      :: any(),
                dirty          :: boolean() | {maybe, reference()},
                write_mavg     :: any(),
                no_req_count   :: pos_integer() | pre_hibernate,
                cleanup_period :: pos_integer() }).

%%% API
-spec start_link(path()) -> pathpid().
start_link(Path) ->
    gen_server:start_link(?MODULE, Path, []).

%% @doc This function starts path handler during user's request.
%% It should be fast, so it's asynchronious.
-spec new(path()) -> {ok, pathpid()} | {error, Reason :: any()}.
new(Path) ->
    Prefix = get_prefix(Path),
    case ets:lookup(rules, Prefix) of
        [] -> {error, no_prefix};
        _ ->
            supervisor:start_child(yawndb_stor_pathsup, [Path])
    end.

-spec write(pathpid(), timestamp(), value()) -> ok.
write(Pid, Time, Value) ->
    gen_server:cast(Pid, {write, Time, Value}).

-spec slice(pathpid(), rule_name(),
            timestamp(), timestamp()) -> [{timestamp(), maybe_value()}].
slice(Pid, RuleName, FromTS, ToTS) ->
    gen_server:call(Pid, {slice, RuleName, FromTS, ToTS}).

-spec last(pathpid(), rule_name(),
           pos_integer()) -> [{timestamp(), maybe_value()}].
last(Pid, RuleName, N) ->
    gen_server:call(Pid, {last, RuleName, N}).

-spec get_rules(pathpid()) -> [rule_name()].
get_rules(Pid) ->
    gen_server:call(Pid, {get_rules}).

-spec persist_conveyors(pathpid()) -> {ok, {reference(), [binary()]}}.
persist_conveyors(Pid) ->
    %% this timeout can be critical for system under load
    gen_server:call(Pid, persist_conveyors, 300).

ack_persistence(Pid, Ref) ->
    gen_server:cast(Pid, {persist_done, Ref}).

%%% gen_server callbacks
init(Path) ->
    % trap_exit should be expicitly set for terminate/2 to work
    % under supervisor
    process_flag(trap_exit, true),
    MaxSlice = case application:get_env(yawndb, max_slice) of
                   {ok, MS}  -> MS;
                   undefined -> 100000
               end,
    %% if there is already one path handler with such name we should
    %% exit gracefully and don't touch supervisor.
    try gproc:reg({n, l, {path, Path}})
    catch error:badarg -> exit(normal)
    end,
    Conveyors = case yawndb_stor_reader:check_data(Path) of
                    {has_data, ConvBinaries} ->
                        Convs = load_conveyors(Path, ConvBinaries),
                        erlang:garbage_collect(self()),
                        Convs;
                    no_data ->
                        [];
                    Other ->
                        lager:error("Got strange reply from flusher ~p",
                                    [Other]),
                        exit({flusher_check_data_error, Other})
                end,
    gproc:reg({p, l, {dirty, Path}}, false),
    gproc:reg({c, l, read_rpm}, 0),
    gproc:reg({c, l, write_rpm}, 0),
    gproc:reg({c, l, path_counter}, 1),
    gproc:reg({c, l, dirty_counter}, 0),

    CleanupEnabled = case application:get_env(yawndb, cleanup_enabled) of
                         {ok, CE}  -> CE;
                         undefined -> true
                     end,
    CleanupPeriodS = case application:get_env(yawndb, cleanup_period) of
                         {ok, CPS} -> CPS;
                         undefined -> 300
                     end,
    CleanupPeriod = CleanupPeriodS * 1000,
    case CleanupEnabled of
        true -> erlang:send_after(crypto:rand_uniform(0, CleanupPeriod),
                                  self(), cleanup);
        _    -> ok
    end,

    {ok, #state{path           = Path,
                conveyors      = Conveyors,
                max_slice      = MaxSlice,
                dirty          = false,
                read_mavg      = jn_mavg:new_mavg(60),
                write_mavg     = jn_mavg:new_mavg(60),
                no_req_count   = 0,
                cleanup_period = CleanupPeriod},
     ?NO_REQ_TIMEOUT}.

handle_call({slice, RuleName, FromTS, ToTS}, _From,
            #state{path=Path,
                   conveyors=Convs,
                   max_slice=MaxSlice}=State) ->
    Rules = ets:lookup(rules, get_prefix(Path)),
    RulesNames = sets:from_list([R#rule.name || R <- Rules]),
    CorrectRule = sets:is_element(RuleName, RulesNames),
    Reply = case lists:filter(fun(#conveyor{rule=#rule{name=Name}}) ->
                                      Name == RuleName
                              end, Convs) of
                [] when CorrectRule ->
                    {ok, empty};
                [] ->
                    lager:info("rule ~p not found", [RuleName]),
                    {error, rule_not_found};
                [Conv] ->
                    TF = Conv#conveyor.rule#rule.timeframe,
                    case (abs(ToTS - FromTS) div TF) > MaxSlice of
                        true ->
                            {error, slice_too_big};
                        false ->
                            read_conveyor(Conv, FromTS, ToTS)
                    end
            end,
    {reply, Reply, flush_nrc(bump_read(State, 1)), ?NO_REQ_TIMEOUT};
handle_call({last, RuleName, N}, _From,
            #state{path=Path,
                   conveyors=Convs,
                   max_slice=MaxSlice}=State) ->
    Rules = ets:lookup(rules, get_prefix(Path)),
    RulesNames = sets:from_list([R#rule.name || R <- Rules]),
    CorrectRule = sets:is_element(RuleName, RulesNames),
    Reply = case lists:filter(fun(#conveyor{rule=#rule{name=Name}}) ->
                                      Name == RuleName
                              end, Convs) of
                [] when CorrectRule ->
                    {ok, empty};
                [] ->
                    {error, rule_not_found};
                [Conv] ->
                    case N > MaxSlice of
                        true ->
                            {error, slice_too_big};
                        false ->
                            TF = Conv#conveyor.rule#rule.timeframe,
                            case Conv#conveyor.last_ts of
                                none ->
                                    {ok, empty};
                                LastTS ->
                                    ToTS = LastTS,
                                    FromTS = max(ToTS - (N - 1) * TF, 0),
                                    read_conveyor(Conv, FromTS, ToTS)
                            end
                    end
            end,
    {reply, Reply, flush_nrc(bump_read(State, 1)), ?NO_REQ_TIMEOUT};
handle_call({get_rules}, _From, #state{path=Path}=State) ->
    Rules = ets:lookup(rules, get_prefix(Path)),
    RuleNames = [R#rule.name || R <- Rules],
    {reply, RuleNames, flush_nrc(bump_read(State, 1)), ?NO_REQ_TIMEOUT};
handle_call(persist_conveyors, _From, #state{conveyors=Convs}=State) ->
    PersistentData = [persist_conveyor(Conv) || Conv <- Convs],
    Ref = make_ref(),
    %% persister will now try to save data before any new data arrive
    NewState = State#state{dirty = {maybe, Ref}},
    Reply = {ok, {Ref, PersistentData}},
    BumpedState = bump_read(bump_write(NewState, 0), 0),
    {reply, Reply, BumpedState, ?NO_REQ_TIMEOUT};
handle_call(Request, _From, State) ->
    lager:warning("got unhandled call: ~p", [Request]),
    {reply, ok, bump_read(State, 1), ?NO_REQ_TIMEOUT}.

%% last persister attempt done properly before any new data, path is now clean
handle_cast({persist_done, Ref}, #state{dirty={maybe, Ref},
                                        path=Path}=State) ->
    gproc:set_value({p, l, {dirty, Path}}, false),
    gproc:set_value({c, l, dirty_counter}, 0),
    NewState = State#state{dirty=false},
    BumpedState = bump_read(bump_write(NewState, 0), 0),
    {noreply, BumpedState, ?NO_REQ_TIMEOUT};
%% some persister attempt was finished when path is dirty or clean or
%% slow persister attempt was finished, ignore anyway
handle_cast({persist_done, _}, State) ->
    BumpedState = bump_read(bump_write(State, 0), 0),
    {noreply, BumpedState, ?NO_REQ_TIMEOUT};
handle_cast({write, Time, Value},
            #state{conveyors=Convs,
                   path=Path}=State) when Value >= 0 ->
    Rules = ets:lookup(rules, get_prefix(Path)),
    RulesSet = sets:from_list(Rules),
    ProvidedRules = sets:from_list([X#conveyor.rule || X <- Convs]),
    MissingRules = sets:to_list(sets:subtract(RulesSet, ProvidedRules)),
    MissingConvs = [init_conveyor(Rule) || Rule <- MissingRules],

    NewConvs = [case is_data_actual(Time, Conv) of
                    true  -> update_conveyor(Conv, Time, Value);
                    false -> Conv
                end || Conv <- Convs ++ MissingConvs],
    %% some sort of caching - we need to update process registry
    %% only when we *changing* state
    NewState = case State#state.dirty of
                   false -> gproc:set_value({p, l, {dirty, Path}}, true),
                            gproc:set_value({c, l, dirty_counter}, 1),
                            State#state{dirty=true};
                   _     -> State
    end,
    BumpedState = bump_write(NewState#state{conveyors=NewConvs}, 1),
    {noreply, flush_nrc(BumpedState), ?NO_REQ_TIMEOUT};
handle_cast({write, Time, Value},State) ->
    lager:warning("got negative value: ~p at ~p", [Value, Time]),
    {noreply, flush_nrc(bump_write(State, 1)), ?NO_REQ_TIMEOUT};
handle_cast(Msg, State) ->
    lager:warning("got unhandled cast: ~p", [Msg]),
    NewState = bump_read(bump_write(State, 0), 0),
    {noreply, NewState, ?NO_REQ_TIMEOUT}.

handle_info(timeout, #state{no_req_count=NRC}=State) when is_integer(NRC) ->
    lager:debug("timeout: ~p", [NRC]),
    BumpedState = bump_read(bump_write(State, 0), 0),
    case NRC < (?NO_REQ_COUNT - 1) of
        true  ->
            {noreply,
             BumpedState#state{no_req_count=NRC+1},
             ?NO_REQ_TIMEOUT};
        false ->
            {noreply,
             BumpedState#state{no_req_count=pre_hibernate},
             ?PRE_HIBERNATE_TIMEOUT}
    end;
handle_info(timeout, #state{no_req_count=pre_hibernate}=State) ->
    lager:debug("hibernating"),
    BumpedState = bump_read(bump_write(State, 0), 0),
    gproc:set_value({c, l, read_rpm}, 0),
    gproc:set_value({c, l, write_rpm}, 0),
    {noreply, BumpedState, hibernate};
handle_info(cleanup, #state{path=Path,
                            conveyors=Convs,
                            cleanup_period=CleanupPeriod}=State) ->
    ActualConvs = [Conv || Conv <- Convs,
                           is_data_actual(Conv#conveyor.last_ts, Conv)],
    ConvsCount = length(Convs),
    ActualConvsCount = length(ActualConvs),
    lager:debug("cleanup path: ~p removed ~p of ~p conveyors ",
                [Path, ConvsCount - ActualConvsCount, ConvsCount]),
    case ActualConvsCount < ConvsCount of
        true  -> self() ! gc;
        false -> erlang:send_after(CleanupPeriod, self(), cleanup)
    end,
    NewState = State#state{conveyors=ActualConvs},
    {noreply, NewState};
handle_info(gc, #state{cleanup_period=CleanupPeriod}=State) ->
    erlang:garbage_collect(self()),
    erlang:send_after(CleanupPeriod, self(), cleanup),
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("got unhandled info: ~p", [Info]),
    {noreply, State, ?NO_REQ_TIMEOUT}.

terminate(Reason, _State) ->
    %% Do not remove this goodbye thing!
    %% This is used to avoid races when gproc
    %% removes record about process REALLY LATER
    %% after its death.
    gproc:goodbye(),
    case Reason of
        shutdown -> ok;
        _        -> lager:info("path handler dying! reason: ~p", [Reason])
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Internal functions
-spec load_conveyors(binary(), [binary()]) -> {ok, [#conveyor{}]}.
load_conveyors(Path, ConvBinaries) ->
    Rules = ets:lookup(rules, get_prefix(Path)),
    DecodedConvs = [X || X <- [try binary_to_term(B)
                               catch error:badarg -> none
                               end || B <- ConvBinaries],
                         X /= none],
    RulesSet = sets:from_list(Rules),
    RightPConvs = [X || X <- DecodedConvs,
                        sets:is_element(X#p_conveyor.rule, RulesSet)],
    [begin
         %% possible optimization: check for last_ts=none
         %% and make ecirca:new instead of ecirca:load

         ValueType = X#p_conveyor.rule#rule.value_size,
         {ok, Circa} = ecirca:load(X#p_conveyor.circa_bin,
                                   ValueType),
         #conveyor{circa       = Circa,
                   last_ts     = X#p_conveyor.last_ts,
                   num_to_atom = X#p_conveyor.num_to_atom,
                   rule        = X#p_conveyor.rule}
     end || X <- RightPConvs,
            is_data_actual(X#p_conveyor.last_ts,
                           #conveyor{rule=X#p_conveyor.rule})].


-spec init_conveyor(#rule{}) -> #conveyor{}.
init_conveyor(Rule) ->
    {ok, CircaEmpty} = ecirca:new(Rule#rule.limit,
                                  Rule#rule.type,
                                  Rule#rule.value_size,
                                  Rule#rule.additional_values),
    NumToAtom = list_to_tuple([X || {X, _} <- Rule#rule.additional_values]),
    #conveyor{circa       = CircaEmpty,
              rule        = Rule,
              num_to_atom = NumToAtom,
              last_ts     = none}.

%% @private
%% @doc Updates conveyor, splitting value if it is neccessary
-spec update_conveyor(#conveyor{}, timestamp(), value()) -> #conveyor{}.
update_conveyor(#conveyor{rule=#rule{split     = SplitType,
                                     timeframe = TF}}=Conv,
                Time, Value) ->
    case split_value(SplitType, TF, Time, Value) of
        {{T1, V1}, {T2, V2}} ->
            update_conveyor_plain(Conv, T1, V1),
            update_conveyor_plain(Conv, T2, V2);
        {T, V} ->
            update_conveyor_plain(Conv, T, V)
    end.

%% @private
%% @doc Just updates conveyor (no logic related to proportional split,
%%      for example)
-spec update_conveyor_plain(#conveyor{}, timestamp(), value()) -> #conveyor{}.
update_conveyor_plain(#conveyor{last_ts=none}=Conv, Time, Value) ->
    TF = Conv#conveyor.rule#rule.timeframe,
    update_conveyor_plain(Conv#conveyor{last_ts=round_up(Time, TF)},
                          Time, Value);
update_conveyor_plain(#conveyor{last_ts=LastTs}=Conv, Time, Value)
  when Time > LastTs ->
    TF = Conv#conveyor.rule#rule.timeframe,
    NewTime = round_up(Time, TF),
    NToPush = (NewTime - LastTs) div TF,
    ok = ecirca:push_many(Conv#conveyor.circa,
                          NToPush, empty),
    update_conveyor_plain(Conv#conveyor{last_ts = NewTime},
                          Time, Value);
update_conveyor_plain(#conveyor{last_ts=_LastTs}=Conv, Time, Value) ->
    Rule = Conv#conveyor.rule,
    NumToAtom = Conv#conveyor.num_to_atom,
    TF = Rule#rule.timeframe,
    Idx = ((Conv#conveyor.last_ts - Time) div TF) + 1,
    RealValue = case Value of
                    {int, V} -> V;
                    {atom, V} when is_integer(V)-> try element(V, NumToAtom)
                                                   catch error:badarg -> none
                                                   end;
                    {atom, V} when is_atom(V) ->
                        ValidAtoms = tuple_to_list(NumToAtom),
                        case lists:member(V, ValidAtoms) of
                            true -> V;
                            false -> none
                        end
               end,
    case (Idx =< Rule#rule.limit andalso
          RealValue /= none) of
        true ->
            case catch ecirca:update(Conv#conveyor.circa, Idx, RealValue) of
                {ok, _} -> ok;
                Error -> lager:warning("can't update! "
                                       "Error: ~p Time: ~p Value: ~p "
                                       "RealValue: ~p Idx: ~p Conv: ~p",
                                       [Error, Time, Value,
                                        RealValue, Idx, Conv])
            end;
        false ->
            lager:debug("won't update because of wrong idx or value is 'none' "
                        "Idx: ~p Value: ~p RealValue: ~p "
                        "NumToAtom: ~p Rule: ~p",
                        [Idx, Value, RealValue,
                         NumToAtom, Conv#conveyor.rule])
    end,
    Conv.

-spec persist_conveyor(#conveyor{}) -> binary().
persist_conveyor(Conveyor) ->
    {ok, CircaBin} = ecirca:save(Conveyor#conveyor.circa),
    term_to_binary(#p_conveyor{circa_bin   = CircaBin,
                               last_ts     = Conveyor#conveyor.last_ts,
                               num_to_atom = Conveyor#conveyor.num_to_atom,
                               rule        = Conveyor#conveyor.rule}).

-spec read_conveyor(#conveyor{}, timestamp(),
                     timestamp()) -> {ok, [{timestamp(), maybe_value()}]} |
                                     {error, slice_too_big}.
read_conveyor(#conveyor{last_ts=none}=_Conv, _FromTS, _ToTS) ->
    {ok, empty};
read_conveyor(Conv, FromTS, ToTS) ->
    try
        TF = Conv#conveyor.rule#rule.timeframe,
        LastTS = Conv#conveyor.last_ts,
        {ok, Size} = ecirca:size(Conv#conveyor.circa),
        FirstTS = LastTS - (Size - 1) * TF,
        FromTSAligned = round_down(FromTS, TF),
        ToTSAligned = round_up(ToTS, TF),
        TSStart = max(FromTSAligned, FirstTS),
        TSEnd = min(ToTSAligned, LastTS),
        MaybeSlice = case (TSStart > LastTS orelse
                           TSEnd < FirstTS) of
                         true -> {ok, []};
                         false ->
                             %% we need slice reversed to avoid
                             %% reversing it as a list. So Idxs are
                             %% reversed and ecirca handles this correctly
                             IdxStart = ((LastTS - TSEnd) div TF) + 1,
                             IdxEnd = ((LastTS - TSStart) div TF) + 1,
                             ecirca:slice(Conv#conveyor.circa,
                                          IdxStart, IdxEnd)
                     end,
        case MaybeSlice of
            {ok, Slice} ->
                FullSlice = build_slice(FromTSAligned, ToTSAligned, TF,
                                        LastTS, Slice, []),
                {ok, FullSlice};
            {error, _}=SliceErr -> SliceErr
        end
    catch
        Type:Reason ->
            lager:error("stor_path:read_conveyor got exception: ~p:~p ~p",
                        [Type, Reason, erlang:get_stacktrace()]),
            {error, internal}
    end.

-spec build_slice(timestamp(), timestamp(), timestamp(),
                  timestamp(), [maybe_value()], Slice) -> Slice when
      Slice :: [{timestamp(), maybe_value()}].
build_slice(End, Now, _TF, _CEnd, _CSlice, Acc) when Now < End ->
    Acc;
build_slice(End, Now, TF, CEnd, [], Acc) ->
    NewAcc = [{Now, empty}|Acc],
    build_slice(End, Now - TF, TF, CEnd, [], NewAcc);
build_slice(End, Now, TF, CEnd, CSlice, Acc) when Now > CEnd ->
    NewAcc = [{Now, empty}|Acc],
    build_slice(End, Now - TF, TF, CEnd, CSlice, NewAcc);
build_slice(End, Now, TF, CEnd, [Val|Vals], Acc) ->
    NewAcc = [{Now, Val}|Acc],
    build_slice(End, Now - TF, TF, CEnd, Vals, NewAcc).

%% @doc Bumps read counters
-spec bump_read(#state{}, integer()) -> #state{}.
bump_read(State, N) ->
    ReadMavg = State#state.read_mavg,
    gproc:set_value({c, l, read_rpm}, jn_mavg:getEventsPer(ReadMavg, 60)),
    State#state{read_mavg=jn_mavg:bump_mavg(ReadMavg, N)}.

%% @doc Bumps write counters
-spec bump_write(#state{}, integer()) -> #state{}.
bump_write(State, N) ->
    WriteMavg = State#state.write_mavg,
    gproc:set_value({c, l, write_rpm}, jn_mavg:getEventsPer(WriteMavg, 60)),
    State#state{write_mavg=jn_mavg:bump_mavg(WriteMavg, N)}.

%% @doc Flush NRC
-spec flush_nrc(#state{}) -> #state{}.
flush_nrc(State) -> State#state{no_req_count=0}.

-spec get_prefix(binary()) -> binary().
get_prefix(Path) -> hd(binary:split(Path, <<"-">>)).

-spec round_up(pos_integer(), pos_integer()) -> pos_integer().
round_up(Val, Div) ->
    (((Val - 1) div Div) + 1) * Div.

-spec round_down(pos_integer(), pos_integer()) -> pos_integer().
round_down(Val, Div) ->
    (Val div Div) * Div.

-spec split_value(split_type(),
                  pos_integer(),
                  timestamp(),
                  value()) -> {{timestamp(), value()},
                               {timestamp(), value()}} |
                              {timestamp(), value()}.
split_value(_, TF, Time, Value) when Time rem TF == 0 -> {Time, Value};
split_value(backward, TF, Time, Value) -> {round_down(Time, TF), Value};
split_value(forward, TF, Time, Value) -> {round_up(Time, TF), Value};
split_value(equal, TF, Time, {atom, _}=A) ->
    {{round_down(Time, TF), A},
     {round_down(Time, TF), A}};
split_value(equal, TF, Time, {int, V}) ->
    T1 = round_down(Time, TF),
    T2 = round_up(Time, TF),
    V2 = V div 2, % IMO adding 1 to first value is better
    V1 = V - V2,
    {{T1, {int, V1}}, {T2, {int, V2}}};
split_value(proportional, TF, Time, {atom, _}=A) ->
    {{round_down(Time, TF), A},
     {round_down(Time, TF), A}};
split_value(proportional, TF, Time, {int, V}) ->
    T1 = round_down(Time, TF),
    T2 = round_up(Time, TF),
    V1 = trunc(V * (1 - (Time - T1) / TF)),
    V2 = V - V1,
    {{T1, {int, V1}}, {T2, {int, V2}}}.

is_data_actual(Time,
               #conveyor{rule=#rule{name=Name,
                                    limit=Limit,
                                    timeframe=TF}}) when is_integer(Time)->
    case yawndb_utils:get_rule_option(Name, autoremove) of
        true ->
            {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
            Now = ?M * MegaSecs + Secs,
            Time + Limit * TF >= Now;
        _ -> true
    end;
is_data_actual(_Time, _Conv) ->
     true.
