-module(yawndb_jsonapi).
-behaviour(cowboy_http_handler).
-include_lib("yawndb.hrl").

-export([init/3, handle/2, terminate/3, json_reply/2]).

-record(state, {action :: atom()}).

%%% cowboy_http_handler callbacks
init({tcp, http}, Req, Opts) ->
    {action, Action} = lists:keyfind(action, 1, Opts),
    {ok, Req, #state{action = Action}}.
handle(Req, #state{action=status}=State) ->
    Result = yawndb_stor_mgr:get_status(),
    {ok, Req2} = json_reply(Result, Req),
    {ok, Req2, State};
handle(Req, #state{action=paths}=State) ->
    Result = yawndb_stor_mgr:get_paths(),
    {ok, Req2} = json_reply(Result, Req),
    {ok, Req2, State};
handle(Req, #state{action=path}=State) ->
    Result = try
                 {MaybePath, _} = cowboy_req:binding(path, Req),
                 Path = is_bin(defined_wrap(MaybePath, no_path)),

                 {Method, _} = cowboy_req:method(Req),
                 %% FIXME(Dmitry): this 'no_fun' is quite shitty
                 check(fun(M) -> M == <<"DELETE">> end, [Method], no_fun),

                 check(fun(PWrapped) ->
                               P = unwrap(PWrapped),
                               gproc:where({n, l, {path, P}}) /= undefined
                       end,
                       [Path], no_path),

                 case yawndb_stor_mgr:delete_path(unwrap(Path)) of
                     ok     -> {ok, <<"deleted">>};
                     failed -> {error, internal}
                 end
             catch
                 throw:{error, _}=Err -> Err
             end,
    {ok, Req2} = json_reply(Result, Req),
    {ok, Req2, State};
handle(Req, #state{action=slice}=State) ->
    Result = try
                 {MaybePath, _} = cowboy_req:binding(path, Req),
                 Path = is_bin(defined_wrap(MaybePath, no_path)),
                 {MaybeRule, _} = cowboy_req:binding(rule, Req),
                 Rule = is_bin(defined_wrap(MaybeRule, no_rule)),

                 {Qs, _} = cowboy_req:qs_vals(Req),
                 From = to_int(is_bin(find_wrap(<<"from">>, Qs, no_from))),
                 To = to_int(is_bin(find_wrap(<<"to">>, Qs, no_to))),
                 check(fun (X, Y) -> X =< Y end, [From, To], from_to_order),
                 yawndb_stor_mgr:slice(unwrap(Rule), unwrap(Path),
                                       unwrap(From), unwrap(To))
             catch
                 throw:{error, _}=Err -> Err
             end,
    Result2 = apply_ok(Result, fun (empty) -> empty;
                                   (X) -> [[A, B] || {A, B} <- X]
                               end),
    {ok, Req2} = json_reply(Result2, Req),
    {ok, Req2, State};
handle(Req, #state{action=last}=State) ->
    Result = try
                 {MaybePath, _} = cowboy_req:binding(path, Req),
                 Path = is_bin(defined_wrap(MaybePath, no_path)),
                 {MaybeRule, _} = cowboy_req:binding(rule, Req),
                 Rule = is_bin(defined_wrap(MaybeRule, no_rule)),

                 {Qs, _} = cowboy_req:qs_vals(Req),
                 N = to_int(is_bin(find_wrap(<<"n">>, Qs, no_n))),
                 yawndb_stor_mgr:last(unwrap(Rule), unwrap(Path), unwrap(N))
             catch
                 throw:{error, _}=Err -> Err
             end,
    %?DBG({Result}),
    Result2 = apply_ok(Result, fun
                                   (empty) -> empty;
                                   (X) -> [[A, B] || {A, B} <- X]
                               end),
    %?DBG({Result2}),
    {ok, Req2} = json_reply(Result2, Req),
    {ok, Req2, State};
handle(Req, #state{action=rules}=State) ->
    Result = try
                 {MaybePath, _} = cowboy_req:binding(path, Req),
                 Path = is_bin(defined_wrap(MaybePath, no_path)),
                 yawndb_stor_mgr:get_rules(unwrap(Path))
             catch
                 throw:{error, _}=Err -> Err
             end,
    {ok, Req3} = json_reply(Result, Req),
    {ok, Req3, State};
handle(Req, #state{action=aggregate}=State) ->
    Result = try
                 {Qs, _} = cowboy_req:qs_vals(Req),
                 FromW = to_int(is_bin(find_wrap(<<"from">>, Qs, no_from))),
                 ToW = to_int(is_bin(find_wrap(<<"to">>, Qs, no_to))),
                 RuleW = is_bin(find_wrap(<<"rule">>, Qs, no_rule)),
                 AggregateW = is_bin(find_wrap(<<"aggregate">>, Qs,
                                               no_aggregate)),
                 check(fun (X, Y) -> X =< Y end, [FromW, ToW], from_to_order),
                 check(fun (A) -> case unwrap(A) of
                                      <<"max">> -> true;
                                      <<"min">> -> true;
                                      <<"avg">> -> true;
                                      <<"sum">> -> true;
                                      _         -> false
                                  end
                       end, [AggregateW], no_aggregate),
                 From = unwrap(FromW),
                 To = unwrap(ToW),
                 Rule = unwrap(RuleW),
                 Aggregate = binary_to_existing_atom(unwrap(AggregateW), utf8),

                 {ok, BodyQs, _} = cowboy_req:body_qs(Req),
                 Paths = binary:split(
                           unwrap(is_bin(find_wrap(<<"paths">>, BodyQs,
                                                   no_paths))),
                           <<",">>,
                           [trim, global]),

                 Data = [case yawndb_stor_mgr:slice(Rule, Path, From, To) of
                             {ok, T} -> T;
                             {error, _}=E -> throw(E)
                         end || Path <- Paths],

                 {ok, aggregate(Aggregate, Data)}
             catch
                 throw:{error, _}=Err -> Err
             end,
    %?DBG({Result}),
    Result2 = apply_ok(Result, fun (empty) -> empty;
                                   (X) -> [[A, B] || {A, B} <- X]
                               end),
    %?DBG({Result2}),
    {ok, Req2} = json_reply(Result2, Req),
    {ok, Req2, State};
handle(Req, #state{action=multi_slice}=State) ->
    Result =
        try
            {Qs, _} = cowboy_req:qs_vals(Req),
            FromW = to_int(is_bin(find_wrap(<<"from">>, Qs, no_from))),
            ToW = to_int(is_bin(find_wrap(<<"to">>, Qs, no_to))),
            check(fun (X, Y) -> X =< Y end, [FromW, ToW], from_to_order),
            From = unwrap(FromW),
            To = unwrap(ToW),

            % Get list of [Path, Rule], specified in POST body as
            % "paths=path1/rule1,path2/rule2"
            {ok, BodyQs, _} =
                try
                    cowboy_req:body_qs(Req)
                catch
                    _:_ -> throw({error, no_body})
                end,
            PathA = unwrap(is_bin(find_wrap(<<"paths">>, BodyQs, no_paths))),
            Paths = binary:split(PathA, <<",">>, [trim, global]),
            check(fun ([]) -> false;
                      (_)  -> true
                  end, [Paths], no_paths),
            PathRules = [
                case binary:split(PR, <<"/">>) of
                    [<<"">>, _]     -> throw({error, no_paths});
                    [_, <<"">>]     -> throw({error, no_paths});
                    PathRule=[_, _] -> PathRule;
                    _               -> throw({error, no_paths})
                end || PR <- Paths],

            % Get slice by path and rule or throw an error.
            GetSlice = fun ([Path, Rule]) ->
                case yawndb_stor_mgr:slice(Rule, Path, From, To) of
                    {ok, T}      -> T;
                    {error, _}=E -> throw(E)
                end
            end,
            % Append new slice to resulting accumulator.
            % Use timestamp as a key, use "empty" value when timestamp
            % not found in appending slice.
            AppendSlice = fun (PathRule, Acc) ->
                Slice = GetSlice(PathRule),
                [Row ++ [proplists:get_value(Time, Slice, empty)]
                    || Row=[Time|_] <- Acc]
            end,
            [First|Tail] = PathRules,
            Data = lists:map(fun erlang:tuple_to_list/1, GetSlice(First)),
            Data2 = lists:foldl(AppendSlice, Data, Tail),
            {ok, Data2}
        catch
            throw:{error, _}=Err -> Err
        end,
    %?DBG({Result}),
    {ok, Req2} = json_reply(Result, Req),
    {ok, Req2, State};
handle(Req, State) ->
    {ok, Req2} = json_reply({error, no_fun}, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.

%%% Internal functions
-spec aggregate_fun(max | min | sum) -> fun((T, T) -> T)
                    when T :: yawndb_stor_path:maybe_value().
aggregate_fun(max) -> fun
                          (empty, _) -> empty;
                          (_, empty) -> empty;
                          (A, B) -> max(A, B)
                      end;
aggregate_fun(min) -> fun
                          (empty, _) -> empty;
                          (_, empty) -> empty;
                          (A, B) -> min(A, B)
                      end;
aggregate_fun(sum) -> fun
                          (empty, _) -> empty;
                          (_, empty) -> empty;
                          (A, B) -> A + B
                      end.

-spec zero(max | min | sum, [integer()]) -> integer().
zero(max, [X|_]) -> X;
zero(min, [X|_]) -> X;
zero(sum, _)     -> 0.

-spec aggregate(max | min | avg | sum,
                [T]) -> T when T :: [{timestamp(),
                                      yawndb_stor_path:maybe_value()}].
aggregate(Type, Data) ->
    aggregate(Type, Data, []).

aggregate(_Type, [[]|_], Result) ->
    lists:reverse(Result);
aggregate(Type, Rest, Result) ->
    {NowTime, _} = hd(hd(Rest)),
    NowValues = [begin
                     NowTime = Time, %% this invariant should hold
                     Value
                 end || [{Time, Value}|_] <- Rest],
    NewRest = [X || [_|X] <- Rest],
    NowResult =
        case Type of
            avg -> %% special case to maintain precision
                case lists:foldl(aggregate_fun(sum), 0, NowValues) of
                    empty -> empty;
                    Sum   -> round(Sum / length(NowValues))
                end;
            OtherType ->
                lists:foldl(aggregate_fun(OtherType), zero(OtherType,
                                                           NowValues),
                            NowValues)
        end,
    aggregate(Type, NewRest, [{NowTime, NowResult} | Result]).

-spec apply_ok({ok, A} | {error, E}, fun((A) -> B)) -> {ok, B} | {error, E}.
apply_ok(Val, Fun) ->
    case Val of
        {ok, T} -> {ok, Fun(T)};
        {error, _}=Err -> Err
    end.

-spec find_wrap(A, [{A, B}], atom()) -> {B | error, atom()}.
find_wrap(Key, Lst, Error) ->
    case lists:keyfind(Key, 1, Lst) of
        {_, T} -> {T, Error};
        _ -> throw({error, Error})
    end.

-spec defined_wrap(A | undefined, atom()) -> {A, atom()}.
defined_wrap(undefined, Error) -> throw({error, Error});
defined_wrap(Value, Error) -> {Value, Error}.

-spec is_bin({any(), atom()}) -> {binary(), atom()}.
is_bin({Val, _}=V) when is_binary(Val) -> V;
is_bin({_, Error}) -> throw({error, Error}).

-spec to_int({binary(), atom()}) -> {integer(), atom()}.
to_int({ValBin, Error}) ->
    try {binary_to_int(ValBin), Error}
    catch error:badarg -> throw({error, Error})
    end.

-spec check(fun((...) -> boolean()),
            [{any(), atom()}], atom()) -> ok.
check(Fun, Args, Err) ->
    case erlang:apply(Fun, Args) of
        true -> ok;
        false -> throw({error, Err})
    end.

-spec unwrap({A, atom()}) -> A.
unwrap({Val, _Error}) -> Val.

-spec json_reply({ok, binary()} | {error, atom()},
                 cowboy_req:req()) -> cowboy_req:req().
json_reply(Result, Req) ->
    {HttpCode, Code, Status, Answer} = result_to_reply(Result),
    Ret = {[{<<"status">>, Status},
            {<<"code">>,   Code},
            {<<"answer">>, Answer}]},
    %?DBG({"encoding to json", Ret}),
    cowboy_req:reply(HttpCode,
                     [{<<"Content-Type">>, <<"application/json">>},
                      {<<"Access-Control-Allow-Origin">>, <<"*">>}],
                     jiffy:encode(Ret), Req).

-spec result_to_reply({ok, binary() | {error, atom()}}) -> {pos_integer(),
                                                            binary(),
                                                            binary()}.
result_to_reply({ok, Ans}) -> {200, ok, <<"ok">>, Ans};
result_to_reply({error, ErrAtom}=Err) ->
    {Code, Answer} = error_to_msg(Err),
    {http_code(Code), ErrAtom, <<"error">>, Answer}.

-spec error_to_msg({error, atom()}) -> {atom(), binary()}.
error_to_msg({error, Error}) ->
    case Error of
        no_path        -> {bad_req,   <<"path is not provided or bad formed">>};
        no_rule        -> {bad_req,   <<"rule is not provided or bad formed">>};
        no_from        -> {bad_req,   <<"from is not provided of bad formed">>};
        no_to          -> {bad_req,   <<"to is not provided or bad formed">>};
        no_n           -> {bad_req,   <<"n is bad provided or bad formed">>};
        no_body        -> {bad_req,   <<"no body specified">>};
        path_not_found -> {not_found, <<"path not found">>};
        slice_too_big  -> {too_big,   <<"slice is too big">>};
        internal       -> {internal,  <<"internal error">>};
        no_fun         -> {bad_req,   <<"unknown request">>};
        rule_not_found -> {not_found, <<"rule not found">>};
        from_to_order  -> {bad_req,   <<"from should be less than to">>};
        process_limit  -> {internal,  <<"processes limit reached">>};
        memory_limit   -> {internal,  <<"memory limit reached">>};
        no_paths       -> {bad_req,   <<"paths is not provided or bad formed">>};
        no_aggregate   -> {bad_req,   <<"aggregate is not provided or bad formed">>};
        _              -> {internal,  <<"internal error">>}
    end.

-spec http_code(atom()) -> pos_integer().
http_code(X) ->
    case X of
        bad_req   -> 400;
        not_found -> 404;
        too_big   -> 413;
        internal  -> 500
    end.

-spec binary_to_int(binary()) -> integer().
binary_to_int(Bin) ->
    list_to_integer(binary_to_list(Bin)).
