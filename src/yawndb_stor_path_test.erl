-module(yawndb_stor_path_test).
-include_lib("yawndb.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile([export_all]).
-import(yawndb_stor_path, [init_conveyor/1, update_conveyor/3, read_conveyor/3]).

%% temporarily disable this test
overflow_avg_test() ->
    Rule = #rule{name = <<"somename">>,
                 prefix = <<"somepref">>,
                 type = avg,
                 timeframe = 60,
                 limit = 1440,
                 split = forward,
                 value_size = small,
                 additional_values = []
                },
    test_overflow(Rule, 4096),
    test_overflow(Rule#rule{value_size = medium}, 268435456),
    test_overflow(Rule#rule{value_size = large}, 1152921504606846976),
    ok.

sum_test() ->
    Rule = #rule{name = <<"somename">>,
                 prefix = <<"somepref">>,
                 type = sum,
                 timeframe = 60,
                 limit = 1440,
                 split = forward,
                 value_size = small,
                 additional_values = []
                },
    ConvTemp = init_conveyor(Rule),
    Conv = ConvTemp#conveyor{last_ts = 0},
    Conv2 = update_conveyor(Conv, 15, {int, 60}),
    %% Ecirca is mutable, but we should get updated last_ts
    ?assertEqual(Conv#conveyor{last_ts = 60}, Conv2),
    ?assertEqual({ok, [{0, empty}, {60, 60}, {120, empty}]},
                 read_conveyor(Conv2, 0, 120)),
    Conv3 = update_conveyor(Conv2, 15, {int, 60}),
    ?assertEqual({ok, [{0, empty}, {60, 120}, {120, empty}]},
                 read_conveyor(Conv3, 0, 120)),
    ok.

%% FIXME(ikkeps): enable test when ecirca will be fixed
overflow_sum_test() ->
    Rule = #rule{name = <<"somename">>,
                 prefix = <<"somepref">>,
                 type = sum,
                 timeframe = 60,
                 limit = 1440,
                 split = forward,
                 value_size = small,
                 additional_values = []
                },
    test_overflow(Rule, 4096-10),
    test_overflow(Rule#rule{value_size = medium}, 268435456-1),
    ok.

test_overflow(Rule, OverflowingValue) ->
    Conveyor = init_conveyor(Rule),
    Conv = Conveyor#conveyor{last_ts = 0},
    Conv1 = update_conveyor(Conv, 15, {int, 60}),
    ?assertEqual({ok, [{0, empty}, {60, 60}, {120, empty}]},
                 read_conveyor(Conv1, 0, 120)),

    update_conveyor(Conv1, 60, {int, OverflowingValue}),
    % State of ecirca is mutable, so check it were not changed
    ?assertEqual({ok, [{0, empty}, {60, 60}, {120, empty}]},
                 read_conveyor(Conv1, 0, 120)).
