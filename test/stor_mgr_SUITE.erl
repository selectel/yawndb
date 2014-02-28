-module(stor_mgr_SUITE).
-compile([export_all]).
-import(helpers, [prepare_to_start_yawndb/1,
                  start_yawndb/1, stop_yawndb/0]).

-define(PROCESSING_TIME, 2000).
-define(M, 1000000).

all() ->
    [{group, paths},
     {group, flusher}].

groups() ->
    [{paths, [], [manage_paths]},
     {flusher, [], [flushed_when_stop]}].

init_per_testcase(_Case, Conf) ->
    bitcask_mock:mock(),
    prepare_to_start_yawndb(Conf),
    Conf.

end_per_testcase(_Case, Conf) ->
    stop_yawndb(),
    bitcask_mock:unmock(),
    Conf.

%% Test cases
manage_paths(_Conf) ->
    [] = proper:module(stor_mgr_statem,
                       [{to_file, user},
                        {numtests, 1000}]),
    ok.

flushed_when_stop(Conf) ->
    start_yawndb(Conf),
    Path = <<"stats-a-bar">>,
    yawndb_stor_mgr:new_path(Path),
    {MegaSecs, Secs, _MicroSecs} = erlang:now(),
    Now = ?M * MegaSecs + Secs,
    yawndb_stor_mgr:write(Path, Now, {int, 1010}),
    stop_yawndb(),
    true = bitcask_mock:validate(),
    true = bitcask_mock:is_open_called(),
    true = bitcask_mock:is_put_called(Path),
    true = bitcask_mock:is_sync_called(),    
    ok.
