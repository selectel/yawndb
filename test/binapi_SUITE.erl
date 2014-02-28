-module(binapi_SUITE).
-compile([export_all]).
-import(helpers, [start_yawndb/1, stop_yawndb/0]).

-define(PROTOCOL_VERSION, 3).
-define(TCP_PORT, 2011).
-define(YAWNDB_HOST, "localhost").


all() ->
    [{group, normal_updates},
     {group, bad_commands}].

groups()->
    [{normal_updates, [], [normal_update]},
     {bad_commands, [], [bad_version_update]}].


init_per_suite(Conf) ->
    ok = start_yawndb(Conf),
    Conf.

end_per_suite(Conf) ->
    ok = stop_yawndb(),
    Conf.

%% Test cases

normal_update(_Opts) ->
    {ok, Socket} = connect(),
    normal_update(Socket, 1, 100000),
    ok = gen_tcp:close(Socket).

normal_update(_Socket, N, Max) when N >= Max -> ok;
normal_update(Socket, N, Max) ->
    ok = gen_tcp:send(Socket, packet(false, N*130, 777000, <<"stats-a-foo">>)),
    normal_update(Socket, N+1, Max).

bad_version_update(_Opts) ->
    send(packet(?PROTOCOL_VERSION+42, false, 0, 0, <<"stats-a-foo">>)),
    ok.

%% Private API helpers

connect() ->
    gen_tcp:connect(?YAWNDB_HOST, ?TCP_PORT, [binary, {packet, 2}]).

send(Packet) ->
    {ok, Socket} = connect(),
    ok = gen_tcp:send(Socket, Packet),
    ok = gen_tcp:close(Socket).

packet(Version, IsAtom, Time, Value, Path) ->
    IsAtomVal = if IsAtom -> 1; true -> 0 end,
    <<Version:1/big-unsigned-integer-unit:8,
      IsAtomVal:1/big-unsigned-integer-unit:8,
      Time:8/big-unsigned-integer-unit:8,
      Value:8/big-unsigned-integer-unit:8,
      Path/binary>>.

packet(IsAtom, Time, Value, Path) ->
    packet(?PROTOCOL_VERSION, IsAtom, Time, Value, Path).
