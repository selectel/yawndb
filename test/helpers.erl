-module(helpers).
-export([start_yawndb/1, stop_yawndb/0, prepare_to_start_yawndb/1]).

-include_lib("common_test/include/ct.hrl").

start_yawndb(Config) ->
    prepare_to_start_yawndb(Config),
    ok = application:start(yawndb),
    ok.

prepare_to_start_yawndb(Config) ->
    DataDir = ?config(data_dir, Config),
    ConfigPath = filename:join([DataDir, "yawndb.yml"]),
    application:set_env(yawndb, yconfig, ConfigPath),
    [case application:start(A) of
         ok -> ok;
         {error, {already_started, _}} -> ok;
         E -> throw(E)
     end || A <- [compiler, syntax_tools, crypto,
                  asn1, public_key, ssl, inets,
                  goldrush, lager, sasl, gproc,
                  ranch, cowlib, cowboy]],
    ok.

stop_yawndb() ->
    yawndb:stop(),
    ok.
