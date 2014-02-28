%% @doc This is an applicataion starter module for YAWNDB.

-module(yawndb).
-behaviour(application).
-include_lib("yawndb.hrl").

-export([start/0, stop/0]).
-export([start/2, prep_stop/1, stop/1]).

-define(CONFIG_PATH, "yawndb.yml").

%%% API
%% @doc Start YAWNDB server
-spec(start/0 :: () -> ok).
start() ->
    [case application:start(A) of
         ok -> ok;
         {error, {already_started, _}} -> ok;
         E -> throw(E)
     end || A <- [compiler, syntax_tools, crypto,
                  asn1, public_key, ssl, inets,
                  goldrush, lager, sasl, gproc,
                  ranch, cowlib, cowboy, yawndb]],
    ok.

%% @doc Stop YAWNDB server
-spec(stop/0 :: () -> ok).
stop() ->
    application:stop(yawndb),
    application:stop(cowboy),
    application:stop(gproc).

%%% Callbacks
%% @private
%% @doc Application start callback for yawndb
-spec start(atom(), list()) -> {ok, pid()} | {error, any()} | ignore.
start(_StartType, _Args) ->
    ConfigFile = get_config_path(),
    case yaml:load_file(ConfigFile, [implicit_atoms]) of
        {ok, [Config]} -> load_config(Config);
        _              -> exit(bad_config)
    end,
    {ok, RawRules} = application:get_env(rules),
    {ok, Rules} = prepare_rules(RawRules),
    ets:new(rules, [bag, named_table, {keypos, #rule.prefix},
                    {read_concurrency, true}]),
    ets:insert(rules, Rules),
    {ok, RulesOptions} = get_rules_options(RawRules),
    ets:new(rules_options, [bag, named_table, {read_concurrency, true}]),
    ets:insert(rules_options, RulesOptions),
    lager:info("starting up root supervisor..."),
    Ret = yawndb_sup:start_link(),

    case Ret of
        {ok, _} ->
            lager:info("getting a list of paths from disk..."),
            Paths = yawndb_stor_flusher:get_paths(),
            lager:info("loading paths from disk..."),
            [yawndb_stor_mgr:new_path(Path) || Path <- Paths],
            lager:info("starting API listeners..."),
            yawndb_sup:start_api_listeners(),
            lager:info("starting flusher..."),
            yawndb_stor_flusher:start_flushing(),
            lager:info("done");
        _ -> ok
    end,
    Ret.

prep_stop(State) ->
    lager:info("shutting down, should make flush"),
    %% Prevent gproc race condition on flushing when new path
    %% created but not registred.
    timer:sleep(1000),
    yawndb_stor_flusher:prepare_shutdown(),
    State.

%% @private
%% @doc Application stop callback for yawndb
-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%%% Internal functions

-spec get_config_path() -> string().
get_config_path() ->
    case application:get_env(yawndb, yconfig) of
        {ok, Path} -> Path;
        undefined ->
            case init:get_argument(yconfig) of
                {ok, [[Path]]} ->
                    Path;
                error ->
                    ?CONFIG_PATH
            end
    end.


-spec prepare_rules([{atom(), any()}]) -> [#rule{}].
prepare_rules(RawRules) ->
    [begin
         {_, AdditionalVals} = proplists:lookup(additional_values, RawRule),
         case length(AdditionalVals) > 14 of
             true -> exit(too_big_additional_values_list);
             false -> ok
         end
     end || RawRule <- RawRules],
    try
        {ok, [begin
                  Rec = yawndb_rectools:'#new-'(rule),
                  RuleRecord = yawndb_rectools:'#fromlist-'(Rule, Rec),
                  Prefix = RuleRecord#rule.prefix,
                  BinPrefix = list_to_binary(atom_to_list(Prefix)),
                  RuleRecord#rule{prefix=BinPrefix}
              end || Rule <- RawRules]}
    catch
        error:bad_record_op ->
            exit(bad_config)
    end.

get_rules_options(RawRules) ->
    {ok, [begin
              Name = proplists:get_value(name, RawRule),
              OptionsKeys = sets:subtract(
                              sets:from_list(proplists:get_keys(RawRule)),
                              sets:from_list(yawndb_rectools:'#info-'(rule))),
              Options = [{K, V} || {K, V} <- RawRule,
                                   sets:is_element(K, OptionsKeys)],
              {Name, Options}
          end || RawRule <- RawRules]}.

load_config(Config) ->
    [load_app_config(AppConfig) || AppConfig <- Config].

load_app_config({App, Pars}) ->
    [begin
         application:load(App),
         %% Specail case for lager handlers
         PreparedVal =
             case (App == lager andalso Par == handlers) of
                 true  -> prepare_lager_handlers(Val);
                 false -> Val
             end,
         application:set_env(App, Par, if_binary_to_list(PreparedVal))
     end || {Par, Val} <- Pars].

prepare_lager_handlers(Handlers) ->
    HandlersList = lists:flatten(Handlers),
    [begin
         Opts2 =
             case is_list(Opts) of
                 true  -> [{K, if_binary_to_list(V)} || {K, V} <- Opts];
                 false -> Opts
             end,
         {Backend, Opts2}
     end || {Backend, Opts} <- HandlersList].

if_binary_to_list(Val) when is_binary(Val) ->
    binary_to_list(Val);
if_binary_to_list(Val) ->
    Val.
