-module(yawndb_utils).
-export([get_rule_option/2]).

get_rule_option(RuleName, Option) ->
    case ets:lookup(rules_options, RuleName) of
        []                    -> {error, no_rule};
        [{RuleName, Options}] -> proplists:get_value(Option, Options)
    end.                                                     
