-type schema_type()       :: last | max | min | sum | avg.
-type split_type()        :: forward | backward | equal | proportional.
-type value_size()        :: small | medium | large.
-type rule_name()         :: atom().
-type path()              :: binary().
-type timestamp()         :: pos_integer().
-type pathpid()           :: pid().

-record(rule, {name              :: rule_name(),
               prefix            :: binary(),
               type              :: schema_type(),
               timeframe         :: pos_integer(),
               limit             :: pos_integer(),
               split             :: split_type(),
               value_size        :: value_size(),
               additional_values :: [{atom(), weak | strong}]}).
%% additional_values should be smaller than 14 elements

-record(conveyor, {circa       :: ecirca:res(),
                   last_ts     :: timestamp(),
                   num_to_atom :: tuple(atom()),
                   rule        :: #rule{}}).

-record(p_conveyor, {circa_bin   :: binary(),
                     last_ts     :: timestamp(),
                     num_to_atom :: tuple(atom()),
                     rule        :: #rule{}}).

-compile([{parse_transform, lager_transform}]).
