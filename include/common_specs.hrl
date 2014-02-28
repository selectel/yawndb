%%% @author Groshev Dmitry <groshev@selectel.ru>
%%% @doc
%%% This file contains common type definitions that are used across
%%% the sourcecode.

-type sup_init_ret() :: {ok,
                         {SupFlags :: {RestartStrategy :: atom(),
                                       MaxRestarts :: pos_integer(),
                                       MaxSecBetweenRestarts :: pos_integer()},
                          ChildSpec :: [{Name :: atom(),
                                         MFA :: {atom(), atom(), list(any())},
                                         RestartType :: atom(),
                                         ShutdownType :: atom(),
                                         Type :: atom(),
                                         Modules :: list(atom())}, ...]}}.

-type gensrv_init_ret() :: {ok, State :: any()} |
                           {ok, State :: any(), Timeout :: pos_integer()} |
                           ignore |
                           {stop, Reason :: any()}.

-type start_link_ret() :: {ok, pid()} | ignore | {error, any()}.

-type proplist(A, B) :: [{Key :: A, Val :: B}, ...].

