%%% @doc Simple_one_for_one supervisor for path handler modules.
-module(yawndb_stor_pathsup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
%% Supervisor callbacks
-export([init/1]).

%% API functions
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% Supervisor callbacks
init([]) ->
    Type = simple_one_for_one,
    MaxRestarts = 5,
    MaxSeconds = 10,
    TypeSup = {yawndb_stor_path,
               {yawndb_stor_path, start_link, []},
               permanent,
               5000, % try to shutdown gracefully, kill after 5secs.
               worker,
               [yawndb_stor_path]},
    {ok, {{Type, MaxRestarts, MaxSeconds},
          [TypeSup]}}.
