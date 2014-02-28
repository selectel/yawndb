%% @doc Core supervisor for the YAWNDB.

-module(yawndb_sup).
-behaviour(supervisor).

%% External exports
-export([start_link/0]).
%% supervisor callbacks
-export([init/1, start_api_listeners/0]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_DYNAMIC(I, Type), {I, {I, start_link, []}, permanent,
                                 5000, Type, dynamic}).

-define(PATH(P, ARG), {P, yawndb_jsonapi, [{action, ARG}]}).

%% @doc API for starting the supervisor
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Supervisor callback
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 60,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    StorPathSup = ?CHILD(yawndb_stor_pathsup, supervisor),
    StorMgr = ?CHILD(yawndb_stor_mgr, worker),
    StorFlusher = {yawndb_stor_flusher,
                   {yawndb_stor_flusher, start_link, []},
                   permanent, 5000, worker, [yawndb_stor_flusher]},
    StorReader = ?CHILD(yawndb_stor_reader, worker),
    Dummy = ?CHILD(yawndb_dummy, worker),

    Processes = ([Dummy, StorPathSup, StorMgr, StorFlusher, StorReader]),

    {ok, {SupFlags, Processes}}.

start_api_listeners() ->
    case application:get_env(listen_tcpapi_reqs) of
        {ok, true} ->
            supervisor:start_child(yawndb_sup, tcp_api_child());
        _ -> ok
    end,
    case application:get_env(listen_jsonapi_reqs) of
        {ok, true} ->
            supervisor:start_child(yawndb_sup, json_api_child());
        _ -> ok
    end,
    case application:get_env(listen_admin_jsonapi_reqs) of
        {ok, true} ->
            supervisor:start_child(yawndb_sup, admin_json_api_child());
        _ -> ok
    end.

admin_json_api_child() ->
    {ok, AdminApiHostStr} = application:get_env(admin_jsonapi_iface),
    {ok, AdminApiHost} = inet_parse:address(AdminApiHostStr),
    {ok, AdminApiPort} = application:get_env(admin_jsonapi_port),
    {ok, AdminApiPresp} = application:get_env(admin_jsonapi_prespawned),
    Dispatch = cowboy_router:compile(
        [{'_', [
                ?PATH("/status", status),
                ?PATH("/paths/all", paths),
                ?PATH("/paths/:path", path),
                ?PATH("/aggregate", aggregate),
                ?PATH("/paths/:path/:rule/slice", slice),
                ?PATH("/paths/:path/:rule/last", last),
                ?PATH("/paths/:path/rules", rules),
                ?PATH("/paths/slice", multi_slice)
               ]}]),
    ranch:child_spec(
      admin_api, AdminApiPresp,
      ranch_tcp, [{ip, AdminApiHost},
                  {port, AdminApiPort}],
      cowboy_protocol, [{env, [{dispatch, Dispatch}]}]).

tcp_api_child() ->
    {ok, TcpApiHostStr} = application:get_env(tcpapi_iface),
    {ok, TcpApiHost} = inet_parse:address(TcpApiHostStr),
    {ok, TcpApiPort} = application:get_env(tcpapi_port),
    {ok, TcpApiPresp} = application:get_env(tcpapi_prespawned),
    TranspOpts = [{ip, TcpApiHost},
                  {port, TcpApiPort}],
    ProtoOpts = [{handler, yawndb_stor_mgr}],
    ranch:child_spec(
      tcp_api, TcpApiPresp,
      ranch_tcp, TranspOpts,
      yawndb_tcpapi_protocol, ProtoOpts).

json_api_child() ->
    {ok, JsonApiHostStr} = application:get_env(jsonapi_iface),
    {ok, JsonApiHost} = inet_parse:address(JsonApiHostStr),
    {ok, JsonApiPort} = application:get_env(jsonapi_port),
    {ok, JsonApiPresp} = application:get_env(jsonapi_prespawned),
    Dispatch = cowboy_router:compile(
        [{'_', [
                ?PATH("/paths/:path/:rule/slice", slice),
                ?PATH("/paths/:path/:rule/last", last),
                ?PATH("/paths/:path/rules", rules),
                ?PATH("/paths/slice", multi_slice)
               ]}]),
    ranch:child_spec(
      json_api, JsonApiPresp,
      ranch_tcp, [{ip, JsonApiHost},
                  {port, JsonApiPort}],
      cowboy_protocol, [{env, [{dispatch, Dispatch}]}]).
