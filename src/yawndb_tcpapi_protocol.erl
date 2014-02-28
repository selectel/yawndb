-module(yawndb_tcpapi_protocol).
-behaviour(ranch_protocol).

-include("yawndb.hrl").

-define(VERSION, 3).

%% API
-export([start_link/4]).
%% Callbacks
-export([init/4]).

-record(state, {socket    :: inet:socket(),
                transport :: module(),
                handler   :: module()}).

%%% API
-spec start_link(pid(), inet:socket(), module(), any()) -> {ok, pid()}.
start_link(ListenerPid, Socket, Transport, Opts) ->
	Pid = spawn_link(?MODULE, init, [ListenerPid, Socket, Transport, Opts]),
	{ok, Pid}.

%%% Internal functions
%% @private
-spec init(pid(), inet:socket(), module(), any()) -> ok.
init(ListenerPid, Socket, Transport, Opts) ->
    {handler, Handler} = lists:keyfind(handler, 1, Opts),
    ranch:accept_ack(ListenerPid),
    ok = Transport:setopts(Socket, [{packet, 2},
                                    {active, once}]),
	wait_request(#state{socket=Socket, transport=Transport,
                        handler=Handler}).

-spec wait_request(#state{}) -> ok.
wait_request(#state{socket=Socket, transport=Transport,
                    handler=Handler}=State) ->
    receive
        {tcp, _, <<?VERSION:1/big-unsigned-integer-unit:8,
                   IsAtom:1/big-unsigned-integer-unit:8,
                   Time:8/big-unsigned-integer-unit:8,
                   Value:8/big-unsigned-integer-unit:8,
                   Path/binary>>} ->
            Type = case IsAtom > 0 of
                       true  -> atom;
                       false -> int
                   end,
            Handler:write(Path, Time, {Type, Value}),
            ok = Transport:setopts(Socket, [{active, once}]),
            wait_request(State);
        {error, timeout} ->
            error_terminate(timeout, State);
        {error, closed} ->
            terminate(State);
        {tcp_closed, _} ->
            terminate(State);
        T ->
            lager:warning("received strange packet: ~p", [T]),
            ok
    end.

-spec error_terminate(any(), #state{}) -> ok.
error_terminate(Error, State) ->
    lager:notice("tcpapi_proto terminated: ~p", [Error]),
    terminate(State).

-spec terminate(#state{}) -> ok.
terminate(#state{socket=Socket, transport=Transport}) ->
	Transport:close(Socket),
	ok.
