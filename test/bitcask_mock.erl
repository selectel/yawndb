-module(bitcask_mock).
-export([mock/0, unmock/0,
         is_sync_called/0,
         is_open_called/0,
         is_put_called/1,
         validate/0]).

-define(TAB, bitcask_mock).

open(_Path, _Params) ->
    fake_bitcask_ref.

get(_Ref, Key) ->
    case ets:lookup(?TAB, Key) of
        [{Key, V}] ->
            {ok, V};
        [] -> not_found
    end.

put(_Ref, Key, Value) ->
    true = ets:insert(?TAB, {Key, Value}),
    ok.

list_keys(_Ref) ->
    [K || {K,_} <- ets:tab2list(?TAB)].

sync(_Ref) -> ok.
needs_merge(_Ref) -> {true, []}.
merge(_DataDir, _Wtf1, _Wtf2) -> ok.

%% Meck-related stuff
    
mock() ->
    ets:new(?TAB, [set, public, named_table, {keypos,1}]),
    ok = meck:new(bitcask, [no_link]),
    e(open, fun open/2),
    e(list_keys, fun list_keys/1),
    e(get, fun get/2),
    e(put, fun put/3),
    e(sync, fun sync/1),
    e(needs_merge, fun needs_merge/1),

    ok = meck:new(bitcask_merge_worker, [no_link, passthrough]),
    meck:expect(bitcask_merge_worker, merge, fun merge/3).

e(Name, F) ->
    meck:expect(bitcask, Name, F).

unmock() ->
    meck:unload(bitcask),
    meck:unload(bitcask_merge_worker),
    ets:delete(?TAB).

history() ->
    [{Mfa, Result} || {_Pid, Mfa, Result} <- meck:history(bitcask)].

is_sync_called() ->
    lists:any(
      fun({{bitcask,sync,[fake_bitcask_ref]},ok}) ->
              true;
         (_) -> false
      end,
      history()).
                  
is_put_called(Key) ->
    lists:any(
      fun({{bitcask, put, [fake_bitcask_ref,K,_Data]},
           ok}) -> K == Key;
         (_) -> false
      end,
      history()).

is_open_called() ->
    lists:any(
      fun({{bitcask, open, [_Path, _Flags]}, fake_bitcask_ref}) ->
              true;
         (_) -> false
      end,
      history()).

validate() ->
    meck:validate(bitcask).
