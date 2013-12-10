-module(riakc_poolboy_SUITE).

-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).
-export([pool/1]).

-include_lib("common_test/include/ct.hrl").

-define(DEPS, [protobuffs, riak_pb, riakc, poolboy, folsom, riakc_poolboy]).

all() ->
    [pool].

init_per_suite(Config) ->
    [application:start(App) || App <- ?DEPS],
    Config.

end_per_suite(_Config) ->
    [application:stop(App) || App <- lists:reverse(?DEPS)].

pool(_Config) ->
    SizeArgs = [{size, 10},
                {max_overflow, 20}],
    WorkerArgs = [{hostname, "127.0.0.1"},
                  {port, 8087},
                  {options, [{auto_reconnect, true}]}],
    PoolName = badger_pool,
    {ok, Pid} = riakc_poolboy:start_pool(PoolName, SizeArgs, WorkerArgs),
    true = is_process_alive(Pid),

    {ok, _ServerInfo} = riakc_poolboy:get_server_info(PoolName, 1000),
    {ok, _Buckets} = riakc_poolboy:list_buckets(PoolName, 1000),

    ok = riakc_poolboy:stop_pool(PoolName),
    false = is_process_alive(Pid).
