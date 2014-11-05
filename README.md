# Riak Protocol Buffers Client Pool

About
=========

Riak client connection pool using poolboy

Usage
=========

```erlang
SizeArgs = [{size, 10},
            {max_overflow, 20}],
WorkerArgs = [{hostname, "127.0.0.1"},
              {port, 8087},
              {ping_every, 50000}, %% undefined or absent to disable
              {sync_connect, true}, %% useful for testing - workers must be connected to be available
              {options, [{auto_reconnect, true}]}],
PoolName = badger_pool,
riakc_poolboy:start_pool(PoolName, SizeArgs, WorkerArgs),

{ok, Obj} = riakc_poolboy:get(PoolName, <<"a_bucket">>, <<"a_key">>).

riakc_poolboy:stop_pool(PoolName).
```

TODO
=========

* Support streaming functions
* Tests
