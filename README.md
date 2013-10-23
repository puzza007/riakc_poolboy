# Riak Protocol Buffers Client Pool

About
=========

Riak client connection pool using poolboy

Config
=========

```erlang
{riakc_poolboy, [
    {pools, [
            {riak_pool,
             [
              {size, 10},
              {max_overflow, 20}
             ],
             [
              {hostname, "127.0.0.1"},
              {port, 8087},
              {options, [{auto_reconnect, true}]}
             ]
            }
    ]}
 ]}
```


TODO
=========

* Support streaming functions
* Tests