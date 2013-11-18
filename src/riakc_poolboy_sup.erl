-module(riakc_poolboy_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_link/3]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(Name, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [{name, {local, Name}},
         {worker_module, riakc_poolboy_worker}] ++ SizeArgs,
    PoolSpec = poolboy:child_spec(Name, PoolArgs, WorkerArgs),
    supervisor:start_child(?MODULE, PoolSpec).

init([]) ->
    {ok, Pools} = application:get_env(riakc_poolboy, pools),
    PoolSpecs =
        lists:map(
          fun({Name, SizeArgs, WorkerArgs}) ->
                  PoolArgs =
                      [{name, {local, Name}},
                       {worker_module, riakc_poolboy_worker}] ++ SizeArgs,
                  poolboy:child_spec(Name, PoolArgs, WorkerArgs)
          end,
          Pools),
    {ok, {{one_for_one, 10, 10}, PoolSpecs}}.
