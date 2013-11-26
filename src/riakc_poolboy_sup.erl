-module(riakc_poolboy_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_pool/3,
         stop_pool/1]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_pool(Name, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [{name, {local, Name}},
         {worker_module, riakc_poolboy_worker}] ++ SizeArgs,
    PoolSpec = poolboy:child_spec(Name, PoolArgs, WorkerArgs),
    supervisor:start_child(?MODULE, PoolSpec).

-spec stop_pool(atom()) -> ok | {error, running | restarting | not_found | simple_one_for_one}.
stop_pool(Name) ->
    ok = supervisor:terminate_child(?MODULE, Name),
    supervisor:delete_child(?MODULE, Name).

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
