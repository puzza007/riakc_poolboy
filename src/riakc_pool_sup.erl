-module(riakc_pool_sup).

-behaviour(supervisor).

-export([start_link/3]).
-export([stop_child/1]).

-export([init/1]).

start_link(Name, SizeArgs, WorkerArgs) when is_atom(Name) ->
    SupName = name(Name),
    supervisor:start_link({local, SupName}, ?MODULE, [Name, SizeArgs, WorkerArgs]).

stop_child(Name) ->
    SupName = name(Name),
    Pid = whereis(SupName),
    ok = supervisor:terminate_child(riakc_poolboy_sup, Pid).

init([Name, SizeArgs, WorkerArgs]) ->
    RestartStrategy = rest_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    PoolSpec = pool_spec(Name, SizeArgs, WorkerArgs),
    StatusSpec = status_spec(Name),

    {ok, {SupFlags, [PoolSpec, StatusSpec]}}.

name(PoolName) when is_atom(PoolName) ->
    NameList = atom_to_list(PoolName),
    list_to_atom("riakc_pool_sup_" ++ NameList).

pool_spec(Name, SizeArgs, WorkerArgs) ->
    PoolArgs =
        [{name, {local, Name}},
         {worker_module, riakc_poolboy_worker}] ++ SizeArgs,
    poolboy:child_spec(Name, PoolArgs, WorkerArgs).

status_spec(Name) ->
    {riakc_pool_status, {riakc_pool_status, start_link, [Name]},
     transient, 5000, worker, [riakc_pool_status]}.
