-module(riakc_poolboy_sup).

-behaviour(supervisor).

-export([start_link/0,
         start_pool/3,
         stop_pool/1]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_pool(atom(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
start_pool(Name, SizeArgs, WorkerArgs) when is_atom(Name) ->
    PoolArgs =
        [{name, {local, Name}},
         {worker_module, riakc_poolboy_worker}] ++ SizeArgs,
    poolboy:start(PoolArgs, WorkerArgs).

-spec stop_pool(atom()) -> ok.
stop_pool(Name) ->
    poolboy:stop(Name).

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
