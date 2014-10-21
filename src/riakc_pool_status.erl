-module(riakc_pool_status).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {name :: atom(),
                num_workers :: non_neg_integer(),
                ref :: reference(),
                timer :: timer:tref()}).

-define(STAT_NAMES, ["workers", "overflow", "monitors"]).

start_link(Name) when is_atom(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

init([Name]) when is_atom(Name) ->
    WorkerList = gen_server:call(Name, get_all_workers),
    NumWorkers = length(WorkerList),
    Ref = make_ref(),
    [begin
         GaugeName = gauge_name(Name, StatName),
         ok = folsom_metrics:new_gauge(GaugeName)
     end || StatName <- ?STAT_NAMES],
    {ok, Tref} = timer:send_interval(1000, self(), {status, Ref}),
    process_flag(trap_exit, true),
    {ok, #state{name=Name, num_workers=NumWorkers, ref=Ref, timer=Tref}}.

handle_call(_Msg, _From, State) ->
    {stop, unhandled_call, State}.

handle_cast(_Msg, State) ->
    {stop, unhandled_cast, State}.

handle_info({status, Ref}, State = #state{name=Name, ref=Ref}) ->
    {_StatusName, WorkerQueueLen, Overflow, MonitorsSize} = poolboy:status(Name),
    WorkerGauge   = gauge_name(Name, "workers"),
    OverflowGauge = gauge_name(Name, "overflow"),
    MonitorsGauge = gauge_name(Name, "monitors"),
    ok = folsom_metrics:notify({WorkerGauge, WorkerQueueLen}),
    ok = folsom_metrics:notify({OverflowGauge, Overflow}),
    ok = folsom_metrics:notify({MonitorsGauge, MonitorsSize}),
    {noreply, State}.

terminate(_Reason, #state{name=Name, timer=Tref}) ->
    [begin
         GaugeName = gauge_name(Name, StatName),
         folsom_metrics:delete_metric(GaugeName)
     end || StatName <- ?STAT_NAMES],
    {ok, cancel} = timer:cancel(Tref),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

gauge_name(PoolName, StatName) ->
    NameString = atom_to_list(PoolName),
    list_to_binary(NameString ++ ".pool." ++ StatName).
