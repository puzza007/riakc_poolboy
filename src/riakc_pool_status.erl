-module(riakc_pool_status).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {name :: atom(),
                ref :: reference(),
                timer :: timer:tref()}).

-define(STAT_NAMES, [{new_gauge, "workers"},
                     {new_gauge, "overflow"},
                     {new_gauge, "monitors"},
                     {new_histogram, "stats_timeout"}
                    ]).

start_link(Name) when is_atom(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

init([Name]) when is_atom(Name) ->
    Ref = make_ref(),
    [begin
         MetricName = metric_name(Name, StatName),
         ok = folsom_metrics:F(MetricName)
     end || {F, StatName} <- ?STAT_NAMES],
    {ok, Tref} = timer:send_interval(1000, self(), {status, Ref}),
    process_flag(trap_exit, true),
    {ok, #state{name=Name, ref=Ref, timer=Tref}}.

handle_call(_Msg, _From, State) ->
    {stop, unhandled_call, State}.

handle_cast(_Msg, State) ->
    {stop, unhandled_cast, State}.

handle_info({status, Ref}, State = #state{name=Name, ref=Ref}) ->
    try
        {_StatusName, WorkerQueueLen, Overflow, MonitorsSize} = poolboy:status(Name),
        WorkerGauge   = metric_name(Name, "workers"),
        OverflowGauge = metric_name(Name, "overflow"),
        MonitorsGauge = metric_name(Name, "monitors"),
        ok = folsom_metrics:notify({WorkerGauge, WorkerQueueLen}),
        ok = folsom_metrics:notify({OverflowGauge, Overflow}),
        ok = folsom_metrics:notify({MonitorsGauge, MonitorsSize})
    catch
        timeout:{gen_server, call, [_, status]} ->
            Timeout = metric_name(Name, "stats_timeout"),
            ok = folsom_metrics:notify({Timeout, 1})
    end,
    {noreply, State}.            

terminate(_Reason, #state{name=Name, timer=Tref}) ->
    [begin
         MetricName = metric_name(Name, StatName),
         folsom_metrics:delete_metric(MetricName)
     end || {_, StatName} <- ?STAT_NAMES],
    {ok, cancel} = timer:cancel(Tref),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

metric_name(PoolName, StatName) ->
    NameString = atom_to_list(PoolName),
    list_to_binary(NameString ++ ".pool." ++ StatName).
