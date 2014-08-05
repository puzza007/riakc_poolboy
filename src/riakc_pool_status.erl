-module(riakc_pool_status).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {name :: atom(),
                num_workers :: pos_integer(),
                ref :: reference(),
                timer :: timer:tref(),
                folsom_gauge :: binary()}).

start_link(Name) when is_atom(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

init([Name]) when is_atom(Name) ->
    NameString = atom_to_list(Name),
    GaugeName = list_to_binary(NameString ++ ".pool.percent_used"),
    %% FIXME: Perhaps send a PR to add this as a poolboy API function
    WorkerList = gen_server:call(Name, get_all_workers),
    NumWorkers = length(WorkerList),
    Ref = make_ref(),
    ok = folsom_metrics:new_gauge(GaugeName),
    {ok, Tref} = timer:send_interval(1000, self(), {status, Ref}),
    process_flag(trap_exit, true),
    {ok, #state{name=Name, num_workers=NumWorkers, ref=Ref, timer=Tref, folsom_gauge=GaugeName}}.

handle_call(_Msg, _From, State) ->
    {stop, unhandled_call, State}.

handle_cast(_Msg, State) ->
    {stop, unhandled_cast, State}.

handle_info({status, Ref}, State = #state{name=Name, num_workers=NumWorkers, ref=Ref, folsom_gauge=GaugeName}) ->
    {_StatusName, _WorkerQueueLen, Overflow, MonitorsSize} = poolboy:status(Name),
    Used = Overflow + MonitorsSize,
    case NumWorkers of
        0 ->
            ok;
        _ ->
            PercentUsed = 100 * (Used / NumWorkers),
            ok = folsom_metrics:notify({GaugeName, PercentUsed})
    end,
    {noreply, State}.

terminate(_Reason, #state{timer=Tref, folsom_gauge=GaugeName}) ->
    folsom_metrics:delete_metric(GaugeName),
    {ok, cancel} = timer:cancel(Tref),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
