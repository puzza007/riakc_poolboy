-module(riakc_poolboy_worker).

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {conn :: pid(),
                ping_every :: undefined | pos_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Args) ->
    Hostname = proplists:get_value(hostname, Args, "127.0.0.1"),
    Port = proplists:get_value(port, Args, 8087),
    PingEvery = proplists:get_value(ping_every, Args, undefined),
    Options = proplists:get_value(options, Args, []),
    {ok, Conn} = riakc_pb_socket:start_link(Hostname, Port, Options),
    case has_auto_reconnect(Options) of
        true ->
            true = ensure_connected(Conn);
        false ->
            ok
    end,
    State = #state{conn=Conn, ping_every=PingEvery},
    case PingEvery of
        undefined ->
            {ok, State};
        PingEvery when is_integer(PingEvery) ->
            {ok, State, PingEvery}
    end.

handle_call({F, A1, A2, A3, A4, A5, A6}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1, A2, A3, A4, A5, A6),
    handle_call_reply(Reply, State);
handle_call({F, A1, A2, A3, A4, A5}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1, A2, A3, A4, A5),
    handle_call_reply(Reply, State);
handle_call({F, A1, A2, A3, A4}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1, A2, A3, A4),
    handle_call_reply(Reply, State);
handle_call({F, A1, A2, A3}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1, A2, A3),
    handle_call_reply(Reply, State);
handle_call({F, A1, A2}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1, A2),
    handle_call_reply(Reply, State);
handle_call({F, A1}, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn, A1),
    handle_call_reply(Reply, State);
handle_call(F, _From, State=#state{conn=Conn}) ->
    Reply = riakc_pb_socket:F(Conn),
    handle_call_reply(Reply, State).

handle_cast(_Msg, State) ->
    {stop, unhandled_cast, State}.

handle_info(timeout, State=#state{conn=Conn, ping_every=PingEvery})
  when is_integer(PingEvery) ->
    try
        pong = riakc_pb_socket:ping(Conn),
        {noreply, State, PingEvery}
    catch _:_ ->
            {stop, ping_failed, State}
    end.

terminate(_Reason, #state{conn=Conn}) ->
    ok = riakc_pb_socket:stop(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

-spec ensure_connected(pid()) -> boolean().
ensure_connected(Conn) ->
    ensure_connected(Conn, 10, 450).

-spec ensure_connected(pid(), non_neg_integer(), non_neg_integer()) -> boolean().
ensure_connected(_Conn, 0, _Delay) ->
    lager:error("Could not ensure connection was established!"),
    false;
ensure_connected(Conn, Retry, Delay) ->
    case riakc_pb_socket:is_connected(Conn) of
        true ->
            true;
        {false, _} ->
            ok = timer:sleep(Delay),
            ensure_connected(Conn, Retry - 1, Delay)
    end.

-spec has_auto_reconnect(list()) -> boolean().
has_auto_reconnect(Options) ->
    lists:member(auto_reconnect, Options) orelse
        lists:member({auto_reconnect, true}, Options).

handle_call_reply(Reply, State=#state{ping_every=undefined}) ->
    {reply, Reply, State};
handle_call_reply(Reply, State=#state{ping_every=PingEvery})
  when is_integer(PingEvery) ->
    {reply, Reply, State, PingEvery}.
