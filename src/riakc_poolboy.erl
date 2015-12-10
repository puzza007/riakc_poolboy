-module(riakc_poolboy).

-include_lib("riakc/include/riakc.hrl").

-export([counter_incr/4]).
-export([counter_val/3]).
-export([delete/3]).
-export([delete/4]).
-export([delete/5]).
-export([delete_obj/2]).
-export([delete_obj/3]).
-export([delete_obj/4]).
-export([delete_vclock/4]).
-export([delete_vclock/5]).
-export([delete_vclock/6]).
-export([get/3]).
-export([get/4]).
-export([get/5]).
-export([get_bucket/2]).
-export([get_bucket/3]).
-export([get_bucket/4]).
-export([get_index/4]).
-export([get_index/5]).
-export([get_index/6]).
-export([get_index/7]).
-export([get_index_range/5]).
-export([get_index_range/6]).
-export([get_server_info/1]).
-export([get_server_info/2]).
-export([list_buckets/1]).
-export([list_buckets/2]).
-export([list_buckets/3]).
-export([list_keys/2]).
-export([list_keys/3]).
-export([mapred/3]).
-export([mapred/4]).
-export([mapred/5]).
-export([mapred_bucket/3]).
-export([mapred_bucket/4]).
-export([mapred_bucket/5]).
-export([put/2]).
-export([put/3]).
-export([put/4]).
-export([search/3]).
-export([search/4]).
-export([search/5]).
-export([search/6]).
-export([set_bucket/3]).
-export([set_bucket/4]).
-export([set_bucket/5]).
-export([start_pool/3]).
-export([stop_pool/1]).
-export([tunnel/4]).


-spec start_pool(atom(), list(term()), list(term())) -> supervisor:startlink_ret().
start_pool(Name, SizeArgs, WorkerArgs) ->
    riakc_poolboy_sup:start_pool(Name, SizeArgs, WorkerArgs).

-spec stop_pool(atom()) -> ok | {error, running | restarting | not_found | simple_one_for_one}.
stop_pool(Name) ->
    riakc_poolboy_sup:stop_pool(Name).

-type msg_id() :: non_neg_integer(). %% Request identifier for tunneled message types

-spec get_server_info(atom()) -> {ok, server_info()} | {error, term()}.
get_server_info(PoolName) ->
    exec(PoolName, get_server_info).

-spec get_server_info(atom(), timeout()) -> {ok, server_info()} | {error, term()}.
get_server_info(PoolName, Timeout) ->
    exec(PoolName, {get_server_info, Timeout}).

-spec get(atom(), bucket(), key()) -> {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key) ->
    exec(PoolName, {get, Bucket, Key}).

-spec get(atom(), bucket(), key(), timeout() |  get_options()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(PoolName, Bucket, Key, Timeout) ->
    exec(PoolName, {get, Bucket, Key, Timeout}).

-spec get(atom(), bucket(), key(), get_options(), timeout()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(PoolName, Bucket, Key, Options, Timeout) ->
    exec(PoolName, {get, Bucket, Key, Options, Timeout}).

-spec put(atom(), riakc_obj()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj) ->
    exec(PoolName, {put, Obj}).

-spec put(atom(), riakc_obj(), timeout() | put_options()) ->
                 ok | {ok, riakc_obj()} |  riakc_obj() | {ok, key()} | {error, term()}.
put(PoolName, Obj, Timeout) ->
    exec(PoolName, {put, Obj, Timeout}).

-spec put(atom(), riakc_obj(), put_options(), timeout()) ->
                 ok | {ok, riakc_obj()} | riakc_obj() | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options, Timeout) ->
    exec(PoolName, {put, Obj, Options, Timeout}).

-spec counter_incr(atom(), bucket(), key(), number()) -> ok | {error, term()}.
counter_incr(PoolName, Bucket, Key, N) ->
    exec(PoolName, {counter_incr, Bucket, Key, N}).

-spec counter_val(atom(), bucket(), key()) -> ok | {error, term()}.
counter_val(PoolName, Bucket, Key) ->
    exec(PoolName, {counter_val, Bucket, Key}).

-spec delete(atom(), bucket(), key()) -> ok | {error, term()}.
delete(PoolName, Bucket, Key) ->
    exec(PoolName, {delete, Bucket, Key}).

-spec delete(atom(), bucket(), key(), timeout() | delete_options()) ->
                    ok | {error, term()}.
delete(PoolName, Bucket, Key, Timeout) ->
    exec(PoolName, {delete, Bucket, Key, Timeout}).

-spec delete(atom(), bucket(), key(), delete_options(), timeout()) -> ok | {error, term()}.
delete(PoolName, Bucket, Key, Options, Timeout) ->
    exec(PoolName, {delete, Bucket, Key, Options, Timeout}).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock()) -> ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock) ->
    exec(PoolName, {delete_vclock, Bucket, Key, VClock}).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock(), timeout() | delete_options()) ->
                           ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock, TimeoutOrOptions) ->
    exec(PoolName, {delete_vclock, Bucket, Key, VClock, TimeoutOrOptions}).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock(), delete_options(), timeout()) ->
                           ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock, Options, Timeout) ->
    exec(PoolName, {delete_vclock, Bucket, Key, VClock, Options, Timeout}).

-spec delete_obj(atom(), riakc_obj()) -> ok | {error, term()}.
delete_obj(PoolName, Obj) ->
    exec(PoolName, {delete_obj, Obj}).

-spec delete_obj(atom(), riakc_obj(), delete_options()) -> ok | {error, term()}.
delete_obj(PoolName, Obj, Options) ->
    exec(PoolName, {delete_obj, Obj, Options}).

-spec delete_obj(atom(), riakc_obj(), delete_options(), timeout()) -> ok | {error, term()}.
delete_obj(PoolName, Obj, Options, Timeout) ->
    exec(PoolName, {delete_obj, Obj, Options, Timeout}).

-spec list_buckets(atom()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(PoolName) ->
    exec(PoolName, list_buckets).

-spec list_buckets(atom(), timeout()|list()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(PoolName, Timeout) ->
    exec(PoolName, {list_buckets, Timeout}).

-spec list_buckets(atom(), timeout()|list(), timeout()|list()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(PoolName, Timeout, CallTimeout) ->
    exec(PoolName, {list_buckets, Timeout, CallTimeout}).

-spec list_keys(atom(), bucket()) -> {ok, [key()]} | {error, term()}.
list_keys(PoolName, Bucket) ->
    exec(PoolName, {list_keys, Bucket}).

-spec list_keys((atom), bucket(), list()|timeout()) -> {ok, [key()]} |
                                                      {error, term()}.
list_keys(PoolName, Bucket, Timeout) ->
    exec(PoolName, {list_keys, Bucket, Timeout}).

-spec get_bucket(atom(), bucket()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket) ->
    exec(PoolName, {get_bucket, Bucket}).

-spec get_bucket(atom(), bucket(), timeout()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout) ->
    exec(PoolName, {get_bucket, Bucket, Timeout}).

-spec get_bucket(atom(), bucket(), timeout(), timeout()) -> {ok, bucket_props()} |
                                                           {error, term()}.
get_bucket(PoolName, Bucket, Timeout, CallTimeout) ->
    exec(PoolName, {get_bucket, Bucket, Timeout, CallTimeout}).

-spec set_bucket(atom(), bucket(), bucket_props()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps) ->
    exec(PoolName, {set_bucket, Bucket, BucketProps}).

-spec set_bucket(atom(), bucket(), bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout) ->
    exec(PoolName, {set_bucket, Bucket, BucketProps,  Timeout}).

-spec set_bucket(atom(), bucket(), bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout, CallTimeout) ->
    exec(PoolName, {set_bucket, Bucket, BucketProps, Timeout, CallTimeout}).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()]) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query) ->
    exec(PoolName, {mapred, Inputs, Query}).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query, Timeout) ->
    exec(PoolName, {mapred, Inputs, Query, Timeout}).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query, Timeout, CallTimeout) ->
    exec(PoolName, {mapred, Inputs, Query, Timeout, CallTimeout}).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()]) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query) ->
    exec(PoolName, {mapred_bucket, Bucket, Query}).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()], timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout) ->
    exec(PoolName, {mapred_bucket, Bucket, Query, Timeout}).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()],
                    timeout(), timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout, CallTimeout) ->
    exec(PoolName, {mapred_bucket, Bucket, Query, Timeout, CallTimeout}).

-spec search(atom(), binary(), binary()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery) ->
    exec(PoolName, {search, Index, SearchQuery}).

-spec search(atom(), binary(), binary(), search_options()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options) ->
    exec(PoolName, {search, Index, SearchQuery, Options}).

-spec search(atom(), binary(), binary(), search_options(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout) ->
    exec(PoolName, {search, Index, SearchQuery, Options, Timeout}).

-spec search(atom(), binary(), binary(), search_options(), timeout(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout, CallTimeout) ->
    exec(PoolName, {search, Index, SearchQuery, Options, Timeout, CallTimeout}).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key) ->
    exec(PoolName, {get_index, Bucket, Index, Key}).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key, Timeout, CallTimeout) ->
    exec(PoolName, {get_index, Bucket, Index, Key, Timeout, CallTimeout}).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey) ->
    exec(PoolName, {get_index, Bucket, Index, StartKey, EndKey}).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer() | list(),
                key() | integer() | list(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout) ->
    exec(PoolName, {get_index, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout}).

-spec get_index_range(atom(), bucket(), binary() | secondary_index_id(), key() | integer() | list(),
                      key() | integer() | list()) ->
                       {ok, index_results()} | {error, term()}.
get_index_range(PoolName, Bucket, Index, StartKey, EndKey) ->
    exec(PoolName, {get_index_range, Bucket, Index, StartKey, EndKey}).

-spec get_index_range(atom(), bucket(), binary() | secondary_index_id(), key() | integer() | list(),
                      key() | integer() | list(), list()) ->
                       {ok, index_results()} | {error, term()}.
get_index_range(PoolName, Bucket, Index, StartKey, EndKey, Opts) ->
    exec(PoolName, {get_index_range, Bucket, Index, StartKey, EndKey, Opts}).

-spec tunnel(atom(), msg_id(), binary(), timeout()) -> {ok, binary()} | {error, term()}.
tunnel(PoolName, MsgId, Pkt, Timeout) ->
    exec(PoolName, {tunnel, MsgId, Pkt, Timeout}).

-spec exec(atom(), atom() | tuple()) -> ok | tuple().
exec(PoolName, X) ->
    PoolNameBin = atom_to_binary(PoolName, latin1),
    Name = list_to_binary([<<"riakc_poolboy.">>, PoolNameBin, <<".">> | stat(X)]),
    Fun = fun(Worker) ->
                  Metric = folsom_metrics:histogram_timed_begin(Name),
                  Res = gen_server:call(Worker, X, infinity),
                  ok = histogram_timed_notify(Metric),
                  Res
          end,
    poolboy:transaction(PoolName, Fun).

-spec stat(tuple() | atom()) -> [binary()].
stat(T) when is_tuple(T) andalso
             (element(1, T) =:= get orelse
              element(1, T) =:= delete orelse
              element(1, T) =:= delete_vclock orelse
              element(1, T) =:= list_keys orelse
              element(1, T) =:= get_bucket orelse
              element(1, T) =:= set_bucket orelse
              element(1, T) =:= mapred_bucket orelse
              element(1, T) =:= get_index) ->
    Op = element(1, T),
    Bucket = element(2, T),
    [atom_to_binary(Op, latin1), <<".">>, Bucket];
stat(Op) when is_atom(Op) ->
    [atom_to_binary(Op, latin1)];
stat(T) when is_tuple(T) ->
    Op = element(1, T),
    [atom_to_binary(Op, latin1)].

histogram_timed_notify({Name, _} = Metric) ->
     try
         ok = folsom_metrics:histogram_timed_notify(Metric)
     catch _:_ ->
             folsom_metrics:new_histogram(Name),
             folsom_metrics:safely_histogram_timed_notify(Metric)
     end.
