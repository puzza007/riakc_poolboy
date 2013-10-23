-module(riakc_poolboy).

%% @headerfile "riakc.hrl"
-include_lib("riakc/include/riakc.hrl").

-export([get_server_info/1, get_server_info/2,
         get/3, get/4, get/5,
         put/2, put/3, put/4,
         delete/3, delete/4, delete/5,
         delete_vclock/4, delete_vclock/5, delete_vclock/6,
         delete_obj/2, delete_obj/3, delete_obj/4,
         list_buckets/1, list_buckets/2, list_buckets/3,
         list_keys/2, list_keys/3,
         get_bucket/2, get_bucket/3, get_bucket/4,
         set_bucket/3, set_bucket/4, set_bucket/5,
         mapred/3, mapred/4, mapred/5,
         mapred_bucket/3, mapred_bucket/4, mapred_bucket/5,
         search/3, search/4, search/5, search/6,
         get_index/4, get_index/5, get_index/6, get_index/7,
         tunnel/4]).

-type msg_id() :: non_neg_integer(). %% Request identifier for tunneled message types

-define(POOLBOY_TRANSACTION(X), poolboy:transaction(PoolName, fun(Worker) -> gen_server:call(Worker, X) end).

-spec get_server_info(atom()) -> {ok, server_info()} | {error, term()}.
get_server_info(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, get_server_info)
    end).

-spec get_server_info(atom(), timeout()) -> {ok, server_info()} | {error, term()}.
get_server_info(PoolName, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_server_info, Timeout})
    end).

-spec get(atom(), bucket(), key()) -> {ok, riakc_obj()} | {error, term()}.
get(PoolName, Bucket, Key) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get, Bucket, Key})
    end).

-spec get(atom(), bucket(), key(), timeout() |  get_options()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(PoolName, Bucket, Key, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get, Bucket, Key, Timeout})
    end).

-spec get(atom(), bucket(), key(), get_options(), timeout()) ->
                 {ok, riakc_obj()} | {error, term()} | unchanged.
get(PoolName, Bucket, Key, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get, Bucket, Key, Options, Timeout})
    end).

-spec put(atom(), riakc_obj()) ->
                 ok | {ok, riakc_obj()} | {ok, key()} | {error, term()}.
put(PoolName, Obj) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {put, Obj})
    end).

-spec put(atom(), riakc_obj(), timeout() | put_options()) ->
                 ok | {ok, riakc_obj()} |  riakc_obj() | {ok, key()} | {error, term()}.
put(PoolName, Obj, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {put, Obj, Timeout})
    end).

-spec put(atom(), riakc_obj(), put_options(), timeout()) ->
                 ok | {ok, riakc_obj()} | riakc_obj() | {ok, key()} | {error, term()}.
put(PoolName, Obj, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {put, Obj, Options, Timeout})
    end).

-spec delete(atom(), bucket(), key()) -> ok | {error, term()}.
delete(PoolName, Bucket, Key) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete, Bucket, Key})
    end).
  
-spec delete(atom(), bucket(), key(), timeout() | delete_options()) ->
                    ok | {error, term()}.
delete(PoolName, Bucket, Key, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete, Bucket, Key, Timeout})
    end).

-spec delete(atom(), bucket(), key(), delete_options(), timeout()) -> ok | {error, term()}.
delete(PoolName, Bucket, Key, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete, Bucket, Key, Options, Timeout})
    end).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock()) -> ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_vclock, Bucket, Key, VClock})
    end).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock(), timeout() | delete_options()) ->
                           ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock, TimeoutOrOptions) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_vclock, Bucket, Key, VClock, TimeoutOrOptions})
    end).

-spec delete_vclock(atom(), bucket(), key(), riakc_obj:vclock(), delete_options(), timeout()) ->
                           ok | {error, term()}.
delete_vclock(PoolName, Bucket, Key, VClock, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_vclock, Bucket, Key, VClock, Options, Timeout})
    end).

-spec delete_obj(atom(), riakc_obj()) -> ok | {error, term()}.
delete_obj(PoolName, Obj) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_obj, Obj})
    end).

-spec delete_obj(atom(), riakc_obj(), delete_options()) -> ok | {error, term()}.
delete_obj(PoolName, Obj, Options) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_obj, Obj, Options})
    end).

-spec delete_obj(atom(), riakc_obj(), delete_options(), timeout()) -> ok | {error, term()}.
delete_obj(PoolName, Obj, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {delete_obj, Obj, Options, Timeout})
    end).

-spec list_buckets(atom()) -> {ok, [bucket()]} | {error, term()}.
list_buckets(PoolName) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, list_buckets)
    end).

-spec list_buckets(atom(), timeout()|list()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(PoolName, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {list_buckets, Timeout})
    end).

-spec list_buckets(atom(), timeout()|list(), timeout()|list()) -> {ok, [bucket()]} |
                                                   {error, term()}.
list_buckets(PoolName, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {list_buckets, Timeout, CallTimeout})
    end).

-spec list_keys(atom(), bucket()) -> {ok, [key()]} | {error, term()}.
list_keys(PoolName, Bucket) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {list_keys, Bucket})
    end).

-spec list_keys((atom), bucket(), list()|timeout()) -> {ok, [key()]} |
                                                      {error, term()}.
list_keys(PoolName, Bucket, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {list_keys, Bucket, Timeout})
    end).

-spec get_bucket(atom(), bucket()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_bucket, Bucket})
    end).

-spec get_bucket(atom(), bucket(), timeout()) -> {ok, bucket_props()} | {error, term()}.
get_bucket(PoolName, Bucket, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_bucket, Bucket, Timeout})
    end).

-spec get_bucket(atom(), bucket(), timeout(), timeout()) -> {ok, bucket_props()} |
                                                           {error, term()}.
get_bucket(PoolName, Bucket, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_bucket, Bucket, Timeout, CallTimeout})
    end).

-spec set_bucket(atom(), bucket(), bucket_props()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {set_bucket, Bucket, BucketProps})
    end).

-spec set_bucket(atom(), bucket(), bucket_props(), timeout()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {set_bucket, Bucket, BucketProps,  Timeout})
    end).

-spec set_bucket(atom(), bucket(), bucket_props(), timeout(), timeout()) -> ok | {error, term()}.
set_bucket(PoolName, Bucket, BucketProps, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {set_bucket, Bucket, BucketProps, Timeout, CallTimeout})
    end).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()]) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred, Inputs, Query})
    end).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()], timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred, Inputs, Query, Timeout})
    end).

-spec mapred(atom(), mapred_inputs(), [mapred_queryterm()], timeout(), timeout()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(PoolName, Inputs, Query, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred, Inputs, Query, Timeout, CallTimeout})
    end).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()]) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred_bucket, Bucket, Query})
    end).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()], timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred_bucket, Bucket, Query, Timeout})
    end).

-spec mapred_bucket(atom(), bucket(), [mapred_queryterm()],
                    timeout(), timeout()) ->
                           {ok, mapred_result()} |
                           {error, {badqterm, mapred_queryterm()}} |
                           {error, timeout} |
                           {error, term()}.
mapred_bucket(PoolName, Bucket, Query, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {mapred_bucket, Bucket, Query, Timeout, CallTimeout})
    end).

-spec search(atom(), binary(), binary()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {search, Index, SearchQuery})
    end).

-spec search(atom(), binary(), binary(), search_options()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {search, Index, SearchQuery, Options})
    end).

-spec search(atom(), binary(), binary(), search_options(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {search, Index, SearchQuery, Options, Timeout})
    end).

-spec search(atom(), binary(), binary(), search_options(), timeout(), timeout()) ->
                    {ok, search_result()} | {error, term()}.
search(PoolName, Index, SearchQuery, Options, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {search, Index, SearchQuery, Options, Timeout, CallTimeout})
    end).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_index, Bucket, Index, Key})
    end).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, Key, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_index, Bucket, Index, Key, Timeout, CallTimeout})
    end).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer(), key() | integer()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_index, Bucket, Index, StartKey, EndKey})
    end).

-spec get_index(atom(), bucket(), binary() | secondary_index_id(), key() | integer() | list(),
                key() | integer() | list(), timeout(), timeout()) ->
                       {ok, index_results()} | {error, term()}.
get_index(PoolName, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {get_index, Bucket, Index, StartKey, EndKey, Timeout, CallTimeout})
    end).

-spec tunnel(atom(), msg_id(), binary(), timeout()) -> {ok, binary()} | {error, term()}.
tunnel(PoolName, MsgId, Pkt, Timeout) ->
    poolboy:transaction(PoolName, fun(Worker) ->
        gen_server:call(Worker, {tunnel, MsgId, Pkt, Timeout})
    end).
