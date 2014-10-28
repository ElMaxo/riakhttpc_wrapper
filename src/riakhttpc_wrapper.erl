-module(riakhttpc_wrapper).
-author('Max Davidenko').

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/0, stop/1, startPool/1, stopPool/1]).

%% API
-export([createObject/5, createLocalObject/5, storeObject/2, getObject/3, deleteObject/3, getBucketProperties/2, setBucketProperties/4, getKeysList/2,
  addSecondaryIndex/2, setSecondaryIndex/2, getSecondaryIndex/2, getSecondaryIndexes/1, deleteSecondaryIndex/2,
  clearSecondaryIndexes/1, searchBySecondaryIndex/4, getObjectMetadata/1, updateObjectMetadata/2]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() ->
  IsRunning = whereis(riakhttpc_wrapper_poolmgr),
  case IsRunning of
    undefined ->
      start(normal, []);
    _App ->
      ok
  end.

stop() ->
  IsRunning = whereis(riakhttpc_wrapper_poolmgr),
  case IsRunning of
    undefined ->
      ok;
    _App ->
      stop([])
  end.

start(_StartType, _StartArgs) ->
  riakhttpc_wrapper_sup:start_link().

stop(_State) ->
  riakhttpc_wrapper_poolmgr:stop(),
  Pid = whereis(riakhttpc_wrapper_sup),
  if
    Pid /= undefined ->
      exit(Pid, shutdown);
    true ->
      ok
  end,
  ok.

%%%===================================================================
%%% API functions
%%%===================================================================

startPool(Args) ->
  riakhttpc_wrapper_poolmgr:startPool(Args).

stopPool(Pool) ->
  riakhttpc_wrapper_poolmgr:stopPool(Pool).

%%--------------------------------------------------------------------
%% @doc
%% Creates specified object at specified bucket
%%
%% @spec createObject(Bucket, Key, ContentType, Data) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% ContentType - Data content type - binary()
%% Data - Object to create and store - term()
%% @end
%%--------------------------------------------------------------------
createObject(PoolName, Bucket, Key, ContentType, Data) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {create, Bucket, Key, ContentType, Data}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Creates specified object at specified bucket without storing it
%%
%% @spec createLocalObject(Bucket, Key, ContentType, Data) -> riakc_obj() | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associate object with - binary()
%% ContentType - Data content type - binary()
%% Data - Object to create and store - term()
%% @end
%%--------------------------------------------------------------------
createLocalObject(PoolName, Bucket, Key, ContentType, Data) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {create_local, Bucket, Key, ContentType, Data}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Stores specified object at Riak
%%
%% @spec storeObject(RiakObject) -> ok | {error, Error}
%% RiakObject - riakc_obj()
%% @end
%%--------------------------------------------------------------------
storeObject(PoolName, RiakObject) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {store_object, RiakObject}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get object associated with key at specified bucket
%%
%% @spec getObject(Bucket, Key) -> {ok, Object} | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% @end
%%--------------------------------------------------------------------
getObject(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get, Bucket, Key}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Deletes object associated with key at specified bucket
%%
%% @spec deleteObject(Bucket, Key) -> ok | {error, Error}
%% Bucket - Bucket name - binary()
%% Key - Key to associated with object - binary()
%% @end
%%--------------------------------------------------------------------
deleteObject(PoolName, Bucket, Key) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {delete, Bucket, Key}, infinity)
  end, infinity).

searchBySecondaryIndex(PoolName, Bucket, Index, IndexKey) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {query_by_index, Bucket, Index, IndexKey}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Gets specified bucket properties
%%
%% @spec getBucketProperties(Bucket) -> BucketProps | {error, Error}
%% BucketProps - proplist()
%% @end
%%--------------------------------------------------------------------
getBucketProperties(PoolName, Bucket) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {get_bucket_props, Bucket}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Sets specified bucket properties
%%
%% @spec setBucketProperties(Bucket, NVal, AllowMult) -> ok | {error, Error}
%% NVal - replicas count
%% AllowMult - multiple objects by one key storage permission
%% @end
%%--------------------------------------------------------------------
setBucketProperties(PoolName, Bucket, NVal, AllowMult) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {set_bucket_props, Bucket, NVal, AllowMult}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Get keys list for specified bucket
%%
%% @spec getKeysList(Bucket) -> keys list [Key1, Key2...] | {error, Error}
%% Bucket - bucket to get keys for
%% @end
%%--------------------------------------------------------------------
getKeysList(PoolName, Bucket) ->
  poolboy:transaction(PoolName, fun(Worker) ->
    gen_server:call(Worker, {keys_list, Bucket}, infinity)
  end, infinity).

%%--------------------------------------------------------------------
%% @doc
%% Adds secondary index to specified riak metadata object
%%
%% @spec addSecondaryIndex(Metadata, Index) -> metadata()
%% MetadataObject - metadata() object
%% Index - secondary_index()
%% @end
%%--------------------------------------------------------------------
addSecondaryIndex(Metadata, Index) ->
  riakc_obj:add_secondary_index(Metadata, Index).

%%--------------------------------------------------------------------
%% @doc
%% Sets secondary index to specified riak metadata object
%%
%% @spec setSecondaryIndex(Metadata, Index) -> metadata()
%% MetadataObject - metadata() object
%% Index - secondary_index()
%% @end
%%--------------------------------------------------------------------
setSecondaryIndex(Metadata, Index) ->
  riakc_obj:set_secondary_index(Metadata, Index).

%%--------------------------------------------------------------------
%% @doc
%% Gets secondary index by ID from specified riak metadata object
%%
%% @spec getSecondaryIndex(Metadata, IndexID) -> secondary_index_value() | notfound
%% MetadataObject - metadata() object
%% IndexID - secondary_index_id()
%% @end
%%--------------------------------------------------------------------
getSecondaryIndex(Metadata, IndexID) ->
  riakc_obj:get_secondary_index(Metadata, IndexID).

%%--------------------------------------------------------------------
%% @doc
%% Gets secondary indexes from specified riak metadata object
%%
%% @spec getSecondaryIndexes(Metadata) -> [secondary_index(), ...]
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
getSecondaryIndexes(Metadata) ->
  riakc_obj:get_secondary_indexes(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Deletes secondary index by ID from specified riak metadata object
%%
%% @spec deleteSecondaryIndex(Metadata, IndexID) -> metadata()
%% MetadataObject - metadata() object
%% IndexID - secondary_index_id()
%% @end
%%--------------------------------------------------------------------
deleteSecondaryIndex(Metadata, IndexID) ->
  riakc_obj:delete_secondary_index(Metadata, IndexID).

%%--------------------------------------------------------------------
%% @doc
%% Deletes all secondary indexes from specified riak metadata object
%%
%% @spec clearSecondaryIndexes(Metadata) -> metadata()
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
clearSecondaryIndexes(Metadata) ->
  riakc_obj:clear_secondary_indexes(Metadata).

%%--------------------------------------------------------------------
%% @doc
%% Get metadata object from specified riak object
%%
%% @spec getObjectMetadata(RiakObject) -> metadata()
%% RiakObject - riak object instance
%% @end
%%--------------------------------------------------------------------
getObjectMetadata(RiakObject) ->
  riakc_obj:get_update_metadata(RiakObject).

%%--------------------------------------------------------------------
%% @doc
%% Update metadata at specified riak object
%%
%% @spec updateObjectMetadata(RiakObject, MetadataObject) -> Riak object
%% RiakObject - riak object instance
%% MetadataObject - metadata() object
%% @end
%%--------------------------------------------------------------------
updateObjectMetadata(RiakObject, MetadataObject) ->
  riakc_obj:update_metadata(RiakObject, MetadataObject).
