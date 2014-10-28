-module(riakhttpc_wrapper_worker).
-author('Max Davidenko').

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {riak_kv_conn = null}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Args) ->
% 1. Connect to Riak node
  Server = proplists:get_value(server, Args),
  Port = proplists:get_value(port, Args),
  Prefix = proplists:get_value(prefix, Args),
  Options = proplists:get_value(conn_opts, Args),
  Connection = rhc:create(Server, Port, Prefix, Options),
  {ok, #state{riak_kv_conn = Connection}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

% Callback for createObject api function
handle_call({create, Bucket, Key, ContentType, Data}, _From, State) ->
  RiakObject = riakc_obj:new(Bucket, Key, Data, ContentType),
  Reply = rhc:put(State#state.riak_kv_conn, RiakObject),
  {reply, Reply, State};

% Callback for createLocalObject api function
handle_call({create_local, Bucket, Key, ContentType, Data}, _From, State) ->
  RiakObject = riakc_obj:new(Bucket, Key, Data, ContentType),
  {reply, RiakObject, State};

% Callback for storeObject api function
handle_call({store_object, RiakObject}, _From, State) ->
  Reply = rhc:put(State#state.riak_kv_conn, RiakObject),
  {reply, Reply, State};

% Callback for getObject api function
handle_call({get, Bucket, Key}, _From, State) ->
  GetResult = rhc:get(State#state.riak_kv_conn, Bucket, Key),
  case GetResult of
    {ok, Fetched} ->
      ValCount = riakc_obj:value_count(Fetched),
      if
        ValCount > 1 ->
          Result = {ok, getValues(Fetched)};
        true ->
          Result = decodeObject(Fetched)
      end,
      Reply = Result;
    {error, _Reason} ->
      Reply = GetResult
  end,
  {reply, Reply, State};

% Callback for deleteObject api function
handle_call({delete, Bucket, Key}, _From, State) ->
  Reply = rhc:delete(State#state.riak_kv_conn, Bucket, Key),
  {reply, Reply, State};

% Callback for searchBySecondaryIndex api function
handle_call({query_by_index, Bucket, Index, IndexKey}, _From, State) ->
  Reply = rhc:get_index(State#state.riak_kv_conn, Bucket, Index, IndexKey),
  {reply, Reply, State};

% Callback for getBucketProperties api function
handle_call({get_bucket_props, Bucket}, _From, State) ->
  Reply = rhc:get_bucket(State#state.riak_kv_conn, Bucket),
  {reply, Reply, State};

% Callback for setBucketProperties api function
handle_call({set_bucket_props, Bucket, NVal, AllowMult}, _From, State) ->
  Reply = rhc:set_bucket(State#state.riak_kv_conn, Bucket, [{n_val, NVal}, {allow_mult, AllowMult}]),
  {reply, Reply, State};

% Callback for getKeysList api function
handle_call({keys_list, Bucket}, _From, State) ->
  Reply = rhc:list_keys(State#state.riak_kv_conn, Bucket),
  {reply, Reply, State};

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  % Disconnecting from server
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

getValues(RiakObject) ->
  CTypes = riakc_obj:get_content_types(RiakObject),
  [CType | _] = CTypes,
  Values = riakc_obj:get_values(RiakObject),
  Decoder = fun(Arg) ->
    if
      CType == "application/x-erlang-term" ->
        Result = binary_to_term(Arg);
      true ->
        Result = Arg
    end,
    Result
  end,
  lists:map(Decoder, Values).

decodeObject(RiakObject) ->
  case riakc_obj:get_content_type(RiakObject) of
    "application/x-erlang-term" ->
      try
        {ok, binary_to_term(riakc_obj:get_value(RiakObject))}
      catch
        _:Reason ->
          {error, Reason}
      end;
    _ ->
      {ok, riakc_obj:get_value(RiakObject)}
  end.