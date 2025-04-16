%%% @author Alexander Minichmair
%%%
%%% @copyright 2015 LineMetrics GmbH
%%%
%%% @doc MQ consuming - worker.
%%%
%%% every worker holds his own connection and amqp-channel
%%%
%%% rmq_consumer is a behaviour for servers to consume from rabbitMQ
%%% combined with a config for setting up a queue, to consume from (and maybe an exchange, which can be
%%% bound to an existing exchange), a callback module must be implemented with the function(s) defined in the
%%% -callback() clause
%%%
%%%
%%%
%%% @end
%%%
%%%
%%% a gen_server abused to behave like a state-machine in some areas ;)
%%%

-module(rmq_consumer).

-behaviour(gen_server).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%-define(MAX_TAG, 16#F423F). %% 999999
-define(MAX_TAG, 999999). %% 999999
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Required Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("amqp_client/include/amqp_client.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Types.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(state, {
   connection = undefined :: undefined|pid(),
   channel = undefined:: undefined|pid(),
   channel_ref = undefined:: undefined|reference(),
   spawned_tasks = []:: [{pid(), reference()}],
   config = []:: proplists:proplist(),
   amqp_config :: term(),
   callback :: atom(),
   callback_state :: term(),
   available = false:: boolean(),
   confirm = true :: boolean(),
   internal_tag = 0,
   tag_list = [] :: list(),
   qname :: binary(),
   %% if false, no queue will be setup, but an amqp channel is available for
   %% non consuming tasks
   setup_queue :: false
}).

-type state():: #state{}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Exports.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%% Public API.

%%% gen_server/worker_pool callbacks.
-export([
   init/1, terminate/2, code_change/3,
   handle_call/3, handle_cast/2, handle_info/2
   , start_link/2, start_monitor/2, stop/1, handle_ack/2, handle_ack_multiple/2]).
-export([delete_queue/2]).


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%

%%% init the callback
-callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

%%% handle a newly arrived amqp message
-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
   {ok, NewProcessorState :: term()} | {ok, noack, NewProcessorState :: term()}
   | {error, Reason :: term(), NewProcessorState :: term()}.

%%% handle termination of the process
-callback terminate(TReason :: term(), ProcessorState :: term()) ->
   ok | {error, Reason :: term()}.

%% this callback is optional for handling other info messages for the callback
-callback handle_info(TEvent :: term(), ProcessorState :: term()) ->
   {ok, NewProcessorState :: term()} | {error, Reason :: term()}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Public API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
stop(Server) ->
   Server ! stop.

delete_queue(Server, Queue) ->
   Server ! {unbind_delete_queue, Queue}.

start_link(Callback, Config) ->
   gen_server:start_link(?MODULE, [Callback, Config], []).


start_monitor(Callback, Config) ->
   case gen_server:start(?MODULE, [Callback, Config], []) of
      {ok, Pid}      -> Ref = erlang:monitor(process, Pid), {ok, Pid, Ref};
      {error, What}  -> {error, What}
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% gen_server API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec init(proplists:proplist()) -> {ok, state()}.
init([Callback, Config]) ->
   process_flag(trap_exit, true),
   Confirm = proplists:get_value(confirm, Config, true),
   SetupQ = proplists:get_value(setup_queue, Config, true),
   {Callback1, CBState} =
   case is_pid(Callback) orelse (whereis(Callback) /= undefined) of
      true  -> erlang:monitor(process, Callback), {Callback, undefined};
      false -> {ok, CallbackState} = Callback:init(), {Callback, CallbackState}
   end,
   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config, amqp_config = carrot_connect_options:parse(Config),
      callback = Callback1, callback_state = CBState, confirm = Confirm, setup_queue = SetupQ}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:warning("Invalid cast: ~p", [Msg]),
   {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State = #state{setup_queue = DoSetup}) ->
   NewState = start_connection(State),
   State1 =
   case NewState#state.available of
      true ->
         case DoSetup of
            true ->
               QName = carrot_amqp:setup(NewState#state.channel, State#state.config, State#state.confirm),
               NewState#state{qname = QName};
            false -> NewState
         end;
      false -> NewState
   end,
   {noreply, State1};

handle_info({unbind_delete_queue, Queue}, State = #state{}) ->
   do_delete_queue(Queue, State),
   {noreply, State};
handle_info(unbind_delete_queue, State = #state{qname = Queue}) ->
   do_delete_queue(Queue, State),
   {noreply, State};

handle_info(stop, State=#state{}) ->
   {stop, shutdown, State};

handle_info( {'DOWN', _Ref, process, Callback, _Reason}, State=#state{callback = Callback}) ->
   %% looks like the parent process died, so stop myself
   {stop, normal, State};
handle_info( {'DOWN', _Ref, process, _Pid, _Reason} = Req, State=#state{callback = Callback, callback_state = CBState}) ->
%%   lager:alert("MQ channel is DOWN: ~p", [Reason]),
   NewCallbackState =
      case is_callable(Callback, handle_info, 2) of
         true  ->
                     {ok, NewCBState} = Callback:handle_info(Req, CBState), NewCBState;
         _Other         ->
                     CBState
      end,
   {noreply, State#state{callback_state = NewCallbackState}};


handle_info({'EXIT', Conn, Reason}, State=#state{connection = Conn, qname = Queue} ) ->
   lager:notice("MQ connection (q: ~p) DIED: ~p", [Queue, Reason]),
   NewState = invalidate_tags(State),
   send_conn_status(amqp_disconnected, NewState),
   {noreply, NewState#state{
      channel_ref = undefined,
      available = false
   }};

handle_info({'EXIT', MQPid, Reason}, State=#state{channel = MQPid, callback = CB, callback_state = CBState} ) ->
   lager:notice("MQ channel (q: ~p) DIED: ~p", [State#state.qname, Reason]),

   NCBState =
      case is_callable(CB, channel_down, 1) of
         true  ->
            {ok, NewCBState} = CB:channel_down(CBState), NewCBState;
         _Other         ->
            send_conn_status(amqp_disconnected, State),
            CBState
      end,
   erlang:send_after(0, self(), connect),
   NewState = invalidate_tags(State),
   {noreply, NewState#state{
      channel = undefined,
      channel_ref = undefined,
      available = false,
      callback_state = NCBState
   }};

handle_info({'EXIT', _OtherPid, _Reason} = Message,
               State=#state{callback = Callback, callback_state = CallbackState} ) ->
   NewCallbackState =
   case is_callable(Callback, handle_info, 2) of
      true -> {ok, NewCBState} = Callback:handle_info(Message, CallbackState), NewCBState;
      false ->
         CallbackState
   end,

   {noreply, State#state{callback_state = NewCallbackState}};

%% @doc handle incoming messages from rmq, when callback is a process
handle_info(_Event = {#'basic.deliver'{delivery_tag = DTag, routing_key = RKey, redelivered = _Redelivered}, #'amqp_msg'{
      payload = Payload, props = #'P_basic'{headers = Headers, correlation_id = CorrId}
   }}, #state{callback = Callback, channel = Channel} = State)
                           when is_pid(Callback) ->

   {NewTag, NewState} = add_tag(DTag, State),
%%   Msg = { {NewTag, RKey}, {Payload, CorrId, Headers}, Channel},
   Msg = {deliver, State#state.qname, Channel, {NewTag, RKey}, {Payload, CorrId, Headers}},
   Callback ! Msg,
   {noreply, NewState};
%% @doc handle incoming messages from rmq, when callback is a module
handle_info(Event = {#'basic.deliver'{delivery_tag = DTag, routing_key = _RKey},
   #'amqp_msg'{payload = _Msg, props = #'P_basic'{headers = _Headers, correlation_id = _CorrId}}},
    #state{callback = Callback, callback_state = CState} = State)    ->

   NewCallbackState =
   case Callback:process(Event, CState) of
      {ok, NewState}                -> %lager:info("OK processing queue-message: ~p",[Event]),
         amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = DTag}), NewState;

      {ok, noack, NewState}       ->
         NewState;

      {error, _Error, NewState}     -> lager:error("Error when processing queue-message: ~p",[_Error]),
         amqp_channel:cast(State#state.channel,
            #'basic.nack'{delivery_tag = DTag, requeue = true, multiple = false}),
         NewState

   end,
   {noreply, State#state{callback_state = NewCallbackState}}
;
handle_info({'basic.consume_ok', _Tag}, State) ->
   {noreply, State}
;
handle_info({'basic.qos_ok', {}}, State) ->
   {noreply, State}
;
handle_info({ack, InternalTag}, State=#state{tag_list = TagList}) ->
  case lists:keytake(InternalTag, 1, TagList) of
    {value, {InternalTag, DTag}, NewTagList} ->
      handle_ack(DTag, State#state.channel),
      {noreply, State#state{tag_list = NewTagList}};
    false ->
      {noreply, State}
  end;
handle_info({ack, multiple, InternalTag}, State=#state{tag_list = TagList, qname = Q}) ->

  case lists:keyfind(InternalTag, 1, TagList) of
    {InternalTag, DTag} ->
      handle_ack_multiple(DTag, State#state.channel),
      NewTagList = lists:filter(fun({K, _V}) -> K >= InternalTag end, TagList),
%%       lager:notice("ack multi to ~p (tag_list_size ~p)", [{InternalTag, DTag}, length(NewTagList)]),
      {noreply, State#state{tag_list = NewTagList}};
    false ->
      lager:notice("acking (multiple) for queue ~p, internal tag ~p, NOT FOUND, ignore", [Q, InternalTag]),
      %% nope
      {noreply, State}
  end
;
handle_info({nack, Tag}, State) ->
   maybe_handle_with_tag(
      fun(DTag) ->
         amqp_channel:call(State#state.channel, #'basic.nack'{delivery_tag = DTag, multiple = false, requeue = true})
      end,
      Tag, State)
;
handle_info({nack, multiple, Tag}, State) ->
   maybe_handle_with_tag(
      fun(DTag) ->
         amqp_channel:cast(State#state.channel, #'basic.nack'{delivery_tag = DTag, multiple = true, requeue = true})
      end,
      Tag, State)
;
handle_info({reject, Tag, Requeue}, State) ->
   maybe_handle_with_tag(
      fun(DTag) ->
         amqp_channel:cast(State#state.channel, #'basic.nack'{delivery_tag = DTag, multiple = false, requeue = Requeue})
      end,
      Tag, State)
;
handle_info({reject, Tag}, State) ->
   maybe_handle_with_tag(
      fun(DTag) ->
         amqp_channel:cast(State#state.channel, #'basic.nack'{delivery_tag = DTag, multiple = false, requeue = true})
      end,
      Tag, State)
;
handle_info({'basic.cancel', ConsumerTag, What}, State = #state{qname = Queue, channel = Chan}) ->
   lager:warning("Rabbit cancelled queue ~p, consumer with tag ~p with nowait ~p",[Queue, ConsumerTag, What]),
   %% stop our channel and restart, to get back the queue
   catch amqp_channel:close(Chan, 1, <<"rabbitmq cancelled queue, restart channel">>),
   {noreply, State}
;
handle_info(Msg, State) ->
   lager:error("Unhandled msg in rabbitmq_consumer : ~p", [Msg]),
   {noreply, State}.

maybe_handle_with_tag(Fun, InternalTag, State = #state{tag_list = TagList}) ->
   case lists:keyfind(InternalTag, 1, TagList) of
      {InternalTag, DTag} -> Fun(DTag);
      false -> ok
   end,
   {noreply, State}.

handle_call(Req, _From, State) ->
   lager:error("Invalid request: ~p", [Req]),
   {reply, invalid_request, State}.

-spec terminate(atom(), state()) -> ok.
terminate(Reason, #state{
                        callback = Callback,
                        callback_state = CBState,
%%                        channel = Channel,
                        connection = Conn}) ->
   catch amqp_connection:close(Conn),
%%   catch amqp_channel:close(Channel),
   lager:info("~p is terminating with reason: ~p",[?MODULE, Reason]),
   case is_pid(Callback) of
      true -> ok;
      false -> Callback:terminate(Reason, CBState)
   end
   .

-spec code_change(string(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
   {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Private API.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
send_conn_status(Status, #state{callback = CB}) when is_pid(CB) ->
   case is_process_alive(CB) of
      true -> CB ! {Status, self()};
      false -> ok
   end;
send_conn_status(_S, _State) -> ok.

%% dtag handling
add_tag(_DTag, State=#state{confirm = false}) ->
   %% do not store tags, if we do not confirm anyways, but we increase the internal tag, to not break any code outside
   %% dealing with the tags
   next_tag(State);
add_tag(DTag, State=#state{tag_list = TagList}) ->
   {NewIntTag, NewState} = next_tag(State),
   {NewIntTag,
      NewState#state{tag_list = [{NewIntTag, DTag}|TagList]}
   }.

invalidate_tags(State) ->
   State#state{tag_list = []}.

next_tag(State = #state{internal_tag = ITag}) when ITag > ?MAX_TAG ->
   {1, State#state{internal_tag = 1}};
next_tag(State = #state{internal_tag = ITag}) ->
   {ITag+1, State#state{internal_tag = ITag+1}}.


do_delete_queue(Queue, #state{channel = Channel, amqp_config = #amqp_params_network{virtual_host = VHost}}) ->
   Delete = #'queue.delete'{queue = Queue},
   case amqp_channel:call(Channel, Delete) of
      #'queue.delete_ok'{} -> lager:notice("Queue ~p deleted on vhost ~p",[Queue, VHost]);
      Other -> lager:warning("Queue delete not ok: ~p",[Other])
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{amqp_config = _Config, callback = _CB}) ->
%%   lager:info("amqp_params: ~p",[lager:pr(Config, ?MODULE)] ),
   Connection = maybe_start_connection(State),
   NewState =
      case Connection of
         {ok, Conn} ->
            Channel = new_channel(Connection),
            case Channel of
               {ok, Chan} ->
                  send_conn_status(amqp_connected, State),
                  State#state{connection = Conn, channel = Chan, available = true};
               Er ->
                  lager:warning("Error starting channel: ~p",[Er]),
                  erlang:send_after(100, self(), connect),
                  State#state{available = false}
            end;
         E ->
            lager:warning("Error starting connection: ~p",[E]),
            erlang:send_after(300, self(), connect),
            State#state{available = false}
      end,
   NewState.


new_channel({ok, Connection}) ->
   configure_channel(amqp_connection:open_channel(Connection));

new_channel(Error) ->
   Error.

configure_channel({ok, Channel}) ->
   link(Channel),
   {ok, Channel};
configure_channel(Error) ->
   Error.

is_callable(Arg, Fun, Artity) ->
   case is_pid(Arg) of
      true -> false;
      false -> erlang:function_exported(Arg, Fun, Artity)
   end.

%%%
handle_ack(Tag, Channel) ->
   Res = amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag, multiple = false}),
   Res
.
handle_ack_multiple(Tag, Channel) ->
   Res = amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag, multiple = true}),
   Res
.

maybe_start_connection(#state{connection = Conn, amqp_config = Config}) ->
   case is_pid(Conn) andalso is_process_alive(Conn) of
      true ->
         {ok, Conn};
      false ->
         case amqp_connection:start(Config) of
            {ok, NewConn} = Res -> link(NewConn), Res;
            Other -> Other
         end
   end.
