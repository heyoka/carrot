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
   last_dtag = 0
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


%%%%%%%%%%%%%%%%%%%%%%%%%%% BEHAVIOUR DEFINITION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%
%%%

%%% init the callback
-callback init() -> {ok, ProcessorState :: term()} | {error, Reason :: term()}.

%%% handle a newly arrived amqp message
-callback process(Event :: { #'basic.deliver'{}, #'amqp_msg'{} }, ProcessorState :: term()) ->
   {ok, NewProcessorState} | {ok, noack, NewProcessorState} | {error, Reason :: term(), NewProcessorState}.

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
   {Callback1, CBState} =
   case is_pid(Callback) of
      true  -> {Callback, undefined};
      false -> {ok, CallbackState} = Callback:init(), {Callback, CallbackState}
   end,
   erlang:send_after(0, self(), connect),
   {ok, #state{config = Config, amqp_config = carrot_connect_options:parse(Config),
      callback = Callback1, callback_state = CBState, confirm = Confirm}}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Msg, State) ->
   lager:warning("Invalid cast: ~p", [Msg]),
   {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(connect, State) ->
   NewState = start_connection(State),
   case NewState#state.available of
      true -> carrot_amqp:setup(NewState#state.channel, State#state.config, State#state.confirm);
      false -> nil
   end,
   {noreply, NewState};
%%   {Available, Channel, Conn} = check_for_channel(State),
%%
%%   case Available of
%%      true  -> carrot_amqp:setup(Channel, State#state.config);
%%      false -> nil
%%   end,
%%   {noreply, State#state{
%%      channel = Channel,
%%      available = Available,
%%      connection = Conn
%%   }};

handle_info(stop, State=#state{}) ->
   lager:notice("stopping rmq_consumer: ~p",[self()]),
   {stop, shutdown, State};

handle_info( {'DOWN', _Ref, process, Conn, Reason}, State=#state{connection = Conn}) ->
   lager:notice("MQ connection is DOWN: ~p", [Reason]),
   {noreply, State};
handle_info( {'DOWN', _Ref, process, _Pid, _Reason} = Req, State=#state{callback = Callback, callback_state = CBState}) ->
%%   lager:alert("MQ channel is DOWN: ~p", [Reason]),
   NewCallbackState =
      case is_callable(Callback, handle_info, 2) of
         true  ->
                     {ok, NewCBState} = Callback:handle_info(Req, CBState), NewCBState;
         _Other         ->
                     lager:info(
                        "Function 'handle_info' not exported in Callback-Module: ~p or Callback is a process",
                        [Callback]),
                     CBState
      end,
   {noreply, State#state{callback_state = NewCallbackState}};


handle_info({'EXIT', MQPid, Reason}, State=#state{channel = MQPid, callback = CB, callback_state = CBState} ) ->
   lager:notice("MQ channel DIED: ~p", [Reason]),
   NCBState =
      case is_callable(CB, channel_down, 1) of
         true  ->
            {ok, NewCBState} = CB:channel_down(CBState), NewCBState;
         _Other         ->
            lager:info(
               "Function 'handle_info' not exported in Callback-Module: ~p or Callback is a process",
               [CB]),
            CBState
      end,
   erlang:send_after(0, self(), connect),
   {noreply, State#state{
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
      false -> lager:info("Function 'handle_info' not exported in Callback-Module: ~p",[Callback]), CallbackState
   end,

   {noreply, State#state{callback_state = NewCallbackState}};

handle_info(_Event = {#'basic.deliver'{delivery_tag = DTag, routing_key = RKey}, #'amqp_msg'{
      payload = Payload, props = #'P_basic'{headers = Headers, correlation_id = CorrId}
   }}, #state{callback = Callback, channel = Channel} = State)
                           when is_pid(Callback) ->
   Msg = { {DTag, RKey}, {Payload, CorrId, Headers}, Channel},
   Callback ! Msg,
   {noreply, State#state{last_dtag = DTag}};
%% @doc handle incoming messages from rmq
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
handle_info({'basic.consume_ok', Tag}, State) ->
   lager:debug("got handle_info basic.consume_ok for Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({'basic.qos_ok', {}}, State) ->
   lager:debug("got handle_info basic.qos_ok for Channel: ~p",[State#state.channel]),
   {noreply, State}
;
handle_info({ack, Tag}, State=#state{last_dtag = LastTag}) when Tag > LastTag ->
   lager:notice("acked Tag > than last_tag seen on this channel"),
   %% nope
   {noreply, State};
handle_info({ack, Tag}, State) ->
   handle_ack(Tag, State#state.channel),
   {noreply, State}
;
handle_info({ack, multiple, Tag}, State=#state{last_dtag = LastTag}) when Tag > LastTag ->
   lager:notice("acked Tag > than last_tag seen on this channel"),
   %% nope
   {noreply, State};
handle_info({ack, multiple, Tag}, State) ->
   handle_ack_multiple(Tag, State#state.channel),
   {noreply, State}
;
handle_info({nack, Tag}, State) ->
   amqp_channel:call(State#state.channel, #'basic.nack'{delivery_tag = Tag, multiple = false, requeue = true}),
   lager:debug("nacked single Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({nack, multiple, Tag}, State) ->
   amqp_channel:cast(State#state.channel, #'basic.nack'{delivery_tag = Tag, multiple = true, requeue = true}),
   lager:debug("nacked multiple till Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info({reject, Tag}, State) ->
   amqp_channel:cast(State#state.channel, #'basic.nack'{delivery_tag = Tag, multiple = false, requeue = false}),
   lager:debug("reject Tag: ~p",[Tag]),
   {noreply, State}
;
handle_info(Msg, State) ->
   lager:error("Unhandled msg in rabbitmq_consumer : ~p", [Msg]),
   {noreply, State}.


handle_call(Req, _From, State) ->
   lager:error("Invalid request: ~p", [Req]),
   {reply, invalid_request, State}.

-spec terminate(atom(), state()) -> ok.
terminate(Reason, #state{
                        callback = Callback,
                        callback_state = CBState,
                        channel = Channel,
                        connection = Conn}) ->
   lager:info("~p is terminating with reason: ~p",[?MODULE, Reason]),
   catch amqp_channel:close(Channel),
   catch amqp_connection:close(Conn),
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% MQ connection functions.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connection(State = #state{amqp_config = Config}) ->
%%   lager:info("amqp_params: ~p",[lager:pr(Config, ?MODULE)] ),
   Connection = maybe_start_connection(State),
   NewState =
      case Connection of
         {ok, Conn} ->
            Channel = new_channel(Connection),
            case Channel of
               {ok, Chan} ->
                  State#state{connection = Conn, channel = Chan, available = true};
               Er ->
                  lager:warning("Error starting channel: ~p",[Er]),
                  erlang:send_after(100, self(), connect),
                  State#state{available = false}
            end;
         E ->
            lager:warning("Error starting connection: ~p",[E]),
            erlang:send_after(100, self(), connect),
            State#state{available = false}
      end,
   NewState.


new_channel({ok, Connection}) ->
%%    link(Connection),
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
         lager:notice("connection still alive"),
         {ok, Conn};
      false ->
         case amqp_connection:start(Config) of
            {ok, NewConn} = Res -> erlang:monitor(process, NewConn), Res;
            Other -> Other
         end
   end.
