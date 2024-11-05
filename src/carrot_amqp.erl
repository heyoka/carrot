%% Copyright heyoka 2015
-module(carrot_amqp).
-author("Alexander Minichmair").

-include_lib("amqp_client/include/amqp_client.hrl").

%% API


-export([setup/3]).

%%% amqp msg default definitions

-define(BASE_ARGS,   [{ticket,0}, {arguments,[]}] ).

-define(Q_TEMP_ARGS, ?BASE_ARGS ++ [{durable, false}, {auto_delete, true}]).
-define(Q_PERM_ARGS, ?BASE_ARGS ++ [{durable, true}, {auto_delete, false}]).

-define(X_TEMP_ARGS, ?Q_TEMP_ARGS).
-define(X_PERM_ARGS, ?Q_PERM_ARGS).

-define(BIND_ARGS,   [{ticket,0}, {routing_key, <<"#">>}, {arguments,[]}] ).


-define(X_DECLARE,   'exchange.declare').
-define(Q_DECLARE,   'queue.declare').
-define(X_BIND,      'exchange.bind').
-define(Q_BIND,      'queue.bind').

-define(RKEY,        <<"routing_key">>).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% setup exchange, queue, bind exchange queue, setup prefetch and consume from the queue
-spec setup(Channel :: pid(), Config::list(), UseAck::true|false) -> QName::binary().
setup(Channel, Config, UseAck) ->

   Setup = proplists:get_value(setup, Config),
   Type = proplists:get_value(setup_type, Config),
   case proplists:get_value(exchange, Setup) of
      undefined -> ok; %% if there is no xchange defined, just declare the mandatory queue

      XCreateConfig ->  %% declare and bind exchange to exchange1
         XDeclare = to_exchange_declare(XCreateConfig, Type),
         #'exchange.declare_ok'{} = amqp_channel:call(Channel, XDeclare),
         XBind = to_exchange_bind(XCreateConfig, Type),
         #'exchange.bind_ok'{} = amqp_channel:call(Channel, XBind)
   end,

   QConfig = proplists:get_value(queue, Setup),

%%   io:format("QConfig is ~p",[QConfig]),

   %% declare and bind queue to exchange
   QDeclare = to_queue_declare(QConfig, Type),
%%   lager:notice("Queue declare ~p",[QDeclare]),

   #'queue.declare_ok'{queue = QName} = amqp_channel:call(Channel, QDeclare),
   case proplists:get_value(exchange, QConfig) of
      undefined   -> ok;
      E           ->
         %% ensure exchange exists
         XDeclareD = to_exchange_declare([{exchange, E},{type, <<"topic">>}], Type),
         #'exchange.declare_ok'{} = amqp_channel:call(Channel, XDeclareD),
         setup_bindings(Channel, QConfig, Type)
   end,
   ConsumerTag = proplists:get_value(consumer_tag, Config, <<"">>),
   consume_queue(Channel, QName, proplists:get_value(prefetch_count, Config, 0), ConsumerTag, UseAck),
   QName.

consume_queue(Channel, Q, Prefetch, CTag, UseAck) ->
   %% set prefetch count if any
   case Prefetch > 0 of
      false   ->
         ok;
      true      ->
         #'basic.qos_ok'{} = amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch})
   end,
   %% actually consume from q
   #'basic.consume_ok'{consumer_tag = _Tag} =
      amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q, consumer_tag = CTag, no_ack = not UseAck}, self())
   .
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%% INTERNAL %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setup_bindings(Channel, QueueConfig, Type) ->
   TBinding = to_queue_bind(QueueConfig, Type),
   QBindings =
   case proplists:get_value(bindings, QueueConfig) of
      undefined ->
         [TBinding];
      Bindings when is_list(Bindings) ->
         [TBinding#'queue.bind'{routing_key = RoutingKey} || RoutingKey <- Bindings]
   end,
   Bind = fun(QBind) ->
      #'queue.bind_ok'{} = amqp_channel:call(Channel, QBind)
      end,
   lists:foreach(Bind, QBindings).


%% Converts a tuple list of values to a queue.declare record
-spec to_exchange_declare([{atom(), term()}], atom()) -> #'exchange.declare'{}.
to_exchange_declare(Props, Type) ->
   {NFields, Defaults} = prep_x_declare(Type),
   Props1 = name_postfix(NFields, Props),
   to_record(?X_DECLARE, Props1, Defaults).

prep_x_declare(temporary) ->
   {[exchange], ?X_TEMP_ARGS};
prep_x_declare(permanent) ->
   {[], ?X_PERM_ARGS}.


%% Converts a tuple list of values to a queue.declare record
-spec to_queue_declare([{atom(), term()}], atom()) -> #'queue.declare'{}.
to_queue_declare(Props, Type) ->
   {NFields, Defaults} = prep_q_declare(Type),
   Props1 = name_postfix(NFields, Props),
   to_record(?Q_DECLARE, Props1, Defaults).

prep_q_declare(temporary) ->
   {[queue], ?Q_TEMP_ARGS};
prep_q_declare(permanent) ->
   {[], ?Q_PERM_ARGS}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Converts a tuple list of values to a exchange.bind record
to_exchange_bind(Props, Type) ->
   {NFields, Defaults0} = prep_x_bind(Type),
   Props1 = name_postfix(NFields, Props),
   %% inject exchange-name as destination via defaults
   Destination = proplists:get_value(exchange, Props1),
   Defaults = lists:keystore(destination, 1, Defaults0, {destination, Destination}),
   to_record(?X_BIND, Props1, Defaults).

prep_x_bind(temporary) ->
   {[exchange],  ?BIND_ARGS};
prep_x_bind(permanent) ->
   {[], ?BIND_ARGS}.


%% Converts a tuple list of values to a queue.bind record
to_queue_bind(Props, Type) ->
   XNamePostFix = proplists:get_value(xname_postfix, Props, false),
   NFields = prep_q_bind(Type, XNamePostFix),
   Props1 = name_postfix(NFields, Props),
   to_record(?Q_BIND, Props1, ?BIND_ARGS).

prep_q_bind(temporary, true) ->
   [exchange, queue];
prep_q_bind(permanent, true) ->
   [exchange];
prep_q_bind(permanent, false) ->
   [];
prep_q_bind(temporary, false) ->
   [queue].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Converts a tuple list of values to a record with name RecName
to_record(RecName, Properties, Defaults) ->
%%   lager:info("old props: ~p",[Properties]),
   NewProps =
   case proplists:get_value(arguments, Properties) of
      undefined -> Properties;
      [] -> Properties;
      List when is_list(List) -> NewTable = to_amqp_table(List),
                                 lists:flatten([{arguments, NewTable}|proplists:delete(arguments, Properties)])

   end,
%%   lager:info("converted properties: ~p",[NewProps]),
   Rec = to_record(RecName, carrot_util:proplists_merge(NewProps, Defaults)),
%%   lager:info("Record is ~p",[Rec]),
   Rec.
to_record(RecName, Properties) ->
   list_to_tuple([RecName|[proplists:get_value(X, Properties, false) ||
      X <- recInfo(RecName)]]).

%% this is ugly, but erlang records are a compiler-hack you know
recInfo('exchange.declare') ->
   record_info(fields, 'exchange.declare');
recInfo('queue.declare') ->
   record_info(fields, 'queue.declare');
recInfo('exchange.bind') ->
   record_info(fields, 'exchange.bind');
recInfo('queue.bind') ->
   record_info(fields, 'queue.bind').


name_postfix([], Props) ->
   Props;
name_postfix([Field | R], Props) ->
   Val0 = proplists:get_value(Field, Props),
   Props1 = lists:keystore(Field, 1, Props, {Field, qx_name(Val0)}),
   name_postfix(R, Props1).


-spec qx_name(binary()) -> binary().
qx_name(Prefix) ->
   NodeBinary = list_to_binary(atom_to_list(node())),
   Node = binary:replace(NodeBinary, <<"@">>, <<"-">>),
   <<Prefix/binary, <<"_">>/binary, Node/binary>>.

to_amqp_table(Table) when is_list(Table) ->
   CFun = fun({Key, Val}) ->
            case Val of
               _ when is_integer(Val)  -> {Key, signedint, Val};
               _ when is_binary(Val)   -> {Key, longstr, Val};
               _ when is_list(Val)     -> {Key, longstr, list_to_binary(Val)};
               _ when is_float(Val)    -> {Key, float, Val}
            end
          end,
   lists:map(CFun, Table).
