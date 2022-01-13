%% Copyright LineMetrics 2015
-module(rmq_test).
-author("Alexander Minichmair").

-behaviour(rmq_consumer).

-include_lib("amqp_client/include/amqp_client.hrl").
%% API
-export([process/2, init/0, terminate/2, handle_info/2, channel_down/1, publish/0]).

-record(state, {}).

publish() ->
   Opts = #{
      host => "10.14.204.28",
      port => 5672,
      ssl => false,
      user => <<"miae">>,
      pass => <<"tgw2019">>,
      vhost => <<"/">>,
      exchange => <<"x_lm_fanout">>,

      persistent => true,
      safe_mode => false
      },

   {ok, Publisher} = rmq_publisher:start(undefined, Opts),
%%   timer:sleep(5000),
   Exchange = <<"x_lm_fanout">>,
   Key = <<"my.very.special.route">>,
   Payload = <<"does not matter so much">>,
   Args = [],
   Publisher ! {deliver, Exchange, Key, Payload, Args},
   timer:sleep(5000),
   Publisher ! stop.



init() ->
%%   rmq_test_server:start_link(),
   {ok, #state{}}.

process( {Event = #'basic.deliver'{delivery_tag = _DTag, routing_key = _RKey},
         Msg = #'amqp_msg'{payload = _Msg, props = #'P_basic'{headers = _Headers}}} , #state{} = State) ->

   io:format("~p got message to PROCESS ::: ~p ~n ~p",[?MODULE, Event, Msg]),
   {ok, State}.

handle_info({'EXIT', MQPid, Reason}, State ) ->
   io:format("Pid: ~p exited with Reason: ~p",[MQPid, Reason]),
   {ok, State}.

channel_down(State) ->
   io:format("MQ Channel is DOWN, lets do some cleanup: ~p",["yeah"]),
   {ok, State}.

terminate(_Reason, _State) ->
   io:format("~p got terminate message with reason: ~p",[?MODULE, _Reason]),
   ok.