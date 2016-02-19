### Carrot

Carrot is a small Erlang app that helps Erlang Systems to easly consume and work on messages from
a rabitmq broker by providing a callback module that handles the incoming messages.

All this is done via a simple proplist-config which is also updateable on the fly.

Note: carrot is fairly new, so things can change

## The Configuration

#

    [

      {carrot,
        [
           %% connection parameters
           {broker, [
              {hosts, [ {"192.168.0.1",5672} ]},
              {user, "user"},
              {pass, "pass"},
              {vhost, "/"},
              {reconnect_timeout, 4000},
              {ssl_options, none} % Optional. Can be 'none' or [ssl_option()]
             ]},
           %% consumer setups
           {bunnies,
              [
                 %% cassandra topics
                 {cass_worker_1005, [
                    {workers, 3}, % Number of connections,
                    {callback, rmq_test},
                    {setup_type, temporary},
                    {prefetch_count, 40},
                    {setup,
                       [
                          {queue, [
                             {queue, <<"cassandra_1005">>},
                             {exchange, <<"x_cassandra_topic">>}, {xname_postfix, false},
                             {routing_key, <<"5.#">>}
                          ]}
                       ]
                    }


                 ]}


         ]} %% end bunnies

      ]} %% end carrot


    ]

#
