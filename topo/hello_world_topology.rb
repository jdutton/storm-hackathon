require 'red_storm'

class HelloWorldSpout < RedStorm::DSL::Spout
  on_init {@words = ["hello", "world"]}
  on_send {@words.shift unless @words.empty?}
end

class HelloWorldBolt < RedStorm::DSL::Bolt
  on_receive :emit => false do |tuple|
    log.info(tuple.getString(0))
  end
end

class HelloWorldTopology < RedStorm::DSL::Topology
  spout HelloWorldSpout do
    output_fields :word
  end

  bolt HelloWorldBolt do
    source HelloWorldSpout, :global
  end
end
