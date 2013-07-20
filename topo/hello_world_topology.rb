require 'red_storm'

class HelloWorldSpout < RedStorm::DSL::Spout
  on_init {@words = ["hello", "storm", "world", "Hoor"]}

  on_fail do |id|
    log.info("***** FAIL #{id}")
  end

  # on_ack do |id|
  #   @ids.delete(id)
  #   log.info("***** ACK #{id}")
  #   if @ids.empty?
  #     log.info("*** SUCCESS")
  #     @redis.lpush(File.basename(__FILE__), "SUCCESS")
  #   end
  # end

  on_send {@words.shift unless @words.empty?}
end

class HelloWorldBolt < RedStorm::DSL::Bolt
  on_receive :emit => false do |tuple|
    log.info(tuple.getString(0))
  end
end

class PigLatinBolt < RedStorm::DSL::Bolt
  output_fields :string

  on_receive :emit => true, :ack => true, :anchor => true do |tuple|
    tuple[0] + "ay"
  end
end

class HelloWorldTopology < RedStorm::DSL::Topology
  configure "Happyzone-Exclaim" do |env|
    if env == :cluster
      num_workers 3
      max_task_parallelism 16
    end
  end
  
  spout HelloWorldSpout do
    output_fields :word
  end

  bolt PigLatinBolt do
    source HelloWorldSpout, :global
  end

  bolt HelloWorldBolt do
    source PigLatinBolt, :global
  end
end
