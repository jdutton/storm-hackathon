java_import 'storm.kafka.SpoutConfig'
java_import 'storm.kafka.StringScheme'
java_import 'storm.kafka.KafkaSpout'
java_import 'storm.kafka.KafkaConfig'
java_import 'com.google.common.collect.ImmutableList'
java_import 'java.util.ArrayList'

require 'red_storm'

class EchoBolt < RedStorm::DSL::Bolt
  on_receive {|tuple| log.info(tuple[0].to_s)}
end

class KafkaTopology < RedStorm::DSL::Topology
  spout_config = SpoutConfig.new(
    KafkaConfig::ZkHosts.new("cluster-7-slave-01.sl.hackreduce.net:2181", "/brokers"), # ["cluster-7-slave-02.sl.hackreduce.net"  "cluster-7-slave-04.sl.hackreduce.net"]
    "twitter_spritzer", # topic
    "/colin_test",      # Zookeeper root path to store the consumer offsets
    "test1",            # Zookeeper consumer id to store the consumer offsets
  )

  spout KafkaSpout, [spout_config]

  bolt EchoBolt do
    source KafkaSpout, :shuffle
  end

  configure do |env|
    debug true
  end

  on_submit do |env|
    if env == :local
      sleep(10)
      cluster.shutdown
    end
  end
end