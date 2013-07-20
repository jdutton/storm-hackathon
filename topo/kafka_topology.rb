java_import 'storm.kafka.SpoutConfig'
java_import 'storm.kafka.StringScheme'
java_import 'storm.kafka.KafkaSpout'
java_import 'storm.kafka.KafkaConfig'
java_import 'com.google.common.collect.ImmutableList'
java_import 'java.util.ArrayList'

require 'red_storm'
require 'topo/tweet_xml_parse_bolt'
require 'topo/semantria_bolt'
require 'json'

class EchoBolt < RedStorm::DSL::Bolt
  on_receive :ack => true, :anchor => true do |tuple|
    tweet = JSON.parse(tuple[0].to_s)
    score = tuple[1]
    # log.info("******************\n#{tuple[0].to_s}")
    log.info("******************\nScore: #{score}:  #{tweet["id_str"]} from #{tweet["user"]["screen_name"]}\n#{tweet["text"]}")
  end
end

class KafkaTopology < RedStorm::DSL::Topology
  spout_config = SpoutConfig.new(
    KafkaConfig::ZkHosts.new("cluster-7-slave-01.sl.hackreduce.net:2181", "/brokers"), # ["cluster-7-slave-02.sl.hackreduce.net"  "cluster-7-slave-04.sl.hackreduce.net"]
    # "twitter_spritzer", # topic
    # "/colin_test",      # Zookeeper root path to store the consumer offsets
    # "test1",            # Zookeeper consumer id to store the consumer offsets

    "twitter_spritzer_json", # topic
    "/colin_test_json",      # Zookeeper root path to store the consumer offsets
    "test1_json",            # Zookeeper consumer id to store the consumer offsets
  )

  spout KafkaSpout, [spout_config]

  # bolt TweetXmlParseBolt do
  #   output_fields :tweet_json
  #   source KafkaSpout, :shuffle
  # end

  bolt SemantriaBolt do
    source KafkaSpout, :shuffle
    output_fields :tweet_json, :score
  end

  bolt EchoBolt do
    source SemantriaBolt, :shuffle
  end

  configure do |env|
    debug true
  end

  on_submit do |env|
    if env == :local
      sleep(120)
      cluster.shutdown
    end
  end
end
