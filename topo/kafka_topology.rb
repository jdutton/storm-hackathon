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
require 'socket'
  
class EchoBolt < RedStorm::DSL::Bolt
  on_init do
    host = 'kastlersteinhauser.com'
    port = 1337
    @socket = TCPSocket.open(host, port) rescue nil
  end

  on_receive :ack => true, :anchor => true do |tuple|
    tweet = JSON.parse(tuple[0].to_s)
    score = tuple[1]
    if score.abs >= 0.6
      j = (score.abs * 10).to_i - 5
      msg = '      '
      j.times { |offset| msg[offset] = (score > 0) ? '+' : '-' }
      msg << tweet["text"]
      @socket.puts "#{msg}\n" unless @socket.nil?
    end
    log.info("****************** Score: #{score}\n#{msg}\n")
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

  bolt EchoBolt, :parallelism => 1 do
    source SemantriaBolt, :shuffle
  end

  configure "Happyzon-Exclaim" do |env|
    if env == :cluster
      num_workers 2
      max_task_parallelism 4
    else
      #debug true
    end
  end

  on_submit do |env|
    if env == :local
      # sleep(120)
      # cluster.shutdown
    end
  end
end
