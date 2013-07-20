require 'red_storm'
require 'thread'
require 'redis'

class RedisSpout < RedStorm::DSL::Spout
  output_fields :string

  # Redis cluster node :6379
  on_init do
    @redis = Redis.new(:host => "cluster-7-slave-26.sl.hackreduce.net", :port => 6379)
    @ids = [1, 2]

    @q = Queue.new
    @ids.each{|id| @q << id}
  end


  on_fail do |id|
    log.info("***** FAIL #{id}")
  end

  on_ack do |id|
    @ids.delete(id)
    log.info("***** ACK #{id}")
    if @ids.empty?
      log.info("*** SUCCESS")
      @redis.lpush(File.basename(__FILE__), "SUCCESS")
    end
  end

  on_send :reliable => true do
    # avoid putting the thread to sleep endlessly on @q.pop which will prevent local cluster.shutdown
    sleep(1)
    unless @q.empty?
      id = @q.pop
      [id, "DATA#{id}"] # reliable & autoemit, first element must be message_id
    end
  end

end

class PigLatinBolt < RedStorm::DSL::Bolt
  output_fields :string

  on_receive :emit => true, :ack => true, :anchor => true do |tuple|
    tuple[0] + "ay"
  end
end

class RedisTopology < RedStorm::DSL::Topology
  spout RedisSpout, :parallelism => 1

  bolt PigLatinBolt, :parallelism => 1 do
    source RedisSpout, :shuffle
  end

  configure do |env|
    debug true
    message_timeout_secs 10
    num_ackers 2
  end

  on_submit do |env|
    case env
    when :local
      sleep(10)
      cluster.shutdown
    end
  end

end
