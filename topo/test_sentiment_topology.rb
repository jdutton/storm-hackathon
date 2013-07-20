require 'red_storm'
require 'topo/semantria_bolt'

class TestSentimentSpout < RedStorm::DSL::Spout
  on_init do
    @words = [
      'Lisa - there\'s 2 Skinny cow coupons available $5 skinny cow ice cream coupons on special k boxes and Printable FPC from facebook - a teeny tiny cup of ice cream. I printed off 2 (1 from my account and 1 from dh\'s). I couldn\'t find them instore and i\'m not going to walmart before the 19th. Oh well sounds like i\'m not missing much ...lol',

      "In Lake Louise - a guided walk for the family with Great Divide Nature Tours  rent a canoe on Lake Louise or Moraine Lake  go for a hike to the Lake Agnes Tea House. In between Lake Louise and Banff - visit Marble Canyon or Johnson Canyon or both for family friendly short walks. In Banff a picnic at Johnson Lake  rent a boat at Lake Minnewanka  hike up Tunnel Mountain  walk to the Bow Falls and the Fairmont Banff Springs Hotel  visit the Banff Park Museum. The \"must-do\" in Banff is a visit to the Banff Gondola and some time spent on Banff Avenue - think candy shops and ice cream.",

      'On this day in 1786 - In New York City  commercial ice cream was manufactured for the first time.'
    ]
  end

  on_send {@words.shift unless @words.empty?}
end

class EchoBolt < RedStorm::DSL::Bolt
  on_receive {|tuple| log.info(tuple[0].to_s)}
end

class TestSentimentTopology < RedStorm::DSL::Topology
  spout TestSentimentSpout do
    output_fields :message
  end

  bolt SemantriaBolt do
    source TestSentimentSpout, :global
    output_fields :message, :score
  end

  bolt EchoBolt do
    source SemantriaBolt, :shuffle
  end

  on_submit do |env|
    case env
    when :local
      sleep(20)
      cluster.shutdown
    end
  end

end
