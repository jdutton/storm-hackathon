class PigLatinBolt < RedStorm::DSL::Bolt
  output_fields :string

  on_receive :emit => true, :ack => true, :anchor => true do |tuple|
    tuple[0] + "ay"
  end
end

