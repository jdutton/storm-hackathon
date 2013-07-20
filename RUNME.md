## Install RedStorm

rbenv install jruby 1.7.4

## Setup ~/.storm/storm.yaml

Make sure `~/.storm/storm.yaml`
```
nimbus.host: "cluster-7-master.sl.hackreduce.net"
nimbus.thrift.port: 8745
```

## Bundle Ruby Gems for RedStorm

Initially, or whenever Gemfile changes...

```
bundle install
bundle exec redstorm install
bundle exec redstorm bundle topology
```

## Running Redstorm Topology locally

```
bundle exec redstorm local topo/redis_topology.rb
```

```
bundle exec redstorm jar topo/redis_topology.rb
bundle exec redstorm cluster topo/redis_topology.rb
```
