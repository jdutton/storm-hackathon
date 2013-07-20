## Install RedStorm

rbenv install jruby 1.7.4
redstorm install

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
redstorm bundle topology
```

## Running Redstorm Topology locally

```
redstorm local topo/redis_topology.rb
```

```
redstorm jar topo/redis_topology.rb
redstorm cluster topo/redis_topology.rb
```
