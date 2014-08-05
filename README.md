kafkat
======

Simplified command-line administration for Kafka brokers.

## Usage

1. Install the gem.

```
gem install kafkat
```

2. Create a new configuration file to match your deployment.

```
{
  "kafka_path": "/srv/kafka/kafka_2.10-0.8.1.1",
  "log_path": "/mnt/kafka-logs",
  "zk_path": "zk0.foo.ca:2181,zk1.foo.ca:2181,zk2.foo.ca:2181/kafka"
}
```

3. At any time, you can run `kafkat` to get a list of available commands and their arguments.

```
$ bin/kafkat
kafkat 0.0.1: Simplified command-line administration for Kafka brokers
usage: kafkat [command] [options]

Here's a list of supported commands:

  brokers                                             Print available brokers from Zookeeper.
  clean-indexes                                       Delete untruncated Kafka log indexes from the filesystem.
  elect-leaders [topic]                               Begin election of the preferred leaders.
  partitions [topic]                                  Print partitions by topic.
  partitions [topic] --under-replicated               Print partitions by topic (only under-replicated).
  partitions [topic] --unavailable                    Print partitions by topic (only unavailable).
  reassign [topic] [--brokers <ids>] [--replicas <n>] Begin reassignment of partitions.
  resign-rewrite <broker id>                          Forcibly rewrite leaderships to exclude a broker.
  resign-rewrite <broker id> --force                  Same as above but proceed if there are no available ISRs.
  shutdown <broker id>                                Gracefully remove leaderships from a broker (requires JMX).
  topics                                              Print all topics.
```

## Important Note

The gem needs read/write access to the Kafka log directory for some operations (clean indexes).
