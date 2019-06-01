[![Build Status](https://travis-ci.org/ctxswitch/kafkat.png?branch=master)](https://travis-ci.org/ctxswitch/kafkat)
[![Coverage Status](https://coveralls.io/repos/github/ctxswitch/kafkat/badge.svg?branch=master)](https://coveralls.io/github/ctxswitch/kafkat?branch=master)

kafkat
======

Simplified command-line administration for Kafka brokers.

## Contact 
**Let us know!** If you fork this, or if you use it, or if it helps in anyway, we'd love to hear from you! opensource@airbnb.com

## License & Attributions
This project is released under the Apache License Version 2.0 (APLv2).

## How to release

- update the version number in `lib/kafkat/version.rb`
- execute `bundle exec rake release`


## Usage

* Install the gem.

```
gem install kafkat
```

* Create a new configuration file to match your deployment.

```
{
  "kafka_path": "/srv/kafka/kafka_2.10-0.8.1.1",
  "log_path": "/mnt/kafka-logs",
  "zk_path": "zk0.foo.ca:2181,zk1.foo.ca:2181,zk2.foo.ca:2181/kafka"
}
```

Kafkat searches for this file in two places, `~/.kafkatcfg` and `/etc/kafkatcfg`.

* At any time, you can run `kafkat` to get a list of available commands and their arguments.

```
$ kafkat
kafkat 0.0.10: Simplified command-line administration for Kafka brokers
usage: kafkat [command] [options]

Here's a list of supported commands:

  brokers                                                             Print available brokers from Zookeeper.
  clean-indexes                                                       Delete untruncated Kafka log indexes from the filesystem.
  controller                                                          Print the current controller.
  elect-leaders [topic]                                               Begin election of the preferred leaders.
  partitions [topic]                                                  Print partitions by topic.
  partitions [topic] --under-replicated                               Print partitions by topic (only under-replicated).
  partitions [topic] --unavailable                                    Print partitions by topic (only unavailable).
  reassign [topic] [--brokers <ids>] [--replicas <n>]                 Begin reassignment of partitions.
  resign-rewrite <broker id>                                          Forcibly rewrite leaderships to exclude a broker.
  resign-rewrite <broker id> --force                                  Same as above but proceed if there are no available ISRs.
  set-replication-factor [topic] [--newrf <n>] [--brokers id[,id]]    Set the replication factor of
  shutdown <broker id>                                                Gracefully remove leaderships from a broker (requires JMX).
  topics                                                              Print all topics.
  drain <broker id> [--topic <t>] [--brokers <ids>]                   Reassign partitions from a specific broker to other brokers.
  
```

## Important Note

The gem needs read/write access to the Kafka log directory for some operations (clean indexes).


