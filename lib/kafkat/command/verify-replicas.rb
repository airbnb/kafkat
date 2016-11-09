module Kafkat
  module Command
    class VerifyReplicas < Base
      register_as 'verify-replicas'

      usage 'verify-replicas  [--topics] [--broker <id>] [--print-details] [--print-summary]',
            'Check if all partitions in a topic have same number of replicas.'

      def run
        opts = Trollop.options do
          opt :topics, "topic names", type: :string
          opt :broker, "broker ID", type: :string
          opt :print_details, "show replica size of mismatched partitions", :default => false
          opt :print_summary, "show summary of mismatched partitions", :default => false
        end

        topic_names = opts[:topics]
        print_details = opts[:print_details]
        print_summary = opts[:print_summary]

        if topic_names
          topics_list = topic_names.split(',')
          topics = zookeeper.get_topics(topics_list)
        end
        topics ||= zookeeper.get_topics
        broker = opts[:broker] && opts[:broker].to_i

        partition_replica_size, partition_replica_size_stat = verify_replicas(broker, topics)

        print_mismatched_partitions(partition_replica_size, partition_replica_size_stat, print_details, print_summary)
      end

      def verify_replicas(broker, topics)
        partition_replica_size = {}
        partition_replica_size_stat = {}

        topics.each do |_, t|
          partition_replica_size[t.name] = {}
          partition_replica_size_stat[t.name] = {}

          t.partitions.each do |p|
            replica_size = p.replicas.length

            next if broker && !(p.replicas.include? broker)

            if !partition_replica_size_stat[t.name].key? replica_size
              partition_replica_size_stat[t.name][replica_size] = 0
            end
            partition_replica_size_stat[t.name][replica_size] += 1

            partition_replica_size[t.name][p.id] = replica_size
          end

        end

        return partition_replica_size, partition_replica_size_stat
      end

      def print_mismatched_partitions(partition_replica_size, partition_replica_size_stat, print_details, print_summary)
        if print_details
          print "topic\tpartition\treplica_size\treplication_factor\n"

          partition_replica_size.each do |topic_name, partition|
            replication_factor = partition_replica_size_stat[topic_name].key(partition_replica_size_stat[topic_name].values.max)

            partition.each do |id, replica_size|
              if replica_size != replication_factor
                print "#{topic_name}\t#{id}\t#{replica_size}\t#{replication_factor}\n"
              end
            end
          end
        end

        if !print_details || print_summary
          print "topic\treplica_size\tcount\tpercentage\treplication_factor\n"
          partition_replica_size_stat.each do |topic_name, partition|
            if partition.values.size > 1
              replication_factor = partition_replica_size_stat[topic_name].key(partition_replica_size_stat[topic_name].values.max)
              num_partitions = 0.0
              partition.each { |key, value| num_partitions += value }

              partition.each do |replica_size, cnt|
                print "#{topic_name}\t#{replica_size}\t#{cnt}\t#{(cnt * 100 / num_partitions).to_i}%\t#{replication_factor}\n"
              end
            end
          end
        end
      end
    end
  end
end
