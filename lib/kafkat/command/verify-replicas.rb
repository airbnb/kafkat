module Kafkat
  module Command
    class VerifyReplicas < Base
      register_as 'verify-replicas'

      usage 'verify-replicas  [--topics] [--broker <id>]',
            'Check if all partitions in a topic have same number of replicas.'

      def run
        opts = Trollop.options do
          opt :topics, "topic names", type: :string
          opt :broker, "broker ID", type: :string
        end

        topic_names = opts[:topics]
        if topic_names
          topics_list = topic_names.split(',')
          topics = zookeeper.get_topics(topics_list)
        end
        topics ||= zookeeper.get_topics
        broker = opts[:broker] && opts[:broker].to_i

        mismatched_partitions = verify_replicas(broker, topics)

        if mismatched_partitions.length > 0
          print_mismatched_partitions(mismatched_partitions)
          exit 1
        else
          print "Num of replicas of a topic are same.\n"
        end
      end

      def verify_replicas(broker, topics)
        err_message = []

        topics.each do |_, t|
          replication_factor = t.partitions[0].replicas.length

          t.partitions.each do |p|
            next if broker && !(p.replicas.include? broker)

            if p.replicas.length != replication_factor
              err_message << "#{t.name}\t#{p.id}\t#{replication_factor}\n"
            end
          end
        end

        return err_message
      end

      def print_mismatched_partitions(mismatched_partitions)
        print_err("ERROR: Num of replicas of a topic are different\n")
        print_err("Topic\tpartition\tnumber of replicas\n")

        mismatched_partitions.each do |partition|
          print_err "#{partition}"
        end
      end

    end
  end
end
