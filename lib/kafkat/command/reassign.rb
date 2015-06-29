module Kafkat
  module Command
    class Reassign < Base
      register_as 'reassign'

      usage 'reassign [topic] [--brokers <ids>] [--strategy smart|load-balanced]',
            'Begin reassignment of partitions.\n'\
            'Default smart strategy minimizes data moved around, '\
            'load-balanced strategy randomly assign partitions.'

      def run
        topic_name = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')

        all_brokers = zookeeper.get_brokers
        topics = topic_name && zookeeper.get_topics([topic_name])
        topics ||= zookeeper.get_topics

        opts = Trollop.options do
          opt :brokers, "replica set (broker IDs)", type: :string
          opt :replicas, "NO longer support changing replication factor through this command",
            type: :integer
          opt :strategy, "assignment strategy used", type: :string
        end

        if opts[:replicas]
          print "ERROR: No longer support changing replication factor through this command. "\
                "Use `set-replication-factor` instead.\n"
          exit 1
        end

        destination_broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        strategy = opts[:strategy]
        strategy ||= "smart"

        destination_broker_ids ||= zookeeper.get_brokers.values.map(&:id)

        all_broker_ids = all_brokers.values.map(&:id)
        destination_broker_ids.each do |id|
          if !all_broker_ids.include? id
            print "ERROR: Broker #{id} is not currently active.\n"
            exit 1
          end
        end

        if strategy == "smart"
          assignment_strategy = SmartStrategy.new
        elsif strategy == "load-balanced"
          assignment_strategy = LoadBalancedStrategy.new
        else
          print "ERROR: Unrecognized strategy \"#{strategy}\".\n"
          exit 1
        end

        broker_count = destination_broker_ids.size
        assignments = []

        topics.each do |_, topic|
          topic_replica_count = get_topic_replica_count(topic)
          if topic_replica_count > broker_count
            print "ERROR: Replication factor (#{topic_replica_count}) "\
                  "is larger than brokers (#{broker_count}).\n"
            exit 1
          end

          assignments += assignment_strategy.generate_topic_assignment(
            topic, topic_replica_count, destination_broker_ids, all_broker_ids, broker_count)
        end

        prompt_and_execute_assignments(assignments)
      end
    end

    # This is how Kafka's AdminUtils determines these values.
    def get_topic_replica_count(topic)
      if topic.partitions.empty?
        print "ERROR: Topic \"#{topic.name}\" has no partition.\n"
        exit 1
      end
      topic.partitions[0].replicas.size
    end

    class AssignmentStrategy
      def generate_topic_assignment(topic, topic_replica_count, destination_broker_ids)

        raise NotImplementedError.new
      end
    end

    class LoadBalancedStrategy < AssignmentStrategy
      # *** This logic is duplicated from Kakfa 0.8.1.1 ***
      def generate_topic_assignment(topic, topic_replica_count, destination_broker_ids)

        assignment = []
        broker_count = destination_broker_ids.size
        start_index = Random.rand(broker_count)
        replica_shift = Random.rand(broker_count)

        topic.partitions.each do |p|
          replica_shift += 1 if p.id > 0 && p.id % broker_count == 0
          first_replica_index = (p.id + start_index) % broker_count

          replicas = [destination_broker_ids[first_replica_index]]

          (0...topic_replica_count-1).each do |i|
            shift = 1 + (replica_shift + i) % (broker_count - 1)
            index = (first_replica_index + shift) % broker_count
            replicas << destination_broker_ids[index]
          end

          replicas.reverse!
          assignment << Assignment.new(topic.name, p.id, replicas)
        end

        assignment
      end
    end

    class SmartStrategy < AssignmentStrategy
      # This is the default assignment generation strategy.
      # There are two requirements:
      #   1. replicas should be as evenly distributed among brokers as possible;
      #   2. replicas moving around should be avoided as possible.
      def generate_topic_assignment(topic, topic_replica_count, destination_broker_ids)

        remove_unused_brokers(topic, destination_broker_ids)
        num_partitions_on_broker = build_num_partitions_on_broker_map(topic, destination_broker_ids)
        remove_overfilled_brokers(
          topic, topic_replica_count, destination_broker_ids, num_partitions_on_broker)
        fill_underfilled_partitions(
          topic, topic_replica_count, destination_broker_ids, num_partitions_on_broker)

        assignment = []
        topic.partitions.each do |p|
          assignment << Assignment.new(topic.name, p.id, p.replicas)
        end
        assignment
      end

    private
      def remove_unused_brokers(topic, destination_broker_ids)
        topic.partitions.each do |p|
          p.replicas.delete_if { |broker| !destination_broker_ids.include?(broker) }
        end
      end

      # Remove overfilled brokers from partitions with most replicas.
      def remove_overfilled_brokers(
        topic, topic_replica_count, destination_broker_ids, num_partitions_on_broker)

        partition_count = topic.partitions.size
        total_partitions = partition_count * topic_replica_count
        min_replicas_per_broker = total_partitions / destination_broker_ids.size
        num_partitions_on_broker.select { |_, num| num > min_replicas_per_broker }.each do |id, _|
          topic.partitions
            .select { |p| p.replicas.include? id }
            .sort_by { |p| -p.replicas.size }
            .first(num_partitions_on_broker[id] - min_replicas_per_broker).each do |p|

            p.replicas.delete(id)
          end
          num_partitions_on_broker[id] = min_replicas_per_broker
        end
      end

      # Fill the partition with least number of replicas with brokers with least number partitions.
      def fill_underfilled_partitions(
        topic, topic_replica_count, destination_broker_ids, num_partitions_on_broker)

        while p = topic.partitions.select { |p| p.replicas.size < topic_replica_count}
          .sort_by { |p| p.replicas.size }.first

          assigned_broker =
            (destination_broker_ids - p.replicas).sort_by { |id| num_partitions_on_broker[id]}.first
          p.replicas << assigned_broker
          num_partitions_on_broker[assigned_broker] += 1
        end
      end
    end
  end
end
