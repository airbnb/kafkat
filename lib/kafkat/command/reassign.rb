module Kafkat
  module Command
    class Reassign < Base
      register_as 'reassign'

      usage 'reassign [topic] [--brokers <ids>] [--replicas <n>] [--strategy smart|load-balanced]',
            'Begin reassignment of partitions.\n'\
            'Default smart strategy minimizes data moved around, load-balanced strategy randomly assign partitions.'

      def run
        topic_name = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')

        all_brokers = zookeeper.get_brokers
        topics = topic_name && zookeeper.get_topics([topic_name])
        topics ||= zookeeper.get_topics

        opts = Trollop.options do
          opt :brokers, "replica set (broker IDs)", type: :string
          opt :replicas, "number of replicas (count)", type: :integer
          opt :strategy, "assignment strategy used", type: :string
        end

        destination_broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        replica_count = opts[:replicas]
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
          assignment_strategy = SmartStrategy.new()
        elsif strategy == "load-balanced"
          assignment_strategy = LoadBalancedStrategy.new()
        else
          print "ERROR: Unrecognized strategy \"#{strategy}\".\n"
          exit 1
        end

        broker_count = destination_broker_ids.size
        assignments = []

        topics.each do |_, topic|
          topic_replica_count = get_topic_replica_count(topic, replica_count)
          if topic_replica_count > broker_count
            print "ERROR: Replication factor (#{topic_replica_count}) "\
                  "is larger than brokers (#{broker_count}).\n"
            exit 1
          end

          assignments += assignment_strategy.generate_topic_assignment(
            topic, topic_replica_count, destination_broker_ids, replica_count, all_broker_ids)
        end

        prompt_and_execute_assignments(assignments)
      end
    end

    # This is how Kafka's AdminUtils determines these values.
    def get_topic_replica_count(topic, replica_count)
      if topic.partitions.empty?
        print "ERROR: Topic \"#{topic.name}\" has no partition.\n"
        exit 1
      end
      replica_count || topic.partitions[0].replicas.size
    end

    class AssignmentStrategy
      def generate_topic_assignment(
        topic, topic_replica_count, destination_broker_ids, all_broker_ids, broker_count)

        raise NotImplementedError.new()
      end
    end

    class LoadBalancedStrategy < AssignmentStrategy
      # *** This logic is duplicated from Kakfa 0.8.1.1 ***
      def generate_topic_assignment(
        topic, topic_replica_count, destination_broker_ids, _, broker_count)

        assignment = []
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
      #   1. replicas should be evenly distributed among brokers;
      #   2. replicas moving around should be avoided as possible.
      # We also need to address that user can sepcify both number of brokers and
      # replication factors.
      def generate_topic_assignment(
        topic, topic_replica_count, destination_broker_ids, all_broker_ids, _)

        assignment = []

        partition_count = topic.partitions.size
        num_partitions_on_broker = build_num_partitions_on_broker_map(topic, all_broker_ids)
        partitions_quota_on_broker = generate_partitions_quota_on_broker(
          partition_count, topic_replica_count, destination_broker_ids, num_partitions_on_broker)

        remove_replicas_with_overfilled_broker_and_partition(
          topic,
          topic_replica_count,
          num_partitions_on_broker,
          partitions_quota_on_broker,
          destination_broker_ids)

        topic.partitions.each do |p|
          available_brokers = find_available_brokers(
            destination_broker_ids, p, num_partitions_on_broker, partitions_quota_on_broker)

          remove_assignment_with_overfilled_broker(
            p,
            available_brokers,
            topic_replica_count,
            num_partitions_on_broker,
            partitions_quota_on_broker)

          fill_underfilled_partition(
            p, topic_replica_count, available_brokers, num_partitions_on_broker)
          trim_overfilled_partition(p, topic_replica_count, num_partitions_on_broker)

          assignment << Assignment.new(topic.name, p.id, p.replicas)
        end

        assignment
      end

    private
      # Assign quota to brokers.
      # Quota is evenly distributed among detination brokers where as those destination brokers
      # with more replicas initially might be assigned with one more if quota cannot be
      # distrbuted completely evenly.
      def generate_partitions_quota_on_broker(
        partition_count,
        topic_replica_count,
        destination_broker_ids,
        num_partitions_on_broker)

        total_partitions = partition_count * topic_replica_count
        min_replicas_per_broker = total_partitions / destination_broker_ids.size
        remaining_replicas_quota = total_partitions % destination_broker_ids.size

        partitions_quota_on_broker = Hash.new(0)
        destination_broker_ids.each do |id|
          partitions_quota_on_broker[id] = min_replicas_per_broker
        end
        num_partitions_on_broker.select { |id, _| destination_broker_ids.include? id }
          .sort_by { |id, num| -num }.first(remaining_replicas_quota).each do |id, _|

          partitions_quota_on_broker[id] += 1
        end

        partitions_quota_on_broker
      end

      # Existing assignments where partitions with too many replicas ( due to decrese of
      # replication factor) and brokers with too many replicas (due to decrease of replication
      # factor or increase of number of brokers) are removed.
      # Brokers not in destination brokers are also removed.
      def remove_replicas_with_overfilled_broker_and_partition(
        topic,
        topic_replica_count,
        num_partitions_on_broker,
        partitions_quota_on_broker,
        destination_broker_ids)

        topic.partitions.each do |p|
          p.replicas.delete_if do |broker|
            if (p.replicas.size > topic_replica_count &&
                num_partitions_on_broker[broker] > partitions_quota_on_broker[broker]) ||
               !destination_broker_ids.include?(broker)

              num_partitions_on_broker[broker] -= 1
              true
            end
          end
        end
      end

      # Find brokers not assigned with the partition and not meet quota
      def find_available_brokers(
        destination_broker_ids, partition, num_partitions_on_broker, partitions_quota_on_broker)

        available_brokers = destination_broker_ids - partition.replicas
        available_brokers.delete_if do |broker|
          num_partitions_on_broker[broker] >= partitions_quota_on_broker[broker]
        end
        available_brokers
      end

      # Remove broker exceeding quota if enough available brokers exit.
      # If not enough brokers available while existing broker exceeding quota, this means the broker
      # is overfilled with other partitions, and will meet quota in later iteration.
      def remove_assignment_with_overfilled_broker(
        partition,
        available_brokers,
        topic_replica_count,
        num_partitions_on_broker,
        partitions_quota_on_broker)

        partition.replicas.delete_if do |broker|
          if partition.replicas.size + available_brokers.size > topic_replica_count &&
            num_partitions_on_broker[broker] > partitions_quota_on_broker[broker]

            num_partitions_on_broker[broker] -= 1
            true
          end
        end
      end

      def fill_underfilled_partition(
        partition, topic_replica_count, available_brokers, num_partitions_on_broker)

        while partition.replicas.size < topic_replica_count
          assigned_broker = available_brokers.shift
          num_partitions_on_broker[assigned_broker] += 1
          partition.replicas << assigned_broker
        end
      end

      def trim_overfilled_partition(partition, topic_replica_count, num_partitions_on_broker)
        while partition.replicas.size > topic_replica_count
          removed_broker = partition.replicas.shift
          num_partitions_on_broker[removed_broker] -= 1
        end
      end
    end
  end
end
