module Kafkat
  module Command
    class Drain < Base
      register_as 'drain'

      usage 'drain <broker id> [--topic <t>] [--brokers <ids>]',
            'Reassign partitions from a specific broker to destination brokers.'

      # For each partition (of speicified topic) on the source broker, the command is to
      # assign the partition to one of the destination brokers that does not already have
      # this partition, along with existing brokers to achieve minimal movement of data.
      # To help distribute data evenly, if there are more than one destination brokers
      # meet the requirement, the command will always choose the brokers with the lowest
      # number of partitions of the involving topic.
      #
      # In order to find out the broker with lowest number of partitions, the command maintain
      # a hash table with broker id as key and number of partitions as value. The hash table
      # will be updated along with assignment.
      def run
        source_broker_id = ARGV[0] && ARGV.shift.to_i
        if source_broker_id.nil?
          puts "You must specify a broker ID."
          exit 1
        end

        opts = Trollop.options do
          opt :brokers, "destination broker IDs", type: :string
          opt :topic,   "topic name to reassign", type: :string
        end

        topic_name = opts[:topic]
        topics = topic_name && zookeeper.get_topics([topic_name])
        topics ||= zookeeper.get_topics

        destination_broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        destination_broker_ids ||= zookeeper.get_brokers.values.map(&:id)
        destination_broker_ids.delete(source_broker_id)
        all_active_broker_ids = zookeeper.get_brokers.values.map(&:id)

        unless (inactive_broker_ids = destination_broker_ids - all_active_broker_ids).empty?
          print "ERROR: Broker #{inactive_broker_ids} are not currently active.\n"
          exit 1
        end

        assignments = generate_assignments(
          source_broker_id, topics, destination_broker_ids, all_active_broker_ids)
        prompt_and_execute_assignments(assignments)
      end

      def generate_assignments(
        source_broker_id, topics, destination_broker_ids, all_active_broker_ids)

        assignments = []
        topics.each do |_, t|
          num_partitions_on_broker = build_num_partitions_on_broker_map(t, all_active_broker_ids)

          t.partitions.each do |p|
            if p.replicas.include? source_broker_id
              replicas = p.replicas - [source_broker_id]
              potential_broker_ids = destination_broker_ids - replicas
              if potential_broker_ids.empty?
                print "ERROR: Not enough destination brokers to reassign topic \"#{t.name}\".\n"
                exit 1
              end

              num_partitions_on_potential_broker =
                num_partitions_on_broker.select { |id, _| potential_broker_ids.include? id }
              assigned_broker_id = num_partitions_on_potential_broker.min_by{ |id, num| num }[0]
              replicas << assigned_broker_id
              num_partitions_on_broker[assigned_broker_id] += 1

              assignments << Assignment.new(t.name, p.id, replicas)
            end
          end
        end

        assignments
      end

      # Build a hash map from broker_id to number of partitions on it to facilitate
      # finding the broker with lowest number of partitions to help balance brokers.
      def build_num_partitions_on_broker_map(topic, all_active_broker_ids)
        num_partitions_on_broker = Hash.new(0)
        all_active_broker_ids.each { |id| num_partitions_on_broker[id] = 0 }
        topic.partitions.each do |p|
          p.replicas.each do |r|
            num_partitions_on_broker[r] += 1
          end
        end
        num_partitions_on_broker
      end
    end
  end
end
