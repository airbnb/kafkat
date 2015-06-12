module Kafkat
  module Command
    class Drain < Base
      register_as 'drain'

      usage 'drain <broker id> [topic] [--brokers <ids>]',
            'Reassign partitions from a dead broker to healthy brokers.'

      def run
        dead_broker_id = ARGV[0] && ARGV.shift.to_i
        if dead_broker_id.nil?
          puts "You must specify a broker ID."
          exit 1
        end

        topic_name = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')
        topics = topic_name && zookeeper.get_topics([topic_name])
        topics ||= zookeeper.get_topics

        opts = Trollop.options do
          opt :brokers, "replica set (broker IDs)", type: :string
        end

        healthy_broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        healthy_broker_ids ||= zookeeper.get_brokers.values.map(&:id)
        healthy_broker_ids.delete(dead_broker_id)

        all_brokers = zookeeper.get_brokers
        all_brokers_id = all_brokers.values.map(&:id)

        healthy_broker_ids.each do |id|
          if !all_brokers_id.include?(id)
            print "ERROR: Broker #{id} is not currently active.\n"
            exit 1
          end
        end

        assignments = generate_assignments(dead_broker_id, topics, healthy_broker_ids)
        prompt_and_execute_assignments(assignments)
      end

      def generate_assignments(dead_broker_id, topics, healthy_broker_ids)
        assignments = []

        topics.each do |_, t|
          t.partitions.each do |p|
            if p.replicas.include? dead_broker_id
              replicas = p.replicas - [dead_broker_id]
              potential_broker_ids = healthy_broker_ids - replicas
              replicas << potential_broker_ids.sample unless potential_broker_ids.empty?
              assignments << Assignment.new(t.name, p.id, replicas)
            end
          end
        end

        assignments
      end
    end
  end
end
