module Kafkat
  module Command
    class Reassign < Base
      register_as 'reassign'

      usage 'reassign [topics] [--brokers <ids>] [--replicas <n>]',
            'Begin reassignment of partitions.'

      def run
        topic_names = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')

        all_brokers = zookeeper.get_brokers

        topics = nil
        if topic_names
           topics_list = topic_names.split(',')
           topics = zookeeper.get_topics(topics_list)
        end
        topics ||= zookeeper.get_topics

        opts = Optimist.options do
          opt :brokers, "replica set (broker IDs)", type: :string
          opt :replicas, "number of replicas (count)", type: :integer
        end

        broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        replica_count = opts[:replicas]

        broker_ids ||= zookeeper.get_brokers.values.map(&:id)

        all_brokers_id = all_brokers.values.map(&:id)
        broker_ids.each do |id|
          if !all_brokers_id.include?(id)
            print "ERROR: Broker #{id} is not currently active.\n"
            exit 1
          end
        end

        # *** This logic is duplicated from Kakfa 0.8.1.1 ***

        assignments = []
        broker_count = broker_ids.size

        topics.each do |_, t|
          # This is how Kafka's AdminUtils determines these values.
          partition_count = t.partitions.size
          topic_replica_count = replica_count || t.partitions[0].replicas.size

          if topic_replica_count > broker_count
            print "ERROR: Replication factor (#{topic_replica_count}) is larger than brokers (#{broker_count}).\n"
            exit 1
          end

          start_index = Random.rand(broker_count)
          replica_shift = Random.rand(broker_count)

          t.partitions.each do |p|
            replica_shift += 1 if p.id > 0 && p.id % broker_count == 0
            first_replica_index = (p.id + start_index) % broker_count

            replicas = [broker_ids[first_replica_index]]

            (0...topic_replica_count-1).each do |i|
              shift = 1 + (replica_shift + i) % (broker_count - 1)
              index = (first_replica_index + shift) % broker_count
              replicas << broker_ids[index]
            end

            replicas.reverse!
            assignments << Assignment.new(t.name, p.id, replicas)
          end
        end

        # ****************

        prompt_and_execute_assignments(assignments)
      end
    end
  end
end
