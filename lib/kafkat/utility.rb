require 'kafkat/utility/formatting'
require 'kafkat/utility/command_io'

# Build a hash map from broker_id to number of partitions on it to facilitate
# finding the broker with lowest number of partitions to help balance brokers.
def build_num_partitions_on_broker_map(topic, all_broker_ids)
  num_partitions_on_broker = Hash[all_broker_ids.collect { |id| [id, 0] }]
  topic.partitions.each do |p|
    p.replicas.each do |r|
      num_partitions_on_broker[r] += 1
    end
  end
  num_partitions_on_broker
end
