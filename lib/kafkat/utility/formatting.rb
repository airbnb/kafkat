module Kafkat
  module Formatting
    def justify(field, width=2)
      field = field.to_s
      count = [width - (field.length / 8), 0].max
      field + "\t" * count
    end

    def print_broker(broker)
      print justify(broker.id)
      print justify("#{broker.host}:#{broker.port}")
      print "\n"
    end

    def print_broker_header
      print justify('Broker')
      print justify('Socket')
      print "\n"
    end

    def print_topic(topic)
      print justify(topic.name)
      print "\n"
    end

    def print_topic_header
      print justify('Topic')
      print "\n"
    end

    def print_partition(partition)
      print justify(partition.topic_name)
      print justify(partition.id)
      print justify(partition.leader || 'none')
      print justify(partition.replicas.inspect, 3)
      print justify(partition.isr.inspect, 3)
      print "\n"
    end

    def print_partition_header
      print justify('Topic')
      print justify('Partition')
      print justify('Leader')
      print justify('Replicas', 3)
      print justify('ISRs', 3)
      print "\n"
    end

    def print_assignment(assignment)
      print justify(assignment.topic_name)
      print justify(assignment.partition_id)
      print justify(assignment.replicas.inspect)
      print "\n"
    end

    def print_assignment_header
      print justify('Topic')
      print justify('Partition')
      print justify('Replicas')
      print "\n"
    end
  end
end
