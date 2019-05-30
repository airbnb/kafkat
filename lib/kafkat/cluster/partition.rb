# frozen_string_literal: true

module Kafkat
  Partition = Struct.new(:topic_name, :id, :replicas, :leader, :isr) do
    def has_leader?(brokers)
      leader && leader != -1 && brokers.include?(leader)
    end

    def under_replicated?
      isr.size < replicas.size
    end
  end
end
