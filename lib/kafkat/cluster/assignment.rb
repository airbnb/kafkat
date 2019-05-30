# frozen_string_literal: true

module Kafkat
  Assignment = Struct.new(:topic_name, :partition_id, :replicas)
end
