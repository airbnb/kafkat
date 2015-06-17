module Kafkat
  class Assignment < Struct.new(:topic_name, :partition_id, :replicas)
  end
end
