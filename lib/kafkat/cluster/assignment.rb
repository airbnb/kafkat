module Kafkat
  class Assignment < Struct.new(:partition, :replicas)
  end
end
