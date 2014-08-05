module Kafkat
  class Topic < Struct.new(:name, :partitions)
  end
end
