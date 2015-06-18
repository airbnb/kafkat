module Kafkat
  FactoryGirl.define do
    factory :topic, class:Topic do
      name "topic_name"
      partitions Hash.new()

      factory :topic_with_one_empty_broker do
        partitions {[Partition.new(name, 0, [0], 0, 0),
                     Partition.new(name, 1, [1], 1, 1),
                     Partition.new(name, 2, [0], 0, 0),
                     Partition.new(name, 3, [0], 0, 0),
                     Partition.new(name, 4, [1], 1, 1)]}
      end

      factory :topic_rep_factor_one do
        partitions {[Partition.new(name, 0, [0], 0, 0),
                     Partition.new(name, 1, [1], 1, 1),
                     Partition.new(name, 2, [2], 2, 2),
                     Partition.new(name, 3, [0], 0, 0),
                     Partition.new(name, 4, [1], 1, 1)]}
      end

      factory :topic_rep_factor_two do
        partitions {[Partition.new(name, 0, [0, 1], 0, 0),
                     Partition.new(name, 1, [0, 2], 2, 2),
                     Partition.new(name, 2, [1, 2], 1, 1),
                     Partition.new(name, 3, [0, 1], 0, 0),
                     Partition.new(name, 4, [0, 2], 2, 2)]}
      end

      factory :topic_rep_factor_three do
        partitions {[Partition.new(name, 0, [0, 1, 2], 0, 0),
                     Partition.new(name, 1, [0, 1, 2], 1, 1),
                     Partition.new(name, 2, [0, 1, 2], 2, 2),
                     Partition.new(name, 3, [0, 1, 2], 0, 0),
                     Partition.new(name, 4, [0, 1, 2], 1, 1)]}
      end

      factory :skewed_topic do
        partitions {[Partition.new(name, 0, [0, 1], 0, 0),
                     Partition.new(name, 1, [], 1, 1)]}
      end

      factory :topic_not_distributed_evenly do
        partitions {[Partition.new(name, 0, [0, 1], 0, 0),
                     Partition.new(name, 1, [0, 1], 1, 1),
                     Partition.new(name, 2, [0, 1], 1, 1),
                     Partition.new(name, 3, [0, 1], 0, 0),
                     Partition.new(name, 4, [0, 1], 1, 1)]}
      end
    end
  end
end