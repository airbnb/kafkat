module Kafkat
  FactoryBot.define do
    factory :topic, class: Topic do
      name { 'topic_name' }

      factory :topic_with_one_empty_broker do
        partitions {[Partition.new(name, 0, [0], 0, [0]),
                     Partition.new(name, 1, [1], 1, [1]),
                     Partition.new(name, 2, [0], 0, [0]),
                     Partition.new(name, 3, [0], 0, [0]),
                     Partition.new(name, 4, [1], 1, [1])]}
      end

      factory :topic_rep_factor_one do
        partitions {[Partition.new(name, 0, [0], 0, [0]),
                     Partition.new(name, 1, [1], 1, [1]),
                     Partition.new(name, 2, [2], 2, [2]),
                     Partition.new(name, 3, [0], 0, [0]),
                     Partition.new(name, 4, [1], 1, [1])]}
      end

      factory :topic_rep_factor_two do
        partitions {[Partition.new(name, 0, [0, 1], 0, [0]),
                     Partition.new(name, 1, [0, 2], 2, [2]),
                     Partition.new(name, 2, [1, 2], 1, [1]),
                     Partition.new(name, 3, [0, 1], 0, [0]),
                     Partition.new(name, 4, [0, 2], 2, [2])]}
      end

      factory :topic_rep_factor_three do
        partitions {[Partition.new(name, 0, [0, 1, 2], 0, [0]),
                     Partition.new(name, 1, [0, 1, 2], 1, [1]),
                     Partition.new(name, 2, [0, 1, 2], 2, [2]),
                     Partition.new(name, 3, [0, 1, 2], 0, [0]),
                     Partition.new(name, 4, [0, 1, 2], 1, [1])]}
      end

      factory :topic_rep_factor_three_with_four_replicas_in_partition1 do
        name { 'topic_name1' }
        partitions { [Partition.new('topic_name1', 0, [0, 1, 2], 0, [0]),
                     Partition.new('topic_name1', 1, [0, 1, 2, 6], 1, [1]),
                     Partition.new('topic_name1', 2, [0, 1, 2], 2, [2])] }
      end

      factory :topic2_rep_factor_three do
        name { 'topic_name2' }
        partitions { [Partition.new('topic_name2', 0, [3, 4, 5], 0, [0]),
                     Partition.new('topic_name2', 1, [3, 4, 5], 0, [0]),
                     Partition.new('topic_name2', 2, [3, 4, 5], 1, [1])] }
      end
    end
  end
end
