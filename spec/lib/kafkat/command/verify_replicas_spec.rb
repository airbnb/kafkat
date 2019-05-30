require 'spec_helper'

module Kafkat
  RSpec.describe Command::VerifyReplicas do
    let(:verify_replicas) { Command::VerifyReplicas.new({}) }

    context 'two topics with replication factor 3' do
      let(:topic_rep_factor_three_with_four_replicas_in_partition1) {
        FactoryBot.build(:topic_rep_factor_three_with_four_replicas_in_partition1) }
      let(:topic2_rep_factor_three) { FactoryBot.build(:topic2_rep_factor_three) }

      it 'should return empty mismatched partitions for all brokers' do
        partition_replica_size, partition_replica_size_stat = verify_replicas.verify_replicas(nil,
                                                                                              {"topic_name2" => topic2_rep_factor_three})

        expect(partition_replica_size).to eq({"topic_name2" => {0 => 3, 1 => 3, 2 => 3}})
        expect(partition_replica_size_stat).to eq({"topic_name2" => {3 => 3}})
      end

      it 'should return topic 1 partition 1 for all brokers' do
        partition_replica_size, partition_replica_size_stat = verify_replicas.verify_replicas(nil,
                                                                                              {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                                                                               "topic_name2" => topic2_rep_factor_three})

        expect(partition_replica_size).to eq({"topic_name1" => {0 => 3, 1 => 4, 2 => 3}, "topic_name2" => {0 => 3, 1 => 3, 2 => 3}})
        expect(partition_replica_size_stat).to eq({"topic_name1" => {3 => 2, 4 => 1}, "topic_name2" => {3 => 3}})
      end

      it 'should return topic 1 partition 1 for broker 6' do
        partition_replica_size, partition_replica_size_stat = verify_replicas.verify_replicas(6,
                                                                                              {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                                                                               "topic_name2" => topic2_rep_factor_three})

        expect(partition_replica_size).to eq({"topic_name1" => {1 => 4}, "topic_name2" => {}})
        expect(partition_replica_size_stat).to eq({"topic_name1" => {4 => 1}, "topic_name2" => {}})
      end

      it 'should return empty mismatched partition for broker 3' do
        partition_replica_size, partition_replica_size_stat = verify_replicas.verify_replicas(3,
                                                                                              {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                                                                               "topic_name2" => topic2_rep_factor_three})

        expect(partition_replica_size).to eq({"topic_name1" => {}, "topic_name2" => {0 => 3, 1 => 3, 2 => 3}})
        expect(partition_replica_size_stat).to eq({"topic_name1" => {}, "topic_name2" => {3 => 3}})
      end
    end


  end
end
