require 'spec_helper'

module Kafkat
  RSpec.describe Command::VerifyReplicas do
    let(:verify_replicas) { Command::VerifyReplicas.new({}) }

    context 'two topics with replication factor 3' do
      let(:topic_rep_factor_three_with_four_replicas_in_partition1) {
        FactoryGirl.build(:topic_rep_factor_three_with_four_replicas_in_partition1) }
      let(:topic2_rep_factor_three) { FactoryGirl.build(:topic2_rep_factor_three) }

      it 'should return empty mismatched partitions for all brokers' do
        mismatched_partitions = verify_replicas.verify_replicas(nil,
                                                                {"topic_name2" => topic2_rep_factor_three})
        expect(mismatched_partitions). to eq([])
      end

      it 'should return partition 1 for all brokers' do
          mismatched_partitions = verify_replicas.verify_replicas(nil,
                                          {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                           "topic_name2" => topic2_rep_factor_three})
          expect(mismatched_partitions). to eq(["topic_name\t1\t3\n"])
      end

      it 'should return partition 1 for broker 6' do
        mismatched_partitions = verify_replicas.verify_replicas(6,
                                                                {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                                                 "topic_name2" => topic2_rep_factor_three})
        expect(mismatched_partitions). to eq(["topic_name\t1\t3\n"])
      end

      it 'should return empty mismatched partition for broker 3' do
        mismatched_partitions = verify_replicas.verify_replicas(3,
                                                                {"topic_name1" => topic_rep_factor_three_with_four_replicas_in_partition1,
                                                                 "topic_name2" => topic2_rep_factor_three})
        expect(mismatched_partitions). to eq([])
      end
    end





  end
end
