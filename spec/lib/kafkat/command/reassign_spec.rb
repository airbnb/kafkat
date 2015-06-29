require 'spec_helper'

module Kafkat
  describe Command::Reassign do
    let(:load_balanced_strategy) { Command::LoadBalancedStrategy.new }
    let(:smart_strategy) { Command::SmartStrategy.new }
    let(:topic_not_distributed_evenly) { FactoryGirl.build(:topic_not_distributed_evenly) }
    let(:topic_not_distributed_evenly2) { FactoryGirl.build(:topic_not_distributed_evenly2) }
    let(:topic_rep_factor_one) { FactoryGirl.build(:topic_rep_factor_one) }
    let(:topic_rep_factor_two) { FactoryGirl.build(:topic_rep_factor_two) }
    let(:topic_rep_factor_three) { FactoryGirl.build(:topic_rep_factor_three) }

    def check_evenly_distributed(count, replication_factor, destination_brokers, num_partitions)
      total_replicas = replication_factor * num_partitions
      expect(count.inject(0) {|sum, pair| sum + pair[1]}).to eq(total_replicas)
      min_replicas_per_broker = total_replicas / destination_brokers.size
      count.each do |_, num|
        expect([min_replicas_per_broker, min_replicas_per_broker + 1]).to include(num)
      end
    end

    def count(assignments)
      counts = Hash.new(0)
      assignments.each do |a|
        a.replicas.each do |id|
          counts[id] += 1
        end
      end
      counts
    end

    context 'strategy load-balanced' do
      it 'should evenly distribute replicas randomly' do
        assignments = load_balanced_strategy.generate_topic_assignment(
          topic_rep_factor_two,
          2,
          [1, 2])
        expect(assignments).to have_exactly(5).Partitions
        assignments.each do |a|
          expect(a).to have_exactly(2).replicas
        end
      end
    end

    context 'default strategy smart' do
      it 'should evenly distribute replicas and minimize data movement' do
        assignments = smart_strategy.generate_topic_assignment(
          topic_rep_factor_one,
          1,
          [1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(1)
        expect(assignments[1].replicas.uniq.length).to eq(1)
        expect(assignments[2].replicas.uniq.length).to eq(1)
        expect(assignments[3].replicas.uniq.length).to eq(1)
        expect(assignments[4].replicas.uniq.length).to eq(1)
        check_evenly_distributed(count(assignments), 1, [1, 2], 5)
      end

      it 'should handle unevenly distribution properly' do
        assignments = smart_strategy.generate_topic_assignment(
          topic_not_distributed_evenly,
          2,
          [1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(2)
        expect(assignments[1].replicas.uniq.length).to eq(2)
        expect(assignments[2].replicas.uniq.length).to eq(2)
        expect(assignments[3].replicas.uniq.length).to eq(2)
        expect(assignments[4].replicas.uniq.length).to eq(2)
        check_evenly_distributed(count(assignments), 2, [1, 2], 5)

        assignments2 = smart_strategy.generate_topic_assignment(
          topic_not_distributed_evenly,
          2,
          [0, 1, 2])
        expect(assignments2).to have_exactly(5).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(2)
        expect(assignments[1].replicas.uniq.length).to eq(2)
        expect(assignments[2].replicas.uniq.length).to eq(2)
        expect(assignments[3].replicas.uniq.length).to eq(2)
        expect(assignments[4].replicas.uniq.length).to eq(2)
        check_evenly_distributed(count(assignments2), 2, [0, 1, 2], 5)

        assignments3 = smart_strategy.generate_topic_assignment(
          topic_not_distributed_evenly2,
          2,
          [0, 2, 3])
        expect(assignments3).to have_exactly(2).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(2)
        expect(assignments[1].replicas.uniq.length).to eq(2)
        check_evenly_distributed(count(assignments3), 2, [0, 2, 3], 2)
      end

      it 'should handle more brokers properly' do
        assignments = smart_strategy.generate_topic_assignment(
          topic_rep_factor_one,
          1,
          [0, 1, 2, 3])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(1)
        expect(assignments[1].replicas.uniq.length).to eq(1)
        expect(assignments[2].replicas.uniq.length).to eq(1)
        expect(assignments[3].replicas.uniq.length).to eq(1)
        expect(assignments[4].replicas.uniq.length).to eq(1)
        check_evenly_distributed(count(assignments), 1, [0, 1, 2, 3], 5)

        assignments2 = smart_strategy.generate_topic_assignment(
          topic_rep_factor_two,
          2,
          [0, 1, 2, 3])
        expect(assignments2).to have_exactly(5).Partitions
        expect(assignments2[0].replicas.uniq.length).to eq(2)
        expect(assignments2[1].replicas.uniq.length).to eq(2)
        expect(assignments2[2].replicas.uniq.length).to eq(2)
        expect(assignments2[3].replicas.uniq.length).to eq(2)
        expect(assignments2[4].replicas.uniq.length).to eq(2)
        check_evenly_distributed(count(assignments2), 2, [0, 1, 2, 3], 5)

        assignments3 = smart_strategy.generate_topic_assignment(
          topic_rep_factor_three,
          3,
          [0, 1, 2, 3])
        expect(assignments3).to have_exactly(5).Partitions
        expect(assignments3[0].replicas.uniq.length).to eq(3)
        expect(assignments3[1].replicas.uniq.length).to eq(3)
        expect(assignments3[2].replicas.uniq.length).to eq(3)
        expect(assignments3[3].replicas.uniq.length).to eq(3)
        expect(assignments3[4].replicas.uniq.length).to eq(3)
        check_evenly_distributed(count(assignments3), 3, [0, 1, 2, 3], 5)

      end

      it 'should handle less brokers properly' do
        assignments = smart_strategy.generate_topic_assignment(
          topic_rep_factor_one,
          1,
          [1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas.uniq.length).to eq(1)
        expect(assignments[1].replicas.uniq.length).to eq(1)
        expect(assignments[2].replicas.uniq.length).to eq(1)
        expect(assignments[3].replicas.uniq.length).to eq(1)
        expect(assignments[4].replicas.uniq.length).to eq(1)
        check_evenly_distributed(count(assignments), 1, [1, 2], 5)

        assignments2 = smart_strategy.generate_topic_assignment(
          topic_rep_factor_two,
          2,
          [1, 2])
        expect(assignments2).to have_exactly(5).Partitions
        expect(assignments2[0].replicas.uniq.length).to eq(2)
        expect(assignments2[1].replicas.uniq.length).to eq(2)
        expect(assignments2[2].replicas.uniq.length).to eq(2)
        expect(assignments2[3].replicas.uniq.length).to eq(2)
        expect(assignments2[4].replicas.uniq.length).to eq(2)
        check_evenly_distributed(count(assignments2), 2, [1, 2], 5)
      end
    end
  end
end
