require 'spec_helper'

module Kafkat
  RSpec.describe Command::Reassign do
    let(:reassign) { Command::Reassign.new({}) }
    let(:skewed_topic) { FactoryGirl.build(:skewed_topic) }
    let(:topic_not_distributed_evenly) { FactoryGirl.build(:topic_not_distributed_evenly) }
    let(:topic_rep_factor_one) { FactoryGirl.build(:topic_rep_factor_one) }
    let(:topic_rep_factor_two) { FactoryGirl.build(:topic_rep_factor_two) }

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
        assignments = reassign.generate_assignment_load_balanced(
          {"topic_name" => topic_rep_factor_two},
          [1, 2],
          2)
        expect(assignments).to have_exactly(5).Partitions
        assignments.each do |a|
          expect(a).to have_exactly(2).replicas
        end
      end
    end

    context 'default strategy smart' do
      it 'should evenly distribute replicas and minimize data movement' do
        assignments = reassign.generate_assignment_smart(
          {"topic_name" => topic_rep_factor_one},
          [1, 2],
          1,
          [0, 1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect([[1], [2]]).to include(assignments[0].replicas)
        expect(assignments[1].replicas).to eq([1])
        expect([[1], [2]]).to include(assignments[2].replicas)
        expect(assignments[3].replicas).to eq([2])
        expect(assignments[4].replicas).to eq([1])
      end

      it 'should handle unevenly distribution properly' do
        assignments = reassign.generate_assignment_smart(
          {"topic_name" => topic_not_distributed_evenly},
          [1, 2],
          1,
          [0, 1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas).to eq([2])
        expect(assignments[1].replicas).to eq([2])
        expect(assignments[2].replicas).to eq([1])
        expect(assignments[3].replicas).to eq([1])
        expect(assignments[4].replicas).to eq([1])
      end

      it 'should handle more replications and brokers properly' do
        assignments = reassign.generate_assignment_smart(
          {"topic_name" => topic_rep_factor_one},
          [0, 1, 2, 3],
          2,
          [0, 1, 2, 3])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas).to include(0)
        expect(assignments[1].replicas).to include(1)
        expect(assignments[2].replicas).to include(2)
        expect(assignments[3].replicas).to include(0)
        expect(assignments[4].replicas).to include(1)

        counts = count(assignments)
        expect(counts).to eq({0 => 3, 1 =>3, 2=>2, 3=>2})
      end

      it 'should handle less replication and brokers properly' do
        assignments = reassign.generate_assignment_smart(
          {"topic_name" => topic_rep_factor_two},
          [1, 2],
          1,
          [0, 1, 2])
        expect(assignments).to have_exactly(5).Partitions
        expect(assignments[0].replicas).to eq([1])
        expect(assignments[1].replicas).to eq([2])
        expect([[1], [2]]).to include(assignments[2].replicas)
        expect(assignments[3].replicas).to eq([1])
        expect(assignments[4].replicas).to eq([2])
      end

      it 'should handle skewed topic properly' do
        assignments = reassign.generate_assignment_smart(
          {"topic_name" => skewed_topic},
          [0, 1],
          1,
          [0, 1])
        expect(assignments).to have_exactly(2).Partitions
        counts = count(assignments)
        expect(counts).to eq({0 => 1, 1 =>1})
      end
    end

    context 'replication factor larger than destination broker size' do
      it 'should fail' do
        expect do
          assignments = reassign.generate_assignment_load_balanced(
            {"topic_name" => topic_rep_factor_two},
            [1, 2],
            3)
        end.to raise_error(SystemExit)

        expect do
          assignments = reassign.generate_assignment_smart(
            {"topic_name" => topic_rep_factor_two},
            [1, 2],
            3,
            [0, 1, 2])
        end.to raise_error(SystemExit)
      end
    end

    context 'topic with no partiton' do
      it 'should fail' do
        expect do
          assignments = reassign.generate_assignment_load_balanced(
            {"topic_name" => FactoryGirl.build(:topic)},
            [1, 2],
            2)
        end.to raise_error(SystemExit)

        expect do
          assignments = reassign.generate_assignment_smart(
            {"topic_name" => FactoryGirl.build(:topic)},
            [1, 2],
            2,
            [0, 1, 2])
        end.to raise_error(SystemExit)
      end
    end
  end
end