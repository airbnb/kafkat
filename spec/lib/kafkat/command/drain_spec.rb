require 'spec_helper'

module Kafkat
  RSpec.describe Command::Drain do
    let(:drain) { Command::Drain.new({}) }
    let(:dead_broker) { 0 }
    let(:healthy_brokers) { [1, 2] }

    context 'three nodes with replication factor 1' do
      let(:topic_rep_factor_one) { FactoryGirl.build(:topic_rep_factor_one) }

      it 'should put replicas to one of the other two healthy nodes' do
        assignments = drain.generate_assignments(dead_broker,
                                                 {"topic_name" => topic_rep_factor_one},
                                                 healthy_brokers)
        expect(assignments).to have_exactly(1).Partition
        assignments.each do |a|
          expect(a.replicas).to have_exactly(1).broker_id

          a.replicas.each do |r|
            expect(healthy_brokers).to include(r)
          end
        end
      end
    end

    context 'three nodes with replication factor 2' do
      let(:topic_rep_factor_two) { FactoryGirl.build(:topic_rep_factor_two) }

      it 'should put replicas to both of the other two healthy nodes' do
        assignments = drain.generate_assignments(dead_broker,
                                                 {"topic_name" => topic_rep_factor_two},
                                                 healthy_brokers)
        expect(assignments).to have_exactly(2).Partitions
        assignments.each do |a|
          expect(a.replicas).to have_exactly(2).broker_ids

          a.replicas.each do |r|
            expect(healthy_brokers).to include(r)
          end
        end
      end
    end

    context 'three nodes with replication factor 3' do
      let(:topic_rep_factor_three) { FactoryGirl.build(:topic_rep_factor_three) }

      it 'should put replicas to both of the other two healthy nodes' do
        assignments = drain.generate_assignments(dead_broker,
                                                 {"topic_name" => topic_rep_factor_three},
                                                 healthy_brokers)
        expect(assignments).to have_exactly(3).Partitions
        assignments.each do |a|
          expect(a.replicas).to have_exactly(2).broker_ids

          a.replicas.each do |r|
            expect(healthy_brokers).to include(r)
          end
        end
      end
    end
  end
end
