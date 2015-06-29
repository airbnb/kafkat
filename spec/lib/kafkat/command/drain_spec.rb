require 'spec_helper'

module Kafkat
  describe Command::Drain do
    let(:drain) { Command::Drain.new({}) }
    let(:broker_id) { 0 }
    let(:destination_broker_ids) { [1, 2] }
    let(:all_broker_ids) { [0, 1, 2] }

    context 'three nodes with replication factor 1' do
      let(:topic_rep_factor_one) { FactoryGirl.build(:topic_rep_factor_one) }

      it 'should put replicas to broker with lowest number of replicas' do
        assignments = drain.generate_assignments(broker_id,
                                                 {"topic_name" => topic_rep_factor_one},
                                                 destination_broker_ids,
                                                 all_broker_ids)
        expect(assignments).to have_exactly(2).Partition
        expect(assignments[0].replicas).to eq([2])
        expect(assignments[1].replicas).to eq([1])
      end
    end

    context 'three nodes with replication factor 2' do
      let(:topic_rep_factor_two) { FactoryGirl.build(:topic_rep_factor_two) }
      it 'should put replicas to broker with lowest number of replicas' do
        assignments = drain.generate_assignments(broker_id,
                                                 {"topic_name" => topic_rep_factor_two},
                                                 destination_broker_ids,
                                                 all_broker_ids)
        expect(assignments).to have_exactly(4).Partition
        expect(assignments[0].replicas).to eq([1, 2])
        expect(assignments[1].replicas).to eq([2, 1])
        expect(assignments[2].replicas).to eq([1, 2])
        expect(assignments[3].replicas).to eq([2, 1])
      end
    end

    context 'not enough brokers to keep all replicas' do
      let(:topic_rep_factor_three) { FactoryGirl.build(:topic_rep_factor_three) }

      it 'should raise SystemExit' do
        expect do
          drain.generate_assignments(broker_id,
                                     {"topic_name" => topic_rep_factor_three},
                                     destination_broker_ids,
                                     all_broker_ids)
        end.to raise_error(SystemExit)
      end
    end

    context 'one destination broker is empty' do
      let(:topic_with_one_empty_broker) { FactoryGirl.build(:topic_with_one_empty_broker) }
      it 'should not raise exception' do
        expect do
          drain.generate_assignments(broker_id,
                                     {"topic_name" => topic_with_one_empty_broker},
                                     destination_broker_ids,
                                     all_broker_ids)
        end.not_to raise_error
      end
    end
  end
end
