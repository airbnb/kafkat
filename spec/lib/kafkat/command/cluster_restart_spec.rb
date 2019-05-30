require 'spec_helper'

module Kafkat
  module ClusterRestart

    describe Kafkat::ClusterRestart do
      around(:all) do |example|
        prev_home = ENV['HOME']
        tmp = Dir.mktmpdir
        ENV['HOME'] = tmp
        begin
          example.run
        ensure
          FileUtils.rm_rf tmp
          ENV['HOME'] = prev_home
        end
      end

      describe Session do

        describe '#allBrokersRestarted?' do
          context 'when some brokers have not been restarted' do
            let(:session) {
              Session.new('broker_states' => {'1' => Session::STATE_NOT_RESTARTED, '2' => Session::STATE_RESTARTED})
            }

            it do
              expect(session.all_restarted?).to be false
            end
          end

          context 'when all brokers have been restarted' do
            let(:session) {
              Session.new('broker_states' => {'1' => Session::STATE_RESTARTED, '2' => Session::STATE_RESTARTED})
            }

            it do
              expect(session.all_restarted?).to be true
            end
          end
        end

        describe '#update_states!' do
          let(:session) {
            Session.new('broker_states' => {'1' => Session::STATE_NOT_RESTARTED, '2' => Session::STATE_RESTARTED})
          }

          it 'validates state' do
            expect { session.update_states!('my_state', []) }.to raise_error UnknownStateError
          end

          it 'validates broker ids' do
            expect { session.update_states!(Session::STATE_RESTARTED, ['1', '4']) }.to raise_error UnknownBrokerError
          end

          it 'changes the states' do
            session.update_states!(Session::STATE_PENDING, ['1'])
            expect(session.broker_states['1']).to eql(Session::STATE_PENDING)
          end
        end

      end


      describe Subcommands do
        let(:p1) { Partition.new('topic1', 'p1', ['1', '2', '3'], '1', 1) }
        let(:p2) { Partition.new('topic1', 'p2', ['1', '2', '3'], '2', 1) }
        let(:p3) { Partition.new('topic1', 'p3', ['2', '3', '4'], '3', 1) }
        let(:topics) {
          {
              'topic1' => Topic.new('topic1', [p1, p2, p3])
          }
        }
        let(:zookeeper) { double('zookeeper') }
        let(:broker_ids) { ['1', '2', '3', '4'] }
        let(:broker_4) { Broker.new('4', 'i-xxxxxx.inst.aws.airbnb.com', 9092) }
        let(:session) { Session.from_brokers(broker_ids) }

        describe Subcommands::Next do

          let(:next_command) { Subcommands::Next.new({}) }

          it 'execute next with 4 brokers and 3 partitions' do
            allow(zookeeper).to receive(:broker_ids).and_return(broker_ids)
            allow(zookeeper).to receive(:get_broker).and_return(broker_4)
            allow(zookeeper).to receive(:topics).and_return(topics)
            allow(Session).to receive(:exists?).and_return(true)
            allow(Session).to receive(:load!).and_return(session)
            allow(session).to receive(:save!)
            allow(next_command).to receive(:zookeeper).and_return(zookeeper)

            expect(Session).to receive(:load!)
            expect(session).to receive(:save!)

            next_command.run
            expect(next_command.session.broker_states['4']).to eq(Session::STATE_PENDING)
          end
        end

        describe Subcommands::Good do
          let(:good_command) { Subcommands::Good.new({}) }

          let(:session){
            Session.new('broker_states' => {'1' => Session::STATE_PENDING})
          }

          it 'set one broker to be restarted' do
            allow(Session).to receive(:exists?).and_return(true)
            allow(Session).to receive(:load!).and_return(session)
            allow(session).to receive(:save!)

            # expect(Session).to receive(:load!)
            expect(session).to receive(:save!)
            good_command.restart('1')
            expect(good_command.session.broker_states['1']).to eq(Session::STATE_RESTARTED)
          end
        end
      end

      describe ClusterRestartHelper do
        let(:p1) { Partition.new('topic1', 'p1', ['1', '2', '3'], '1', 1) }
        let(:p2) { Partition.new('topic1', 'p2', ['1', '2', '3'], '2', 1) }
        let(:p3) { Partition.new('topic1', 'p3', ['2', '3', '4'], '3', 1) }
        let(:topics) {
          {
              'topic1' => Topic.new('topic1', [p1, p2, p3])
          }
        }
        let(:zookeeper) { double('zookeeper') }
        let(:broker_ids) { ['1', '2', '3', '4'] }

        describe '#get_broker_to_leader_partition_mapping' do
          it 'initialize a new mapping with 4 nodes' do
            broker_to_partition = ClusterRestartHelper.get_broker_to_leader_partition_mapping(topics)

            expect(broker_to_partition['1']).to eq([p1])
            expect(broker_to_partition['2']).to eq([p2])
            expect(broker_to_partition['3']).to eq([p3])
            expect(broker_to_partition['4']).to eq([])
          end
        end


        describe '#calculate_costs' do
          context 'when no restarted brokers' do
            it do
              broker_to_partition = ClusterRestartHelper.get_broker_to_leader_partition_mapping(topics)
              session = Session.from_brokers(broker_ids)

              expect(ClusterRestartHelper.calculate_cost('1', broker_to_partition['1'], session)).to eq(3)
              expect(ClusterRestartHelper.calculate_cost('2', broker_to_partition['2'], session)).to eq(3)
              expect(ClusterRestartHelper.calculate_cost('3', broker_to_partition['3'], session)).to eq(3)
              expect(ClusterRestartHelper.calculate_cost('4', broker_to_partition['4'], session)).to eq(0)
            end
          end

          context 'when one broker has been restarted' do
            it do
              broker_to_partition = ClusterRestartHelper.get_broker_to_leader_partition_mapping(topics)
              session = Session.from_brokers(broker_ids)
              session.update_states!(Session::STATE_RESTARTED, ['4'])

              expect(ClusterRestartHelper.calculate_cost('1', broker_to_partition['1'], session)).to eq(3)
              expect(ClusterRestartHelper.calculate_cost('2', broker_to_partition['2'], session)).to eq(3)
              expect(ClusterRestartHelper.calculate_cost('3', broker_to_partition['3'], session)).to eq(2)
              expect(ClusterRestartHelper.calculate_cost('4', broker_to_partition['4'], session)).to eq(0)
            end
          end
        end

        describe '#select_broker_with_min_cost' do
          context 'when no restarted brokers' do
            it do
              session = Session.from_brokers(broker_ids)

              broker_id, cost = ClusterRestartHelper.select_broker_with_min_cost(session, topics)
              expect(broker_id).to eq('4')
              expect(cost).to eq(0)
            end
          end

          context 'when next selection after one broker is restarted' do
            it do
              session = Session.from_brokers(broker_ids)
              session.update_states!(Session::STATE_RESTARTED, ['4'])

              broker_id, cost = ClusterRestartHelper.select_broker_with_min_cost(session, topics)
              expect(broker_id).to eq('3')
              expect(cost).to eq(2)
            end
          end
        end
      end
    end
  end
end

