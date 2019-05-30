# frozen_string_literal: true
require 'tempfile'
require 'English'

module Kafkat
  module Interface
    class Admin
      class ExecutionFailedError < StandardError; end

      attr_reader :kafka_path
      attr_reader :zk_path

      def initialize(config)
        @kafka_path = config.kafka_path
        @zk_path = config.zk_path
      end

      def elect_leaders!(partitions)
        file = Tempfile.new('kafkat-partitions.json')

        json_partitions = []
        partitions.each do |p|
          json_partitions << {
            'topic' => p.topic_name,
            'partition' => p.id,
          }
        end

        json = { 'partitions' => json_partitions }
        file.write(JSON.dump(json))
        file.close

        run_tool(
          'kafka-preferred-replica-election',
          '--path-to-json-file', file.path
        )
      ensure
        file.unlink
      end

      def reassign!(assignments)
        file = Tempfile.new('kafkat-partitions.json')

        json_partitions = []
        assignments.each do |a|
          json_partitions << {
            'topic' => a.topic_name,
            'partition' => a.partition_id,
            'replicas' => a.replicas,
          }
        end

        json = {
          'partitions' => json_partitions,
          'version' => 1,
        }

        file.write(JSON.dump(json))
        file.close

        run_tool(
          'kafka-reassign-partitions',
          '--execute',
          '--reassignment-json-file', file.path
        )
      ensure
        file.unlink
      end

      def shutdown!(broker_id, options = {})
        args = ['--broker', broker_id]
        args += ['--num.retries', options[:retries]] if options[:retries]
        args += ['--retry.interval.ms', option[:interval]] if options[:interval]

        run_tool(
          'kafka-run-class',
          'kafka.admin.ShutdownBroker',
          *args
        )
      end

      def run_tool(name, *args)
        path = File.join(kafka_path, "bin/#{name}.sh")
        args += ['--zookeeper', "\"#{zk_path}\""]
        args_string = args.join(' ')
        result = %(#{path} #{args_string})
        raise ExecutionFailedError if $CHILD_STATUS.to_i > 0

        result
      end
    end
  end
end
