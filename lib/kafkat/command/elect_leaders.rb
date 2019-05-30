# frozen_string_literal: true
module Kafkat
  module Command
    class ElectLeaders < Base
      register_as 'elect-leaders'

      usage 'elect-leaders [topic]',
            'Begin election of the preferred leaders.'

      def run
        topic_name = ARGV[0] && ARGV.shift
        topic_names = topic_name && [topic_name]

        topics = zookeeper.topics(topic_names)
        partitions = topics.values.map(&:partitions).flatten

        topics_s = topic_name ? "'#{topic_name}'" : 'all topics'
        print "This operation elects the preferred replicas for #{topics_s}.\n"
        return unless agree('Proceed (y/n)?')

        result = nil
        begin
          print "\nBeginning.\n"
          result = admin.elect_leaders!(partitions)
          print "Started.\n"
        rescue Interface::Admin::ExecutionFailedError
          print result
        end
      end
    end
  end
end
