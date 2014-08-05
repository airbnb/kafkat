module Kafkat
  module Command
    class Topics < Base
      register_as 'topics'

      usage 'topics',
            'Print all topics.'

      def run
        ts = zookeeper.get_topics
        ts.each { |name, t| print_topic(t) }
      end
    end
  end
end
