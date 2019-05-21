module Kafkat
  module Command
    class Describe < Base
      register_as 'partitions'

      usage 'partitions [topic]',
            'Print partitions by topic.'
      usage 'partitions [topic] --under-replicated',
            'Print partitions by topic (only under-replicated).'
      usage 'partitions [topic] --unavailable',
            'Print partitions by topic (only unavailable).'

      def run
        topic_name = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')
        topic_names = topic_name && [topic_name]

        @options = Optimist.options do
          opt :under_replicated, "only under-replicated"
          opt :unavailable, "only unavailable"
        end

        brokers = zookeeper.get_brokers
        topics = zookeeper.get_topics(topic_names)

        print_partition_header
        topics.each do |name, t|
          t.partitions.each do |p|
            print_partition(p) if selected?(p, brokers)
          end
        end
      end

      private

      def selected?(partition, brokers)
        return partition.under_replicated? if only_under_replicated?
        return !partition.has_leader?(brokers) if only_unavailable?
        true
      end

      def only_under_replicated?
        !!@options[:under_replicated]
      end

      def only_unavailable?
        !!@options[:unavailable]
      end
    end
  end
end
