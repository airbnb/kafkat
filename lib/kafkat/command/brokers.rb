# frozen_string_literal: true
module Kafkat
  module Command
    class Brokers < Base
      register_as 'brokers'

      usage 'brokers',
            'Print available brokers from Zookeeper.'

      def run
        bs = zookeeper.brokers
        print_broker_header
        bs.each { |_, b| print_broker(b) }
      end
    end
  end
end
