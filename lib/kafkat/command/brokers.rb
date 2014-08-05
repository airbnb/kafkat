module Kafkat
  module Command
    class Brokers < Base
      register_as 'brokers'

      usage 'brokers',
            'Print available brokers from Zookeeper.'

      def run
        bs = zookeeper.get_brokers
        print_broker_header
        bs.each { |id, b| print_broker(b) }
      end
    end
  end
end
