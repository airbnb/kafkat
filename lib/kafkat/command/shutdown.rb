# frozen_string_literal: true
module Kafkat
  module Command
    class Resign < Base
      register_as 'shutdown'

      usage 'shutdown <broker id>',
            'Gracefully remove leaderships from a broker (requires JMX).'

      def run
        broker_id = ARGV[0] && ARGV.shift.to_i
        if broker_id.nil?
          puts 'You must specify a broker ID.'
          exit 1
        end

        print "This operation gracefully removes leaderships from broker '#{broker_id}'.\n"
        return unless agree('Proceed (y/n)?')

        result = nil
        begin
          print "\nBeginning shutdown.\n"
          result = admin.shutdown!(broker_id)
          print "Started.\n"
        rescue Interface::Admin::ExecutionFailedError
          print result
        end
      end
    end
  end
end
