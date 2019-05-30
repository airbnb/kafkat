# frozen_string_literal: true
module Kafkat
  module Command
    class Controller < Base
      register_as 'controller'

      usage 'controller',
            'Print the current controller.'

      def run
        c = zookeeper.controller
        print "The current controller is '#{c.id}' (#{c.host}:#{c.port}).\n"
      rescue Interface::Zookeeper::NotFoundError
        print "ERROR: Couldn't determine the current controller.\n"
        exit 1
      end
    end
  end
end
