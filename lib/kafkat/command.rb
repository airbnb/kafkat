module Kafkat
  module Command
    class NotFoundError < StandardError; end

    def self.all
      @all ||= {}
    end

    def self.get(name)
      klass = all[name.downcase]
      raise NotFoundError if !klass
      klass
    end

    class Base
      include Formatting
      include CommandIO

      attr_reader :config

      class << self
        attr_reader :command_name
      end

      def self.register_as(name)
        @command_name = name
        Command.all[name] = self
      end

      def self.usages
        @usages ||= []
      end

      def self.usage(format, description)
        usages << [format, description]
      end

      def initialize(config)
        @config = config
      end

      def run
        raise NotImplementedError
      end

      def admin
        @admin ||= begin
          Interface::Admin.new(config)
        end
      end

      def zookeeper
        @zookeeper ||= begin
          Interface::Zookeeper.new(config)
        end
      end

      def kafka_logs
        @kafka_logs ||= begin
          Interface::KafkaLogs.new(config)
        end
      end

      def connect
        @connect ||= begin
          Interface::Connect.new(config)
        end
      end
    end
  end
end

# Require all of the commands.
command_glob = File.expand_path("../command/*.rb", __FILE__)
Dir[command_glob].each { |f| require f }
