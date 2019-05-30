# frozen_string_literal: true

module Kafkat
  class CLI
    attr_reader :config

    def self.run!
      new.run
    end

    def run
      load_config
      run_command
    end

    def load_config
      @config = Config.load!
    rescue Config::NotFoundError
      no_config_error
    rescue Config::ParseError
      bad_config_error
    end

    def run_command
      name = ARGV.shift
      if name
        command_klass = Command.get(name)
        command = command_klass.new(config)
        command.run
      else
        print_banner
        print_commands
        exit 0
      end
    rescue Command::NotFoundError
      no_command_error
    end

    def print_banner
      print "kafkat #{VERSION}: Simplified command-line administration for Kafka brokers\n"
      print "usage: kafkat [command] [options]\n"
    end

    def print_commands
      print "\nHere's a list of supported commands:\n\n"
      Command.all.values.sort_by(&:command_name).each do |klass|
        klass.usages.each do |usage|
          format = usage[0]
          description = usage[1]
          padding_length = 68 - format.length
          padding = ' ' * padding_length unless padding_length.negative?
          print "  #{format}#{padding}#{description}\n"
        end
      end
      print "\n"
    end

    def no_config_error
      print "Configuration file not found (~/.kafkatcfg). See documentation.\n"
      exit 1
    end

    def bad_config_error
      print "Configuration file failed to parse (~/.kafkatcfg).\n"
      exit 1
    end

    def no_command_error
      print "This command isn't recognized.\n"
      print_commands
      exit 1
    end
  end
end
