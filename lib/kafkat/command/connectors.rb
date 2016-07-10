module Kafkat
  module Command
    class Connectors < Base
      register_as 'connectors'

      usage 'connectors [connector-name]',
            'View configured connectors'
      usage 'connectors <connector-name> --configure [--config] [--config-file]',
            'Configure connector with specified id'
      usage 'connectors <connector-name> --delete',
            'Delete connector with specified id'

      def run
        @connector_name = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')

        if !@connector_name
          list
          exit
        end

        @options = Trollop.options do
          opt :configure, "create new connector"
          opt :config, "connector config in JSON format", type: :string
          opt :config_file, "path to file containing connector config in JSON format", type: :string
          opt :remove, "remove connector"
        end

        if @options.configure
          configure
        elsif @options.remove
          remove
        else
          show
        end
      end

      def list
        connectors = connect.list
        if connectors.empty?
          puts 'No connectors configured, add one by running connectors --create'
        else
          puts "#{connectors.length} connector(s) configured:"
          connectors.each do |connector|
            puts connector
          end
        end
      end

      def show
        connector = connect.show(@connector_name)
        if connector
          puts JSON.pretty_generate(connector)
        else
          puts "Connector '#{@connector_name}' not found"
        end
      end

      def configure
        unless @options.config || @options.config_file
          puts "Please specify the connector JSON configuration using --config or --config-file"
          exit 1
        end

        if @options.config_file
          config = File.read(@options.config_file)
        else
          config = @options.config
        end

        puts "Configuring '#{@connector_name}' with:"
        puts config
        return unless agree("Proceed (y/n)?")

        connect.configure(@connector_name, config)

        puts "Connector '#{@connector_name}' configured"
      end

      def remove
        puts "Removing '#{@connector_name}'"
        return unless agree("Proceed (y/n)?")

        connect.remove(@connector_name)

        puts "Connector '#{@connector_name}' removed"
      end
    end
  end
end
