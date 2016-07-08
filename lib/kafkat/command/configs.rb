module Kafkat
  module Command
    class Configs < Base
      register_as 'configs'

      usage 'configs [topic]',
            'Print the version and config by topic.'

      def run
        topic_name = ARGV.shift
        topic_names = topic_name && [topic_name]

        configs = zookeeper.get_configs(topic_names)
        print_config_header
        configs.each do |name, c|
          print_config(name, c)
        end
      end
    end
  end
end
