module Kafkat
  module Command
    class Configs < Base
      register_as 'configs'

      usage 'configs [topic]',
            'Print the version and config by topic.'

      def run
        topic_name = ARGV.shift
        topic_names = topic_name && [topic_name]
        topics = zookeeper.get_topics(topic_names)

        print_config_header
        topics.each do |name, t|
          print_config(name, zookeeper.get_config(name))
        end
      end
    end
  end
end
