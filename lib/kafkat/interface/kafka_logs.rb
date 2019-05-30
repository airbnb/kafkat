# frozen_string_literal: true
module Kafkat
  module Interface
    class KafkaLogs
      UNTRUNCATED_SIZE = 10 * 1024 * 1024 # 1MB

      class NoLogsError < StandardError; end
      class KafkaRunningError < StandardError; end

      attr_reader :log_path

      def initialize(config)
        @log_path = config.log_path
      end

      def clean_indexes!
        check_exists

        to_remove = []
        lock_for_write do
          index_glob = File.join(log_path, '**/*.index')
          Dir[index_glob].each do |index_path|
            size = File.size(index_path)
            to_remove << index_path if size == UNTRUNCATED_SIZE
          end
        end

        to_remove.each do |path|
          print "Removing #{path}.\n"
          File.unlink(path)
        end

        to_remove.size
      end

      private

      def check_exists
        raise NoLogsError unless File.exist?(log_path)
      end

      def lock_for_write
        File.open(lockfile_path, File::CREAT) do |lockfile|
          locked = lockfile.flock(File::LOCK_EX | File::LOCK_NB)
          raise KafkaRunningError unless locked

          yield
        end
      end

      def lockfile_path
        File.join(log_path, '.lock')
      end
    end
  end
end
