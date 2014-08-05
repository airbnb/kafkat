module Kafkat
  module Interface
    class Zookeeper
      class WriteConflictError < StandardError; end

      attr_reader :zk_path

      def initialize(config)
        @zk_path = config.zk_path
      end

      def get_brokers(ids=nil)
        brokers = {}
        ids ||= zk.children(brokers_path)
        ids.each do |id|
          id = id.to_i
          begin
            brokers[id] = get_broker(id)
          rescue
          end
        end
        brokers
      end

      def get_broker(id)
        path = broker_path(id)
        string = zk.get(path).first
        json = JSON.parse(string)
        host, port = json['host'], json['port']
        Broker.new(id, host, port)
      end

      def get_topics(names=nil)
        topics = {}
        names ||= zk.children(topics_path)
        names.each do |name|
          begin
            topics[name] = get_topic(name)
          rescue => e
          end
        end
        topics
      end

      def get_topic(name)
        path1 = topic_path(name)
        topic_string = zk.get(path1).first
        topic_json = JSON.parse(topic_string)

        partitions = []
        path2 = topic_partitions_path(name)
        zk.children(path2).each do |id|
          id = id.to_i

          path3 = topic_partition_state_path(name, id)
          partition_string = zk.get(path3).first
          partition_json = JSON.parse(partition_string)

          replicas = topic_json['partitions'][id.to_s]
          leader = partition_json['leader']
          isr = partition_json['isr']

          partitions << Partition.new(name, id, replicas, leader, isr)
        end

        Topic.new(name, partitions)
      end

      # Returns true if successful
      def write_leader(partition, broker_id)
        path = topic_partition_state_path(partition.topic_name, partition.id)
        string, stat = zk.get(path)

        partition_json = JSON.parse(string)
        partition_json['leader'] = broker_id
        new_string = JSON.dump(partition_json)

        unless zk.set(path, new_string, :version => stat.version)
          raise ChangedDuringUpdateError
        end
      end

      private

      def zk
        @zk ||= ZK.new(zk_path)
      end

      def brokers_path
        '/brokers/ids'
      end

      def broker_path(id)
        "/brokers/ids/#{id}"
      end

      def topics_path
        '/brokers/topics'
      end

      def topic_path(name)
        "/brokers/topics/#{name}"
      end

      def topic_partitions_path(name)
        "/brokers/topics/#{name}/partitions"
      end

      def topic_partition_state_path(name, id)
        "/brokers/topics/#{name}/partitions/#{id}/state"
      end
    end
  end
end
