module Kafkat
  class Config
    CONFIG_PATHS = [
      '~/.kafkatcfg',
      '/etc/kafkatcfg'
    ]

    class NotFoundError < StandardError; end
    class ParseError < StandardError; end

    attr_reader :kafka_path
    attr_reader :log_path
    attr_reader :zk_path
    attr_reader :connect_api_host

    def self.load!
      string = nil
      e = nil

      CONFIG_PATHS.each do |rel_path|
        begin
          path = File.expand_path(rel_path)
          string = File.read(path)
          break
        rescue => e
        end
      end

      raise e if e && string.nil?

      json = JSON.parse(string)
      self.new(json)

    rescue Errno::ENOENT
      raise NotFoundError
    rescue JSON::JSONError
      raise ParseError
    end

    def initialize(json)
      @kafka_path = json['kafka_path']
      @log_path = json['log_path']
      @zk_path = json['zk_path']
      @connect_api_host = json['connect_api_host']
    end
  end
end
