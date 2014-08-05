module Kafkat
  class Config
    class NotFoundError < StandardError; end
    class ParseError < StandardError; end

    attr_reader :kafka_path
    attr_reader :log_path
    attr_reader :zk_path

    def self.load!
      path = File.expand_path('~/.kafkatcfg')
      string = File.read(path)
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
    end
  end
end
