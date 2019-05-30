# frozen_string_literal: true

module Kafkat
  class Config
    CONFIG_PATHS = [
      '~/.kafkatcfg',
      '/etc/kafkatcfg',
    ].freeze

    class NotFoundError < StandardError; end
    class ParseError < StandardError; end

    attr_reader :kafka_path
    attr_reader :log_path
    attr_reader :zk_path

    def self.load!
      string = nil
      e = nil

      CONFIG_PATHS.each do |rel_path|
        path = File.expand_path(rel_path)
        string = File.read(path)
        break
      end

      raise e if e && string.nil?

      json = JSON.parse(string)
      new(json)
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
