require 'json'
require 'rest-client'

module Kafkat
  module Interface
    class Connect
      attr_reader :connect_api_host

      def initialize(config)
        @connect_api_host = config.connect_api_host || 'http://localhost:8083'
      end

      def list
        JSON.parse(RestClient.get(api_path('/connectors'), api_options))
      end

      def show(connector)
        JSON.parse(RestClient.get(api_path("/connectors/#{connector}"), api_options))
      rescue RestClient::NotFound
        nil
      end

      def configure(connector, config)
        RestClient.put(api_path("/connectors/#{connector}/config"), config, api_options)
      end

      def remove(connector)
        RestClient.delete(api_path("/connectors/#{connector}"), api_options)
      end

      protected

      def api_path(path)
        "#{connect_api_host}#{path}"
      end

      def api_options
        {:content_type => :json, :accept => :json}
      end
    end
  end
end
