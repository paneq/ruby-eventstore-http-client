require 'securerandom'
require 'excon'
require 'json'

module Eventstore::Http
  class Client
    APP_JSON     = "application/json".freeze
    CONTENT_TYPE = :"Content-Type"
    EVENT_TYPE   = :"ES-EventType"
    EVENT_ID     = :"ES-EventId"

    def initialize(options = {})
      @options = options
    end

    def stream(stream_name)
      self.class.new(
        @options.merge(stream: stream_name)
      )
    end

    def publish(event)
      stream   = @options.fetch(:stream)
      response = connection.post(
        path: "/streams/#{stream}",
        body: JSON.dump(event.data),
        headers: {
          CONTENT_TYPE => APP_JSON,
          EVENT_TYPE   => event.type,
          EVENT_ID     => event.id,
        }
      )
      raise unless response.status == 201
    end

    def connection
      @options[:connection] ||= Excon.new('http://127.0.0.1:2113/')
    end
  end

  class Event < Struct.new(:data, :type, :id)
    def id
      super || self.id = SecureRandom.uuid
    end
  end
end

describe Eventstore::Http do
  specify "can save one event" do
    client = Eventstore::Http::Client.new
    client.stream(stream_name = SecureRandom.hex).publish(
      Eventstore::Http::Event.new({key1: "value1",}, "my-event-type", event_id = SecureRandom.uuid)
    )
  end
end