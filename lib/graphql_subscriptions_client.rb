require "websocket/driver"
require "permessage_deflate"
require "socket"
require "uri"
require "securerandom"
require "json"

require_relative "graphql_subscriptions_client/version"

module GraphqlSubscriptionsClient
  attr_reader :url, :thread
  attr_reader :subscription_query

  def initialize(url, subscription_query:, protocol: "graphql-ws", &message_callback)
    uri = URI.parse(url)

    @url = url
    @subscription_query = subscription_query
    @message_callback = message_callback

    @tcp = TCPSocket.new(uri.host, uri.port)
    @dead = false

    @driver = WebSocket::Driver.client(self, protocols: [protocol])
    @driver.add_extension(PermessageDeflate)

    @driver.on(:open) do |event|
      send(JSON.dump(init))
    end

    @driver.on(:message) do |event|
      json = JSON.parse(event.data)

      case json["type"]
      when "connection_ack"
        send(JSON.dump(subscription))
      when "data"
        @message_callback.call(json["payload"]["data"])
      end
    end

    @driver.on(:close) do |event|
      finalize(event)
    end

    @thread = Thread.new do
      @driver.parse(@tcp.read(1)) until @dead
    end

    @driver.start
  end

  def send(message)
    @driver.text(message)
  end

  def write(data)
    @tcp.write(data)
  end

  def close
    @driver.close
  end

  def init
    {type: "connection_init", payload: {}}
  end

  def subscription
    {
      id: SecureRandom.hex(10), # FIXME: MIGHT CAUSE MULTIPLE MESSAGES
      type: "start",
      payload: {query: subscription_query}
    }
  end

  def finalize(event)
    @dead = true
    @thread.kill
  end
end
