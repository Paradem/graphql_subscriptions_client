require "json"
require "openssl"
require "permessage_deflate"
require "securerandom"
require "socket"
require "uri"
require "websocket/driver"

require_relative "graphql_subscriptions/version"

module GraphqlSubscriptions
  class SecureSocket
    attr_reader :tcp

    delegate :read, :write, to: :tcp

    def initialize(url)
      uri = URI.parse(url)

      cert_store = OpenSSL::X509::Store.new
      cert_store.set_default_paths

      ctx = OpenSSL::SSL::SSLContext.new
      ctx.ssl_version = :TLSv1_2_client
      ctx.cert_store = cert_store

      @tcp = TCPSocket.new(uri.host, uri.port)
      @tcp = ::OpenSSL::SSL::SSLSocket.new(@tcp, ctx)
      @tcp.sync_close = true
      @tcp.hostname = uri.host
      @tcp.connect
    end
  end

  class Client
    attr_reader :url, :thread
    attr_reader :subscription_query

    def initialize(url, subscription_query:, init_payload: {}, protocol: "graphql-transport-ws", &message_callback)
      @url = url
      @subscription_query = subscription_query
      @init_payload = init_payload
      @protocol = protocol
      @message_callback = message_callback

      @tcp = SecureSocket.new(url)
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
        when "data", "next"
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
      {type: "connection_init", payload: @init_payload}
    end

    def subscription
      {
        id: SecureRandom.hex(10), # FIXME: MIGHT CAUSE MULTIPLE MESSAGES
        type: sub_type,
        payload: {query: subscription_query}
      }
    end

    def sub_type
      {
        "graphql-transport-ws" => "subscribe",
        "graphql-ws" => "start"
      }[@protocol]
    end

    def finalize(event)
      @dead = true
      @thread.kill
    end
  end
end
