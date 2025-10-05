require 'em-hiredis'
require 'multi_json'

require File.expand_path('redis_factory', __dir__)

module Faye
  class Redis
    DEFAULT_GC              = 60
    LOCK_TIMEOUT            = 120
    MAX_RECONNECT_DELAY     = 30
    INITIAL_RECONNECT_DELAY = 1
    MAX_CLIENT_ID_RETRIES   = 10

    def self.create(server, options)
      new(server, options)
    end

    def initialize(server, options)
      @server  = server
      @options = options
      @factory = options[:factory] || RedisFactory.new(options)

      init
    end

    def init
      return if @initialized || !EventMachine.reactor_running?

      gc     = @options[:gc]        || DEFAULT_GC
      @ns    = @options[:namespace] || ''
      @redis = @factory.call

      @subscriber = @redis.pubsub

      @message_channel = @ns + '/notifications/messages'
      @close_channel   = @ns + '/notifications/close'

      @subscriber.subscribe(@message_channel) do
        @server.debug 'Subscribed to message channel'
      end.errback do |error|
        @server.error "Faye::Redis: failed to subscribe to message channel - #{error.message}"
        handle_connection_failure
      end

      @subscriber.subscribe(@close_channel) do
        @server.debug 'Subscribed to close channel'
      end.errback do |error|
        @server.error "Faye::Redis: failed to subscribe to close channel - #{error.message}"
        handle_connection_failure
      end

      @subscriber.on(:message) do |topic, message|
        empty_queue(message) if topic == @message_channel
        @server.trigger(:close, message) if topic == @close_channel
      end

      @gc = EventMachine.add_periodic_timer(gc, &method(:gc))

      @subscriber.on(:failed) do
        @server.error 'Faye::Redis: subscriber connection failed, attempting to reconnect'
        handle_connection_failure
      end

      @subscriber.on(:disconnected) do
        @server.error 'Faye::Redis: subscriber connection disconnected, attempting to reconnect'
        handle_connection_failure
      end

      @redis.on(:failed) do
        @server.error 'Faye::Redis: redis connection failed, attempting to reconnect'
        handle_connection_failure
      end

      @redis.on(:disconnected) do
        @server.error 'Faye::Redis: redis connection disconnected, attempting to reconnect'
        handle_connection_failure
      end

      @initialized = true
    end

    def disconnect
      return unless @initialized

      @subscriber&.unsubscribe(@message_channel) if @message_channel
      @subscriber&.unsubscribe(@close_channel) if @close_channel
      EventMachine.cancel_timer(@gc) if @gc

      @redis&.close_connection
      @subscriber = nil
      @redis = nil
      @gc = nil
      @initialized = false
      @reconnecting = false
      @reconnect_attempts = 0
    end

    def create_client(&callback, retries = 0)
      init

      if retries >= MAX_CLIENT_ID_RETRIES
        @server.error "Faye::Redis: failed to create unique client ID after #{retries} attempts"
        return
      end

      client_id = @server.generate_id
      @redis.zadd(clients_key, 0, client_id) do |added|
        next create_client(&callback, retries + 1) if added.zero?

        @server.debug 'Created new client ?', client_id
        ping(client_id)
        @server.trigger(:handshake, client_id)
        callback.call(client_id)
      end.errback do |error|
        @server.error "Faye::Redis: failed to create client - #{error.message}"
        create_client(&callback, retries + 1)
      end
    end

    def client_exists(client_id, &callback)
      init
      cutoff = get_current_time - (1000 * 1.6 * @server.timeout)

      @redis.zscore(clients_key, client_id) do |score|
        callback.call(score.to_i > cutoff)
      end.errback do |error|
        @server.error "Faye::Redis: failed to check client existence - #{error.message}"
        callback.call(false)
      end
    end

    def destroy_client(client_id, &callback)
      init
      @redis.zadd(clients_key, 0, client_id) do
        @redis.smembers(client_channels_key(client_id)) do |channels|
          n = channels.size
          next after_subscriptions_removed(client_id, &callback) if n.zero?

          completed = 0
          cleanup_called = false

          channels.each do |channel|
            unsubscribe(client_id, channel) do
              completed += 1
              if completed == n && !cleanup_called
                cleanup_called = true
                after_subscriptions_removed(client_id, &callback)
              end
            end
          end
        end.errback do |error|
          @server.error "Faye::Redis: failed to get client channels - #{error.message}"
          after_subscriptions_removed(client_id, &callback)
        end
      end.errback do |error|
        @server.error "Faye::Redis: failed to mark client for destruction - #{error.message}"
      end
    end

    def after_subscriptions_removed(client_id, &callback)
      return callback&.call unless @redis

      @redis.multi
      @redis.del(client_messages_key(client_id))
      @redis.zrem(clients_key, client_id)
      @redis.exec.callback do
        @server.debug 'Destroyed client ?', client_id
        @server.trigger(:disconnect, client_id)
        @redis.publish(@close_channel, client_id) if @redis
        callback&.call
      end.errback do |error|
        @server.error "Faye::Redis: failed to remove client #{client_id} - #{error.message}"
        callback&.call
      end
    end

    def ping(client_id)
      init
      timeout = @server.timeout
      return unless Numeric === timeout

      time = get_current_time
      @server.debug 'Ping ?, ?', client_id, time
      @redis.zadd(clients_key, time, client_id).errback do |error|
        @server.error "Faye::Redis: failed to ping client #{client_id} - #{error.message}"
      end
    end

    def subscribe(client_id, channel, &callback)
      init
      @redis.multi
      @redis.sadd(client_channels_key(client_id), channel)
      @redis.sadd(channel_key(channel), client_id)
      @redis.exec.callback do |results|
        if results && results[0]
          added = results[0]
          @server.trigger(:subscribe, client_id, channel) if added == 1
        end
        @server.debug 'Subscribed client ? to channel ?', client_id, channel
        callback&.call
      end.errback do |error|
        @server.error "Faye::Redis: subscribe failed for #{client_id} - #{error.message}"
      end
    end

    def unsubscribe(client_id, channel, &callback)
      init
      @redis.multi
      @redis.srem(client_channels_key(client_id), channel)
      @redis.srem(channel_key(channel), client_id)
      @redis.exec.callback do |results|
        if results && results[0]
          removed = results[0]
          @server.trigger(:unsubscribe, client_id, channel) if removed == 1
        end
        @server.debug 'Unsubscribed client ? from channel ?', client_id, channel
        callback&.call
      end.errback do |error|
        @server.error "Faye::Redis: unsubscribe failed for #{client_id} - #{error.message}"
      end
    end

    def publish(message, _channels)
      init
      @server.debug 'Publishing message ?', message

      json_message = MultiJson.dump(message)
      channels     = Faye::Channel.expand(message['channel'])
      keys         = channels.map { |c| channel_key(c) }

      @redis.sunion(*keys) do |clients|
        next unless clients && clients.any?

        clients.each do |client_id|
          queue = client_messages_key(client_id)

          @server.debug 'Queueing for client ?: ?', client_id, message
          @redis.rpush(queue, json_message) do
            @redis.publish(@message_channel, client_id)
          end.errback do |error|
            @server.error "Faye::Redis: failed to queue message for #{client_id} - #{error.message}"
          end
        end
      end.errback do |error|
        @server.error "Faye::Redis: failed to get subscribers - #{error.message}"
      end

      @server.trigger(:publish, message['clientId'], message['channel'], message['data'])
    end

    def empty_queue(client_id)
      return unless @server.has_connection?(client_id)

      init
      key = client_messages_key(client_id)

      @redis.multi
      @redis.lrange(key, 0, -1)
      @redis.del(key)
      @redis.exec.callback do |json_messages, _deleted|
        next unless json_messages

        begin
          messages = json_messages.map { |json| MultiJson.load(json) }
          @server.deliver(client_id, messages)
        rescue => e
          @server.error "Faye::Redis: failed to parse messages - #{e.message}"
        end
      end.errback do |error|
        @server.error "Faye::Redis: failed to empty queue for #{client_id} - #{error.message}"
      end
    end

    private

    def cleanup_connection
      EventMachine.cancel_timer(@gc) if @gc
      @subscriber = nil
      @redis = nil
      @gc = nil
      @initialized = false
      # Don't reset @reconnect_attempts here - keep track across reconnection cycles
    end

    def get_current_time
      (Time.now.to_f * 1000).to_i
    end

    def clients_key
      @ns + '/clients'
    end

    def client_channels_key(client_id)
      @ns + "/clients/#{client_id}/channels"
    end

    def client_messages_key(client_id)
      @ns + "/clients/#{client_id}/messages"
    end

    def channel_key(channel)
      @ns + "/channels#{channel}"
    end

    def lock_key(name)
      @ns + "/locks/#{name}"
    end

    def gc
      timeout = @server.timeout
      return unless Numeric === timeout

      with_lock 'gc' do |release_lock|
        cutoff = get_current_time - 1000 * 2 * timeout
        @redis.zrangebyscore(clients_key, 0, cutoff) do |clients|
          n = clients.size
          next release_lock.call if n.zero?

          completed = 0
          lock_released = false

          clients.each do |client_id|
            destroy_client(client_id) do
              completed += 1
              if completed == n && !lock_released
                lock_released = true
                release_lock.call
              end
            end
          end
        end.errback do |error|
          @server.error "Faye::Redis: gc failed to get stale clients - #{error.message}"
          release_lock.call
        end
      end
    end

    def handle_connection_failure
      return if @reconnecting

      @reconnecting = true
      @reconnect_attempts ||= 0
      @reconnect_attempts += 1

      # Exponential backoff: 1, 2, 4, 8, 16, 30, 30, 30...
      delay = [INITIAL_RECONNECT_DELAY * (2 ** (@reconnect_attempts - 1)), MAX_RECONNECT_DELAY].min

      @server.error "Faye::Redis: attempting to reconnect in #{delay}s (attempt #{@reconnect_attempts})"

      cleanup_connection

      EventMachine.add_timer(delay) do
        begin
          init
          if @initialized
            @reconnect_attempts = 0
            @reconnecting = false
          else
            @reconnecting = false
            handle_connection_failure
          end
        rescue => e
          @server.error "Faye::Redis: reconnection failed - #{e.message}"
          @reconnecting = false
          handle_connection_failure
        end
      end
    end

    def with_lock(lock_name, &block)
      key          = lock_key(lock_name)
      current_time = get_current_time
      expiry       = current_time + LOCK_TIMEOUT * 1000 + 1

      release_lock = lambda do
        @redis&.del(key) if @redis && get_current_time < expiry
      end

      @redis.setnx(key, expiry) do |set|
        next block.call(release_lock) if set == 1

        # Lock already held, check if it's expired
        @redis.get(key) do |timeout|
          next unless timeout

          lock_timeout = timeout.to_i(10)
          if current_time >= lock_timeout
            # Lock expired, try to acquire it
            @redis.getset(key, expiry) do |old_value|
              block.call(release_lock) if old_value == timeout
            end.errback do |error|
              @server.error "Faye::Redis: failed to acquire expired lock - #{error.message}"
            end
          else
            @server.debug 'Faye::Redis: lock ? already held and not expired', lock_name
          end
        end.errback do |error|
          @server.error "Faye::Redis: failed to check lock - #{error.message}"
        end
      end.errback do |error|
        @server.error "Faye::Redis: failed to acquire lock - #{error.message}"
      end
    end
  end
end
