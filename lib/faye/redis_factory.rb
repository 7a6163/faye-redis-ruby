module Faye
  class RedisFactory

    DEFAULT_HOST     = '0.0.0.0'
    DEFAULT_PORT     = 6379
    DEFAULT_DATABASE = 0

    def initialize(options)
      @options = options
    end

    def call
      if @options[:uri]
        EventMachine::Hiredis.connect(@options[:uri])
      elsif @options[:socket]
        EventMachine::Hiredis::Client.new(
          @options[:socket],
          nil,
          @options[:password],
          @options[:database] || DEFAULT_DATABASE
        ).connect
      else
        EventMachine::Hiredis::Client.new(
          @options[:host] || DEFAULT_HOST,
          @options[:port] || DEFAULT_PORT,
          @options[:password],
          @options[:database] || DEFAULT_DATABASE
        ).connect
      end
    end

  end
end
