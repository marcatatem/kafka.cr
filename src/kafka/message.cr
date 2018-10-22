require "./lib_rdkafka.cr"

module Kafka
  class Message
    def initialize(@msg : LibKafkaC::Message*)
    end

    def payload : Bytes?
      return nil if @msg.null?
      tmp = @msg.value
      return nil if tmp.len == 0
      Bytes.new(tmp.payload, tmp.len)
    end

    def key : Bytes?
      return nil if @msg.null?
      tmp = @msg.value
      return nil if tmp.key_len == 0
      Bytes.new(tmp.key, tmp.key_len)
    end

    def offset : Int64?
      return nil if @msg.null?
      return @msg.value.offset
    end

    def timestamp : Int64?
      return nil if @msg.null?
      p @msg.value.timestamp
      @msg.value.timestamp.try &.timestamp
    end

    def err : Int32?
      return nil if @msg.null?
      return @msg.value.err
    end

    def valid?
      !@msg.null?
    end

    def to_unsafe
      @msg
    end

    def finalize
      LibKafkaC.message_destroy(@msg) unless @msg.null?
    end
  end
end
