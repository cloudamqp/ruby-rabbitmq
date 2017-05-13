
module RabbitMQ
  module FFI
    
    class FieldValue
      private def value_member(kind)
        case kind
        when :utf8;      :bytes
        when :timestamp; :u64
        else kind
        end
      end
      
      def to_value(free=false)
        kind   = self[:kind]
        value  = self[:value][value_member(kind)]
        result = case kind
        when :bytes;     value.to_s(free)
        when :utf8;      value.to_s(free).force_encoding(Encoding::UTF_8)
        when :timestamp; Time.at(value).utc
        when :table;     value.to_h(free)
        when :array;     value.to_a(free)
        else value
        end
        
        clear if free
        result
      end
      
      def free!
        kind  = self[:kind]
        value = self[:value][value_member(kind)]
        value.free! if value.respond_to? :free!
        self
      end

      def apply(value)
        self[:kind], self[:value] =
          case value
          when ::String  then [:utf8,      FieldValueValue.new(Bytes.from_s(value.encode(Encoding::UTF_8)).pointer)]
          when ::Symbol  then [:utf8,      FieldValueValue.new(Bytes.from_s(value.to_s.encode!(Encoding::UTF_8)).pointer)]
          when ::Array   then [:array,     FieldValueValue.new(Array.from_a(value).pointer)]
          when ::Hash    then [:table,     FieldValueValue.new(Table.from(value).pointer)]
          when ::Float   then [:f64,       FieldValueValue.new.tap { |v| v[:f64] = value }]
          when ::Time    then [:timestamp, FieldValueValue.new.tap { |v| v[:u64] = value.to_i }]
          when true      then [:boolean,   FieldValueValue.new.tap { |v| v[:boolean] = true }]
          when false     then [:boolean,   FieldValueValue.new.tap { |v| v[:boolean] = false }]
          when ::Integer
            if -(2**31) < value && value < 2**31 - 1
              [:i32, FieldValueValue.new.tap { |v| v[:i32] = value }]
            else
              [:i64, FieldValueValue.new.tap { |v| v[:i64] = value }]
            end
          else raise NotImplementedError, "#{self.class}.from(#<#{value.class}>)"
          end
        self
      end

      def self.from(value)
        new.apply(value)
      end
    end
  end
end
