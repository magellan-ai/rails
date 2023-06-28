# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class AbstractCalculation # :nodoc:
      attr_reader :column_name
      def initialize(relation_manager, klass: nil, none: false, async: false, column_name: nil, operation: nil)
        @relation_manager = relation_manager
        @none = none
        @async = async
        @column_name = column_name
        @operation = operation
        @klass = klass
      end

      def type_cast_calculated_value(value, operation, type)
        case operation
        when :count
          value.to_i
        when :sum
          type.deserialize(value || 0)
        when :average
          case type.type
          when :integer, :decimal
            value&.to_d
          else
            type.deserialize(value)
          end
        else
          # "minimum", "maximum"
          type.deserialize(value)
        end
      end

      def operation_over_aggregate_column(column, operation, distinct)
        operation == :count ? column.count(distinct) : column.public_send(operation)
      end

      def aggregate_column(column_name)
        return column_name if Arel::Expressions === column_name

        @relation_manager.relation.send(:arel_column, column_name.to_s) do |name|
          Arel.sql(column_name == :all ? "*" : name)
        end
      end

      def none?
        @none
      end

      def empty_value(operation)
        default_empty_value(operation).then do |result|
          @async ? Promise::Complete.new(result) : result
        end
      end

      def execute
        # If #count is used with #distinct (i.e. `relation.distinct.count`) it is
        # considered distinct.
        @distinct = @relation_manager.distinct?

        perform_calculation
      end

      def default_empty_value(_)
        raise NotImplementedError
      end

      def perform_calculation
        raise NotImplementedError
      end
    end
  end
end
