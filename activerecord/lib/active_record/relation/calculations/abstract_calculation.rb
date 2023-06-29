# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class AbstractCalculation # :nodoc:
      delegate :column_name, to: :@operation

      def initialize(relation_manager, klass: nil, none: false, async: false, operation: nil)
        @relation_manager = relation_manager
        @none = none
        @async = async
        @operation = operation
        @klass = klass
      end

      def type_cast_calculated_value(value, operation, type)
        operation.cast_result(value, type)
      end

      def operation_over_aggregate_column(column, operation)
        operation.over_aggregate_column(column)
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
