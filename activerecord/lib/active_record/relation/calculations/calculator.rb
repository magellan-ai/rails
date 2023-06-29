# frozen_string_literal: true

require_relative "relation_manager"

require_relative "grouped_calculation"
require_relative "simple_calculation"

require_relative "average_operation"
require_relative "count_operation"
require_relative "maximum_operation"
require_relative "minimum_operation"
require_relative "sum_operation"

module ActiveRecord
  module Calculations
    class Calculator # :nodoc:
      def initialize(relation, operation_name, column_name, none: false, async: false, klass: nil)
        @relation_manager = RelationManager.new(relation)
        @column_name = column_name

        operation_class = "#{self.class.module_parent_name}::#{operation_name.to_s.classify}Operation".constantize
        @operation = operation_class.new(@relation_manager, column_name)

        calculation_class = @relation_manager.grouped? ? GroupedCalculation : SimpleCalculation
        @calculation = calculation_class.new(@relation_manager, none: none, klass: klass, async: async, operation: @operation)
      end

      def perform
        return @calculation.empty_value(@operation) if @calculation.none?

        @operation.apply_join_dependency! if @relation_manager.has_include?(@column_name)

        @calculation.execute
      end
    end
  end
end
