# frozen_string_literal: true

require "activerecord/lib/active_record/relation/calculations/relation_manager"

require "activerecord/lib/active_record/relation/calculations/grouped_calculation"
require "activerecord/lib/active_record/relation/calculations/simple_calculation"

require "activerecord/lib/active_record/relation/calculations/average_operation"
require "activerecord/lib/active_record/relation/calculations/count_operation"
require "activerecord/lib/active_record/relation/calculations/maximum_operation"
require "activerecord/lib/active_record/relation/calculations/minimum_operation"
require "activerecord/lib/active_record/relation/calculations/sum_operation"

module ActiveRecord
  module Calculations
    class Calculator # :nodoc:
      def initialize(relation, operation_name, column_name, none: false, async: false)
        @relation_manager = RelationManager.new(relation)

        calculation_class = @relation_manager.grouped? ? GroupedCalculation : SimpleCalculation
        @calculation = calculation_class.new(@relation_manager, none: none, async: async)

        operation_class = "#{self.class.module_parent_name}::#{operation_name.to_s.classify}Operation".constantize
        @operation = operation_class.new(@relation_manager, column_name)
      end

      def perform
        return @calculation.empty_value(@operation) if @calculation.none?

        @operation.apply_join_dependency! if @relation_manager.has_include?(@column_name)

        @calculation.execute
      end
    end
  end
end
