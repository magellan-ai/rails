# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class SumOperation < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = 0

      def over_aggregate_column(column)
        column.public_send(name_in_query)
      end

      def cast_result(value, type)
        type.deserialize(value || 0)
      end

      private

      def select_value_for_calc
        super.tap { |select_value| select_value.distinct = true if distinct? }
      end
    end
  end
end
