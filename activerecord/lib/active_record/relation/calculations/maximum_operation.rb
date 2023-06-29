# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class MaximumOperation < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = nil

      def over_aggregate_column(column)
        column.public_send(name_in_query)
      end

      def cast_result(value, type)
        type.deserialize(value)
      end
    end
  end
end
