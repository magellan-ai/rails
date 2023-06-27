# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class SumOperation  < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = 0

      private

      def select_value_for_calc
        super.tap { |select_value| select_value.distinct = true if distinct? }
      end
    end
  end
end
