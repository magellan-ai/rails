# frozen_string_literal: true

require_relative 'abstract_operation'

module ActiveRecord
  module Calculations
    class AverageOperation  < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = nil

      def over_aggregate_column(column)
        column.public_send(name_in_query)
      end

      def cast_result(value, type)
        case type.type
        when :integer, :decimal
          value&.to_d
        else
          type.deserialize(value)
        end
      end

    end
  end
end
