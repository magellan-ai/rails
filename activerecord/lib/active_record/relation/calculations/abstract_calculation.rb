# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class AbstractCalculation # :nodoc:
      def initialize(relation_manager, none: false, async: false)
        @relation_manager = relation_manager
        @none             = none
        @async            = async
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
    end
  end
end
