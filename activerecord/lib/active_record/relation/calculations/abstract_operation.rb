# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class AbstractOperation # :nodoc:
      attr_reader :column_name

      delegate :distinct?, to: :@relation_manager

      def initialize(relation_manager, column_name)
        @relation_manager = relation_manager
        @column_name = column_name
        # hackathon this method shouldn't be called here
        # but its functionalities are necessary in order to have the
        # right column_name
        distinct?
      end

      def apply_join_dependency!
        @relation_manager.apply_join_dependency!
      end

      def short_circuit?
        false
      end

      def query_builder
        @column = aggregate_column(@column_name)
        @relation_manager.relation_for_calc(select_value_for_calc).arel
      end

      private

      def select_value_for_calc
        operation_over_aggregate_column(@column, distinct?)
      end
    end
  end
end
