# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class AbstractOperation # :nodoc:
      delegate :distinct?, to: :@relation_manager

      def initialize(relation_manager, column_name)
        @relation_manager = relation_manager
        @column_name = column_name
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
