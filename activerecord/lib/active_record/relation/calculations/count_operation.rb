# frozen_string_literal: true

require_relative 'abstract_operation'

module ActiveRecord
  module Calculations
    class CountOperation < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = 0

      def apply_join_dependency!
        super.tap { |relation| @relation_manager.prep_for_count! relation, @column_name }
      end

      def distinct?
        distinct = super
        @column_name ||= @relation_manager.select_for_count
        if @column_name == :all
          if !distinct
            distinct = @relation_manager.distinct_select?(@relation_manager.select_for_count) if @relation_manager.grouped?
          elsif @relation_manager.grouped? || @relation_manager.operate_on_pk?
            @column_name = @relation_manager.primary_key
          end
        elsif @relation_manager.distinct_select?(@column_name)
          distinct = nil
        end

        distinct
      end

      def short_circuit?

      end

      def short_circuit_value
        0
      end
    end
  end
end
