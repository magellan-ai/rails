# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class RelationManager # :nodoc:
      attr_reader :relation

      delegate :has_limit_or_offset?, :primary_key, :group_values, to: :@relation

      def initialize(relation)
        @relation = relation
      end

      def grouped?
        @relation.group_values.any?
      end


      def has_include?(column_name)
        @relation.eager_loading? || (@relation.includes_values.present? && column_name && column_name != :all)
      end

      def apply_join_dependency!
        @relation = @relation.send(:apply_join_dependency)
      end

      def prep_for_count!(relation, column_name)
        unless relation.distinct_value || distinct_select?(column_name || select_for_count)
          relation.distinct!
          relation.select_values = [ relation.klass.primary_key || relation.table[Arel.star] ]
        end
        # PostgreSQL: ORDER BY expressions must appear in SELECT list when using DISTINCT
        relation.order_values = [] if relation.group_values.empty?
      end

      def distinct?
        @relation.distinct_value
      end

      def distinct_select?(column_name)
        column_name.is_a?(::String) && /\bDISTINCT[\s(]/i.match?(column_name)
      end

      def select_for_count
        if @relation.select_values.present?
          return @relation.select_values.first if @relation.select_values.one?
          @relation.select_values.join(", ")
        else
          :all
        end
      end

      def operate_on_pk?
        @relation.select_values.empty? && @relation.order_values.empty?
      end

      def relation_for_calc(select_value)
        # PostgreSQL doesn't like ORDER BY when there are no GROUP BY
        relation = @relation.unscope(:order).distinct!(false)
        relation.select_values = [select_value]
        relation
      end
    end
  end
end
