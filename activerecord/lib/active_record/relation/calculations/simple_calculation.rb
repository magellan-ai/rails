# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class SimpleCalculation < AbstractCalculation # :nodoc:

      private
      def build_count_subquery(relation, column_name, distinct)
        if column_name == :all
          column_alias = Arel.star
          relation.select_values = [ Arel.sql(FinderMethods::ONE_AS_ONE) ] unless distinct
        else
          column_alias = Arel.sql("count_column")
          relation.select_values = [ aggregate_column(column_name).as(column_alias) ]
        end

        subquery_alias = Arel.sql("subquery_for_count")
        select_value = operation_over_aggregate_column(column_alias, "count", false)

        relation.build_subquery(subquery_alias, select_value)
      end
        def default_empty_value(operation)
          operation.class::SINGLE_EMPTY_RESULT
        end



        def perform_calculation
          return @operation.short_circuit_value if @operation.respond_to?(:short_circuit?) && @operation.short_circuit?


          if operation == "count" && ((column_name == :all && @distinct) || @relation_manager.has_limit_or_offset?)
            # Shortcut when limit is zero.
            return 0 if limit_value == 0

            query_builder = build_count_subquery(spawn, column_name, distinct)
          else
            # PostgreSQL doesn't like ORDER BY when there are no GROUP BY
            relation = @relation_manager.relation

            aggregate_column = aggregate_column(column_name)
            select_value = operation_over_aggregate_column(aggregate_column, operation, relation.distinct)
            select_value.distinct = true if operation == "sum" && distinct

            relation.select_values = [select_value]

            query_builder = relation.arel
          end

          query_result = if relation.where_clause.contradiction?
                           ActiveRecord::Result.empty
                         else
                           relation.send(:skip_query_cache_if_necessary) do
                             @klass.connection.select_all(query_builder, "#{@klass.name} #{operation.capitalize}", async: @async)
                           end
                         end

          query_result.then do |result|
            if operation != "count"
              type = aggregate_column.try(:type_caster) || relation.send(:lookup_cast_type_from_join_dependencies, column_name.to_s) || Type.default_value
              type = type.subtype if Enum::EnumType === type
            end

            type_cast_calculated_value(result.cast_values.first, operation, type)
          end
        end
    end
  end
end
