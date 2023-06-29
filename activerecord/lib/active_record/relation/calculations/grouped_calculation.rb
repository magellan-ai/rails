# frozen_string_literal: true

require_relative "abstract_calculation"

module ActiveRecord
  module Calculations
    class GroupedCalculation < AbstractCalculation # :nodoc:

      private
        def default_empty_value
          Hash.new
        end

        def perform_calculation # :nodoc:
          group_fields = relation.group_values
          group_fields = group_fields.uniq if group_fields.size > 1

          if group_fields.size == 1 && group_fields.first.respond_to?(:to_sym)
            association = relation.klass._reflect_on_association(group_fields.first)
            associated = association && association.belongs_to? # only count belongs_to associations
            group_fields = Array(association.foreign_key) if associated
          end
          group_fields = relation.arel_columns(group_fields)

          column_alias_tracker = ColumnAliasTracker.new(relation.connection)

          group_aliases = group_fields.map { |field|
            field = relation.connection.visitor.compile(field) if Arel.arel_node?(field)
            column_alias_tracker.alias_for(field.to_s.downcase)
          }
          group_columns = group_aliases.zip(group_fields)

          column = aggregate_column(column_name)
          column_alias = column_alias_tracker.alias_for("#{@operation} #{column_name.to_s.downcase}")
          select_value = operation_over_aggregate_column(column, @operation)
          select_value.as(relation.connection.quote_column_name(column_alias))

          select_values = [select_value]
          select_values += relation.select_values unless relation.having_clause.empty?

          select_values.concat group_columns.map { |aliaz, field|
            aliaz = relation.connection.quote_column_name(aliaz)
            if field.respond_to?(:as)
              field.as(aliaz)
            else
              "#{field} AS #{aliaz}"
            end
          }

          curr_relation = relation.except(:group).distinct!(false)
          curr_relation.group_values = group_fields
          curr_relation.select_values = select_values

          result = curr_relation.send(:skip_query_cache_if_necessary) { @klass.connection.select_all(curr_relation.arel, "#{@klass.name} #{@operation.name_in_query.capitalize}", async: @async) }
          # result = curr_relation.skip_query_cache_if_necessary { @klass.connection.select_all(curr_relation.arel, "#{@klass.name} #{operation.capitalize}", async: @async) }
          #
          result.then do |calculated_data|
            if association
              key_ids = calculated_data.collect { |row| row[group_aliases.first] }
              key_records = association.klass.base_class.where(association.klass.base_class.primary_key => key_ids)
              key_records = key_records.index_by(&:id)
            end

            key_types = group_columns.each_with_object({}) do |(aliaz, col_name), types|
              types[aliaz] = col_name.try(:type_caster) ||
                type_for(col_name) do
                  calculated_data.column_types.fetch(aliaz, Type.default_value)
                end
            end

            hash_rows = calculated_data.cast_values(key_types).map! do |row|
              calculated_data.columns.each_with_object({}).with_index do |(col_name, hash), i|
                hash[col_name] = row[i]
              end
            end

            # hackathon operation is not a string
            # hackathon TODO checking type/name is a sign of having a polymorphic behaviour
            if @operation.name_in_query != "count"
              type = column.try(:type_caster) ||
                relation.send(:lookup_cast_type_from_join_dependencies, column_name.to_s) || Type.default_value
              type = type.subtype if Enum::EnumType === type
            end

            hash_rows.each_with_object({}) do |row, result|
              key = group_aliases.map { |aliaz| row[aliaz] }
              key = key.first if key.size == 1
              key = key_records[key] if associated

              result[key] = type_cast_calculated_value(row[column_alias], @operation, type)
            end
          end
        end

        def type_for(field, &block)
          field_name = field.respond_to?(:name) ? field.name.to_s : field.to_s.split(".").last
          @klass.type_for_attribute(field_name, &block)
        end

        def relation
          @relation_manager.relation
        end
    end
  end
end
