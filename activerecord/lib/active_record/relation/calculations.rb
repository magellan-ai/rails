# frozen_string_literal: true

require "active_support/core_ext/enumerable"

require_relative "calculations/calculator"
require_relative "calculations/column_alias_tracker"

module ActiveRecord
  # = Active Record \Calculations
  module Calculations
    # Count the records.
    #
    #   Person.count
    #   # => the total count of all people
    #
    #   Person.count(:age)
    #   # => returns the total count of all people whose age is present in database
    #
    #   Person.count(:all)
    #   # => performs a COUNT(*) (:all is an alias for '*')
    #
    #   Person.distinct.count(:age)
    #   # => counts the number of different age values
    #
    # If #count is used with {Relation#group}[rdoc-ref:QueryMethods#group],
    # it returns a Hash whose keys represent the aggregated column,
    # and the values are the respective amounts:
    #
    #   Person.group(:city).count
    #   # => { 'Rome' => 5, 'Paris' => 3 }
    #
    # If #count is used with {Relation#group}[rdoc-ref:QueryMethods#group] for multiple columns, it returns a Hash whose
    # keys are an array containing the individual values of each column and the value
    # of each key would be the #count.
    #
    #   Article.group(:status, :category).count
    #   # =>  {["draft", "business"]=>10, ["draft", "technology"]=>4, ["published", "technology"]=>2}
    #
    # If #count is used with {Relation#select}[rdoc-ref:QueryMethods#select], it will count the selected columns:
    #
    #   Person.select(:age).count
    #   # => counts the number of different age values
    #
    # Note: not all valid {Relation#select}[rdoc-ref:QueryMethods#select] expressions are valid #count expressions. The specifics differ
    # between databases. In invalid cases, an error from the database is thrown.
    def count(column_name = nil)
      if block_given?
        unless column_name.nil?
          raise ArgumentError, "Column name argument is not supported when a block is passed."
        end

        super()
      else
        calculate(:count, column_name)
      end
    end

    # Same as <tt>#count</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_count(column_name = nil)
      async.count(column_name)
    end

    # Calculates the average value on a given column. Returns +nil+ if there's
    # no row. See #calculate for examples with options.
    #
    #   Person.average(:age) # => 35.8
    def average(column_name)
      calculate(:average, column_name)
    end

    # Same as <tt>#average</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_average(column_name)
      async.average(column_name)
    end

    # Calculates the minimum value on a given column. The value is returned
    # with the same data type of the column, or +nil+ if there's no row. See
    # #calculate for examples with options.
    #
    #   Person.minimum(:age) # => 7
    def minimum(column_name)
      calculate(:minimum, column_name)
    end

    # Same as <tt>#minimum</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_minimum(column_name)
      async.minimum(column_name)
    end

    # Calculates the maximum value on a given column. The value is returned
    # with the same data type of the column, or +nil+ if there's no row. See
    # #calculate for examples with options.
    #
    #   Person.maximum(:age) # => 93
    def maximum(column_name)
      calculate(:maximum, column_name)
    end

    # Same as <tt>#maximum</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_maximum(column_name)
      async.maximum(column_name)
    end

    # Calculates the sum of values on a given column. The value is returned
    # with the same data type of the column, +0+ if there's no row. See
    # #calculate for examples with options.
    #
    #   Person.sum(:age) # => 4562
    def sum(initial_value_or_column = 0, &block)
      if block_given?
        map(&block).sum(initial_value_or_column)
      else
        calculate(:sum, initial_value_or_column)
      end
    end

    # Same as <tt>#sum</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_sum(identity_or_column = nil)
      async.sum(identity_or_column)
    end

    # This calculates aggregate values in the given column. Methods for #count, #sum, #average,
    # #minimum, and #maximum have been added as shortcuts.
    #
    #   Person.calculate(:count, :all) # The same as Person.count
    #   Person.average(:age) # SELECT AVG(age) FROM people...
    #
    #   # Selects the minimum age for any family without any minors
    #   Person.group(:last_name).having("min(age) > 17").minimum(:age)
    #
    #   Person.sum("2 * age")
    #
    # There are two basic forms of output:
    #
    # * Single aggregate value: The single value is type cast to Integer for COUNT, Float
    #   for AVG, and the given column's type for everything else.
    #
    # * Grouped values: This returns an ordered hash of the values and groups them. It
    #   takes either a column name, or the name of a belongs_to association.
    #
    #      values = Person.group('last_name').maximum(:age)
    #      puts values["Drake"]
    #      # => 43
    #
    #      drake  = Family.find_by(last_name: 'Drake')
    #      values = Person.group(:family).maximum(:age) # Person belongs_to :family
    #      puts values[drake]
    #      # => 43
    #
    #      values.each do |family, max_age|
    #        ...
    #      end
    def calculate(operation, column_name)
      Calculator.new(self, operation, column_name, none: @none, async: @async, klass: @klass).perform
    end

    # Use #pluck as a shortcut to select one or more attributes without
    # loading an entire record object per row.
    #
    #   Person.pluck(:name)
    #
    # instead of
    #
    #   Person.all.map(&:name)
    #
    # Pluck returns an Array of attribute values type-casted to match
    # the plucked column names, if they can be deduced. Plucking an SQL fragment
    # returns String values by default.
    #
    #   Person.pluck(:name)
    #   # SELECT people.name FROM people
    #   # => ['David', 'Jeremy', 'Jose']
    #
    #   Person.pluck(:id, :name)
    #   # SELECT people.id, people.name FROM people
    #   # => [[1, 'David'], [2, 'Jeremy'], [3, 'Jose']]
    #
    #   Person.distinct.pluck(:role)
    #   # SELECT DISTINCT role FROM people
    #   # => ['admin', 'member', 'guest']
    #
    #   Person.where(age: 21).limit(5).pluck(:id)
    #   # SELECT people.id FROM people WHERE people.age = 21 LIMIT 5
    #   # => [2, 3]
    #
    #   Person.pluck(Arel.sql('DATEDIFF(updated_at, created_at)'))
    #   # SELECT DATEDIFF(updated_at, created_at) FROM people
    #   # => ['0', '27761', '173']
    #
    # See also #ids.
    def pluck(*column_names)
      return [] if @none

      if loaded? && all_attributes?(column_names)
        result = records.pluck(*column_names)
        if @async
          return Promise::Complete.new(result)
        else
          return result
        end
      end

      if has_include?(column_names.first)
        relation = apply_join_dependency
        relation.pluck(*column_names)
      else
        klass.disallow_raw_sql!(column_names.flatten)
        columns = arel_columns(column_names)
        relation = spawn
        relation.select_values = columns
        result = skip_query_cache_if_necessary do
          if where_clause.contradiction?
            ActiveRecord::Result.empty(async: @async)
          else
            klass.connection.select_all(relation.arel, "#{klass.name} Pluck", async: @async)
          end
        end
        result.then do |result|
          type_cast_pluck_values(result, columns)
        end
      end
    end

    # Same as <tt>#pluck</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_pluck(*column_names)
      async.pluck(*column_names)
    end

    # Pick the value(s) from the named column(s) in the current relation.
    # This is short-hand for <tt>relation.limit(1).pluck(*column_names).first</tt>, and is primarily useful
    # when you have a relation that's already narrowed down to a single row.
    #
    # Just like #pluck, #pick will only load the actual value, not the entire record object, so it's also
    # more efficient. The value is, again like with pluck, typecast by the column type.
    #
    #   Person.where(id: 1).pick(:name)
    #   # SELECT people.name FROM people WHERE id = 1 LIMIT 1
    #   # => 'David'
    #
    #   Person.where(id: 1).pick(:name, :email_address)
    #   # SELECT people.name, people.email_address FROM people WHERE id = 1 LIMIT 1
    #   # => [ 'David', 'david@loudthinking.com' ]
    def pick(*column_names)
      if loaded? && all_attributes?(column_names)
        result = records.pick(*column_names)
        return @async ? Promise::Complete.new(result) : result
      end

      limit(1).pluck(*column_names).then(&:first)
    end

    # Same as <tt>#pick</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_pick(*column_names)
      async.pick(*column_names)
    end

    # Returns the base model's ID's for the relation using the table's primary key
    #
    #   Person.ids # SELECT people.id FROM people
    #   Person.joins(:companies).ids # SELECT people.id FROM people INNER JOIN companies ON companies.id = people.company_id
    def ids
      primary_key_array = Array(primary_key)

      if loaded?
        result = records.map do |record|
          if primary_key_array.one?
            record._read_attribute(primary_key_array.first)
          else
            primary_key_array.map { |column| record._read_attribute(column) }
          end
        end
        return @async ? Promise::Complete.new(result) : result
      end

      if has_include?(primary_key)
        relation = apply_join_dependency.group(*primary_key_array)
        return relation.ids
      end

      columns = arel_columns(primary_key_array)
      relation = spawn
      relation.select_values = columns

      result = if relation.where_clause.contradiction?
        ActiveRecord::Result.empty
      else
        skip_query_cache_if_necessary do
          klass.connection.select_all(relation, "#{klass.name} Ids", async: @async)
        end
      end

      result.then { |result| type_cast_pluck_values(result, columns) }
    end

    # Same as <tt>#ids</tt> but perform the query asynchronously and returns an ActiveRecord::Promise
    def async_ids
      async.ids
    end

    private
      def all_attributes?(column_names)
        (column_names.map(&:to_s) - @klass.attribute_names - @klass.attribute_aliases.keys).empty?
      end

      def has_include?(column_name)
        eager_loading? || (includes_values.present? && column_name && column_name != :all)
      end

      def type_for(field, &block)
        field_name = field.respond_to?(:name) ? field.name.to_s : field.to_s.split(".").last
        @klass.type_for_attribute(field_name, &block)
      end

      def lookup_cast_type_from_join_dependencies(name, join_dependencies = build_join_dependencies)
        each_join_dependencies(join_dependencies) do |join|
          type = join.base_klass.attribute_types.fetch(name, nil)
          return type if type
        end
        nil
      end

      def type_cast_pluck_values(result, columns)
        cast_types = if result.columns.size != columns.size
          klass.attribute_types
        else
          join_dependencies = nil
          columns.map.with_index do |column, i|
            column.try(:type_caster) ||
              klass.attribute_types.fetch(name = result.columns[i]) do
                join_dependencies ||= build_join_dependencies
                lookup_cast_type_from_join_dependencies(name, join_dependencies) ||
                  result.column_types[name] || Type.default_value
              end
          end
        end
        result.cast_values(cast_types)
      end
  end
end
