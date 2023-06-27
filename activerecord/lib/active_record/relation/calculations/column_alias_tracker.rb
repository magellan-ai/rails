# frozen_string_literal: true

module ActiveRecord
  module Calculations
    class ColumnAliasTracker # :nodoc:
      def initialize(connection)
        @connection = connection
        @aliases = Hash.new(0)
      end

      def alias_for(field)
        aliased_name = column_alias_for(field)

        if @aliases[aliased_name] == 0
          @aliases[aliased_name] = 1
          aliased_name
        else
          # Update the count
          count = @aliases[aliased_name] += 1
          "#{truncate(aliased_name)}_#{count}"
        end
      end

      private
        # Converts the given field to the value that the database adapter returns as
        # a usable column name:
        #
        #   column_alias_for("users.id")                 # => "users_id"
        #   column_alias_for("sum(id)")                  # => "sum_id"
        #   column_alias_for("count(distinct users.id)") # => "count_distinct_users_id"
        #   column_alias_for("count(*)")                 # => "count_all"
        def column_alias_for(field)
          column_alias = +field
          column_alias.gsub!(/\*/, "all")
          column_alias.gsub!(/\W+/, " ")
          column_alias.strip!
          column_alias.gsub!(/ +/, "_")
          @connection.table_alias_for(column_alias)
        end

        def truncate(name)
          name.slice(0, @connection.table_alias_length - 2)
        end
    end
  end
end
