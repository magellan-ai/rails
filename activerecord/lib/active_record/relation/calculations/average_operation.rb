# frozen_string_literal: true

require_relative 'abstract_operation'

module ActiveRecord
  module Calculations
    class AverageOperation  < AbstractOperation # :nodoc:
      SINGLE_EMPTY_RESULT = nil
    end
  end
end
