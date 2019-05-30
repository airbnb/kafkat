# frozen_string_literal: true

module Kafkat
  module Logging
    def print_err(message)
      STDERR.print message
    end
  end
end
