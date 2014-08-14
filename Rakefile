require "bundler/gem_tasks"
require 'rspec/core/rake_task'
task :default => :spec

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = 'spec/**/*_spec.rb'
  spec.rspec_opts = ['-cfs --backtrace']
end
