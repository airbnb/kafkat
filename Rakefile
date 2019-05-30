# frozen_string_literal: true

require 'bundler/gem_tasks'
require 'rspec/core/rake_task'
require 'rake/clean'

CLEAN << 'reports'
CLOBBER << FileList['kafkat*.gem']

desc 'Run all specs'
RSpec::Core::RakeTask.new :spec do |t|
  t.pattern = ['spec/**/*_spec.rb']
  t.rspec_opts = %w(--order random --format documentation --color --format html --out reports/specs.html)
  t.rspec_opts << '--backtrace' if ENV['backtrace']
  t.verbose = true
end

desc 'Run all specs and generate spec and coverage reports'
task :coverage do
  ENV['COVERAGE'] = 'true'
  Rake::Task['spec'].invoke
end

task test: :spec
task default: :coverage
