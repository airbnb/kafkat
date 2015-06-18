# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'kafkat/version'

Gem::Specification.new do |s|
  s.name         = 'kafkat'
  s.version      = Kafkat::VERSION
  s.platform     = Gem::Platform::RUBY
  s.authors      = ["Nelson Gauthier"]
  s.email        = ['nelson@airbnb.com']
  s.homepage     = 'https://github.com/airbnb/kafkat'
  s.summary      = "Simplified command-line administration for Kafka brokers"
  s.description  = s.summary
  s.license      = 'Apache-v2'

  s.files        = `git ls-files`.split($/)
  s.executables  = s.files.grep(%r{^bin/}) { |f| File.basename f }
  s.test_files   = s.files.grep(%r{^(test|spec|features)/})
  s.require_path = 'lib'

  s.add_runtime_dependency 'zk', '~> 1.9', '>= 1.9.4'
  s.add_runtime_dependency 'trollop', '~> 2.0'
  s.add_runtime_dependency 'highline', '~> 1.6', '>= 1.6.21'
  s.add_runtime_dependency 'retryable', '~> 1.3', '>= 1.3.5'
  s.add_runtime_dependency 'colored', '~> 1.2'

  s.add_development_dependency 'rake'
  s.add_development_dependency 'pry'
  s.add_development_dependency 'simplecov'
  s.add_development_dependency 'rspec', '~> 3.2.0'
  s.add_development_dependency 'rspec-collection_matchers', '~> 1.1.0'
  s.add_development_dependency 'factory_girl', '~> 4.5.0'
end
