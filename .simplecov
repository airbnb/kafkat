if ENV['COVERAGE']
  SimpleCov.start do
    coverage_dir("reports/coverage")
  end
end
