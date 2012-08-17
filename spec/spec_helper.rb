$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "..", "lib"))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require "rspec"
require "olap4r"
require "yaml"

# Requires supporting files with custom matchers and macros, etc,
# in ./support/ and its subdirectories.
Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each {|f| require f}

RSpec.configure do |config|
end

RSPEC_CONFIG = YAML.load_file "spec/config.yml"
require "olap4r-mondrian"
require "olap4r-xmla"

require RSPEC_CONFIG["mondrian"]["jdbc_driver_path"]