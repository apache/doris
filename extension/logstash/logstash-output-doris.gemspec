=begin
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
=end
Gem::Specification.new do |s|
  s.name            = 'logstash-output-doris'
  s.version         = '1.0.1'
  s.author          = 'Apache Doris'
  s.email           = 'dev@doris.apache.org'
  s.homepage        = 'http://doris.apache.org'
  s.licenses        = ['Apache-2.0']
  s.summary         = "This output lets you `PUT` messages in a batched fashion to Doris HTTP endpoint"
  s.description     = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','*.gemspec','*.md','Gemfile','LICENSE' ]

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency 'mini_cache', ">= 1.0.0", "< 2.0.0"
  s.add_runtime_dependency "rest-client", '~> 2.1'

  s.add_development_dependency 'logstash-devutils', '~> 2.0', '>= 2.0.3'
  s.add_development_dependency 'sinatra', '~> 2.0', '>= 2.0.8.1'
  s.add_development_dependency 'webrick', '~> 1.6'
end
