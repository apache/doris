<!-- 
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
-->

# logstash-output-doris

A Logstash output plugin that ships events to Doris via stream load.

Docs: <https://doris.apache.org/docs/dev/ecosystem/logstash>

## Build

Prerequisites: JRuby (>= 9.4 with Java 21, or 9.2 with Java 8/11) and the
`jar-dependencies` gem (`jruby -S gem install jar-dependencies`).

```bash
# 1. Vendor HttpClient4 + transitive jars into lib/ and generate the loader.
#    Reads the 'jar' entry from logstash-output-doris.gemspec.
jruby -e "require 'jars/installer'; Jars::Installer.new.vendor_jars"

# 2. Build the gem.
jruby -S gem build logstash-output-doris.gemspec
```

Produces `logstash-output-doris-<version>-java.gem`.

## Install

The jars are already vendored inside the gem, so the install hook does not
need to talk to Maven — pass `JARS_SKIP=true` to skip the lookup:

```bash
JARS_SKIP=true $LS_HOME/bin/logstash-plugin install \
    logstash-output-doris-<version>-java.gem
```

### Air-gapped install (offline pack)

For a Logstash host without internet access, build an offline pack on a
connected host first:

```bash
# On a connected host (same Logstash major version as the target):
JARS_SKIP=true $LS_HOME/bin/logstash-plugin install \
    logstash-output-doris-<version>-java.gem
$LS_HOME/bin/logstash-plugin prepare-offline-pack \
    --output logstash-output-doris-<version>-offline-pack.zip \
    logstash-output-doris

# Then on the air-gapped target:
$LS_HOME/bin/logstash-plugin install \
    file:///path/to/logstash-output-doris-<version>-offline-pack.zip
```
