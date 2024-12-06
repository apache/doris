# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import env
import os

# online install
status = os.system(f'{env.LOGSTASH_HOME}/bin/logstash-plugin install {env.LOGSTASH_DORIS_DIR}/{env.LOGSTASH_OUTPUT_DORIS}.gem')
if status != 0:
    raise Exception('Failed to install logstash extension online')

# package
status = os.system(f'{env.LOGSTASH_HOME}/bin/logstash-plugin prepare-offline-pack --overwrite --output {env.LOGSTASH_DORIS_DIR}/{env.LOGSTASH_OUTPUT_DORIS}.zip {env.LOGSTASH_OUTPUT_DORIS}')
if status != 0:
    raise Exception('Failed to package logstash extension')

# offline install
status = os.system(f'{env.LOGSTASH_HOME}/bin/logstash-plugin install file://{env.LOGSTASH_DORIS_DIR}/{env.LOGSTASH_OUTPUT_DORIS}.zip')
if status != 0:
    raise Exception('Failed to install logstash extension offline')
