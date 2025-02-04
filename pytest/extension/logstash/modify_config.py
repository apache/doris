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

logstash_config = f'{env.LOGSTASH_HOME}/config/logstash.yml'
logstash_config_bak = f'{logstash_config}.bak'

if not os.path.exists(logstash_config_bak):
    status = os.system(f'cp {logstash_config} {logstash_config_bak}')
    if status != 0:
        raise Exception(f'Failed to backup logstash config file: {logstash_config}')

status = os.system(f'cp {logstash_config_bak} {logstash_config}')
if status != 0:
    raise Exception(f'Failed to restore logstash config file: {logstash_config}')

with open(logstash_config, 'a') as f:
    f.write('config.support_escapes: true\n')
