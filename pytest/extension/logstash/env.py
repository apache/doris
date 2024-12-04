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

import os

def add_env(key: str, value: str):
    os.environ[key] = value
    os.system(f'echo {key}: ${key}')

LOGSTASH_OUTPUT_DORIS = 'logstash-output-doris'
DB = 'logstash_db'
DB_CONFIG = {
    'host': 'localhost',
    'port': 9030,
    'user': 'admin',
    'password': 'admin',
    'database': 'mysql',
}

CONF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

LOGSTASH_DORIS_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../extension/logstash')
add_env('LOGSTASH_DORIS_HOME', LOGSTASH_DORIS_HOME)

LOGSTASH_HOME = '/home/liumx/logstash-8.16.0' # TODO: change me
add_env('LOGSTASH_HOME', LOGSTASH_HOME)
