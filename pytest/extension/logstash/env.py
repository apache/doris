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
from dotenv import load_dotenv

load_dotenv()

LOGSTASH_OUTPUT_DORIS = 'logstash-output-doris-1.0.1'

DB = 'logstash_db'
DB_CONFIG = {
    'host': os.environ.get('DORIS_HOST'),
    'port': int(os.environ.get('DORIS_MYSQL_PORT')),
    'user': os.environ.get('DORIS_USER'),
    'password': os.environ.get('DORIS_PASSWORD'),
    'database': 'mysql',
}

TEST_CONF_DIR = os.environ.get('TEST_CONF_DIR')
TEST_DDL_DIR = os.environ.get('TEST_DDL_DIR')
TEST_DATA_DIR = os.environ.get('TEST_DATA_DIR')

LOGSTASH_DORIS_DIR = os.environ.get('LOGSTASH_DORIS_DIR')
LOGSTASH_HOME = os.environ.get('LOGSTASH_HOME')

RUBY_HOME = os.environ.get('RUBY_HOME')
