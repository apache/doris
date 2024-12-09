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
import env

case_list = [
    'columns',
    'csv_basic',
    'csv_column_separator',
    # 'csv_enclose_escape', # not supported
    'csv_enclose',
    'group_commit_async',
    'group_commit_sync',
    'json_basic',
    'json_fuzzy_parse',
    'json_mapping',
    'json_nested',
    'json_num_as_string',
    'json_paths',
    'json_root',
    'max_filter_ratio',
    'strict_mode',
    'timezone',
]

def execute(cmd: str):
    status = os.system(cmd)
    if status != 0:
        raise Exception(f'Failed to execute command: {cmd}')

execute('python3 build.py')
execute('python3 install.py')
execute('python3 modify_config.py')
execute('python3 start.py')
for case in case_list:
    execute(f'python3 run.py --case {case}')
