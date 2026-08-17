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


def map_to_csv_impl(map1, map2):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_map(m):
        if m is None:
            return 'NULL'
        # Doris passes MAP as Python dict
        items = [f"{safe_str(k)}:{safe_str(v)}" for k, v in m.items()]
        return '{' + ','.join(sorted(items)) + '}'
    
    return '|'.join([format_map(map1), format_map(map2)])