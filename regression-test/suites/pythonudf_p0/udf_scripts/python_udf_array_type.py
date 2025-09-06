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


def array_to_csv_impl(int_arr, str_arr, nested_arr):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(safe_str(item) for item in arr) + ']'
    
    def format_nested_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(format_array(inner) for inner in arr) + ']'
    
    parts = [
        format_array(int_arr),
        format_array(str_arr),
        format_nested_array(nested_arr)
    ]
    return '|'.join(parts)
