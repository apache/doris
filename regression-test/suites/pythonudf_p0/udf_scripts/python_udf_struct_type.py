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


def struct_to_csv_impl(person, point):
    def safe_str(x):
        return 'NULL' if x is None else str(x)
    
    def format_array(arr):
        if arr is None:
            return 'NULL'
        return '[' + ','.join(safe_str(item) for item in arr) + ']'
    
    def format_struct_dict(s, field_names):
        if s is None:
            return 'NULL'
        parts = []
        for field in field_names:
            val = s.get(field)
            parts.append(safe_str(val))
        return '(' + ','.join(parts) + ')'
    
    person_str = format_struct_dict(person, ['name', 'age', 'salary'])
    
    if point is None:
        point_str = 'NULL'
    else:
        x_val = safe_str(point.get('x'))
        y_val = safe_str(point.get('y'))
        tags_val = format_array(point.get('tags'))
        point_str = f"({x_val},{y_val},{tags_val})"
    
    return '|'.join([person_str, point_str])