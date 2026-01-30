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

"""Data type handling UDTF implementations - copied from inline tests"""

import math
import json
from decimal import Decimal


def process_tinyint(v):
    '''Process TINYINT: test small integer range'''
    if v is not None:
        yield (v, v * 2)


def process_smallint(v):
    '''Process SMALLINT: test medium integer range'''
    if v is not None:
        yield (v, v * v)


def process_bigint(v):
    '''Process BIGINT: test large integer range'''
    if v is not None:
        yield (v, v + 1)


def process_float(v):
    '''Process FLOAT: test floating point numbers'''
    if v is not None:
        yield (v, v / 2.0)


def process_double(v):
    '''Process DOUBLE: test high precision floating point'''
    if v is not None and v >= 0:
        yield (v, math.sqrt(v))


def process_boolean(v):
    '''Process BOOLEAN: test true/false values'''
    if v is not None:
        yield (v, not v, 'TRUE' if v else 'FALSE')


def process_string(v):
    '''Process STRING: test text manipulation'''
    if v is not None:
        yield (v, len(v), v.upper(), v.lower())


def process_date(v):
    '''Process DATE: extract date components'''
    if v is not None:
        # v is a datetime.date object
        yield (v, v.year, v.month, v.day)


def process_datetime(v):
    '''Process DATETIME: extract time components'''
    if v is not None:
        # v is a datetime.datetime object
        yield (v, v.hour, v.minute)


def process_array_int(arr):
    '''Process ARRAY<INT>: explode array and process each element'''
    if arr is not None:
        for i, elem in enumerate(arr):
            if elem is not None:
                yield (i, elem, elem * 2)


def process_array_string(arr):
    '''Process ARRAY<STRING>: explode and get string lengths'''
    if arr is not None:
        for elem in arr:
            if elem is not None:
                yield (elem, len(elem))


def process_struct(person):
    '''Process STRUCT: access struct fields'''
    if person is not None:
        name = person['name'] if 'name' in person else None
        age = person['age'] if 'age' in person else None
        
        if name is not None and age is not None:
            category = 'child' if age < 18 else 'adult'
            yield (name, age, category)


def process_multi_types(num, text):
    '''Process multiple input types'''
    if num is not None and text is not None:
        yield (num, text, f"{text}_{num}")


def process_decimal(v):
    '''Process DECIMAL: high precision arithmetic'''
    if v is not None:
        doubled = v * 2
        yield (v, doubled)


def process_map_string(map_str):
    '''Process map-like string (key1:val1,key2:val2)'''
    if map_str:
        pairs = map_str.split(',')
        for pair in pairs:
            if ':' in pair:
                k, val = pair.split(':', 1)
                try:
                    yield (k.strip(), int(val.strip()))
                except ValueError:
                    pass


def process_nested_array(nested_str):
    '''Process nested array string ([[1,2],[3,4]])'''
    if nested_str:
        # Remove brackets and split by ],[
        nested_str = nested_str.strip('[]')
        groups = nested_str.split('],[')
        
        for group_idx, group in enumerate(groups):
            elements = group.strip('[]').split(',')
            for elem in elements:
                try:
                    yield (group_idx, int(elem.strip()))
                except ValueError:
                    pass


def process_array_structs(data):
    '''Process array of structs (name:age:score|name:age:score)'''
    if data:
        items = data.split('|')
        for item in items:
            parts = item.split(':')
            if len(parts) == 3:
                try:
                    yield (parts[0], int(parts[1]), int(parts[2]))
                except ValueError:
                    pass


def process_struct_array(data):
    '''Process struct with array (name:tag1,tag2,tag3)'''
    if data and ':' in data:
        name, tags = data.split(':', 1)
        tag_list = tags.split(',')
        yield (name, len(tag_list), ','.join(tag_list))


def extract_json_fields(json_str):
    '''Extract JSON fields'''
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                for k, v in data.items():
                    yield (k, str(v))
        except:
            pass


def process_complex_struct(data):
    '''Process complex struct (id:name:city:zip)'''
    if data:
        parts = data.split(':')
        if len(parts) == 4:
            try:
                yield (int(parts[0]), parts[1], parts[2], parts[3])
            except ValueError:
                pass
