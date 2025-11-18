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

"""Basic UDTF implementations - copied from inline tests"""

import json


def split_string_udtf(input_str):
    '''Split comma-separated string into rows'''
    if input_str:
        parts = input_str.split(',')
        for part in parts:
            yield (part.strip(),)


def generate_series_udtf(start, end):
    '''Generate integer series from start to end'''
    if start is not None and end is not None:
        for i in range(start, end + 1):
            yield (i,)


def running_sum_udtf(value):
    '''Return value with itself as cumulative sum (stateless)'''
    # Note: Function-based UDTF cannot maintain state
    # This is simplified to return (value, value)
    if value is not None:
        yield (value, value)


def explode_json_udtf(json_str):
    '''Explode JSON ARRAY into rows'''
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    yield (str(item),)
        except:
            pass  # Skip invalid JSON


def top_n_udtf(value, n):
    '''Return single value with rank 1 (stateless)'''
    # Without state, each row is independent
    if value is not None and n is not None and n > 0:
        yield (value, 1)


def duplicate_udtf(text, n):
    '''Duplicate input text N times'''
    if text and n:
        for i in range(n):
            yield (text, i + 1)


def filter_positive_udtf(value):
    '''Only output positive values'''
    if value is not None and value > 0:
        yield (value,)
    # If value <= 0, don't yield (skip this row)


def cartesian_udtf(list1, list2):
    '''Generate cartesian product of two comma-separated lists'''
    if list1 and list2:
        items1 = [x.strip() for x in list1.split(',')]
        items2 = [y.strip() for y in list2.split(',')]
        
        for x in items1:
            for y in items2:
                yield (x, y)


def filter_negative_udtf(value):
    '''Only output negative values (filter all positive numbers)'''
    if value is not None and value < 0:
        yield (value,)
    # For positive numbers, don't yield anything

