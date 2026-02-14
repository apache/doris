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

"""Edge cases UDTF implementations"""

import math


def handle_null_int(value):
    '''Handle NULL integer values'''
    if value is None:
        yield (None, True, -1)  # NULL indicator
    else:
        yield (value, False, value * 2)


def handle_null_string(value):
    '''Distinguish NULL from empty string'''
    if value is None:
        yield ('NULL', -1)
    elif value == '':
        yield ('EMPTY', 0)
    else:
        yield ('NORMAL', len(value))


def handle_empty_array(arr):
    '''Handle NULL vs empty array'''
    if arr is None:
        yield ('NULL', -1)
    elif len(arr) == 0:
        yield ('EMPTY', 0)
    else:
        yield ('NORMAL', len(arr))


def handle_null_struct(person):
    '''Handle NULL fields in STRUCT'''
    if person is None:
        yield (False, False, 'struct_is_null')
    else:
        name = person.get('name')
        age = person.get('age')
        has_name = name is not None
        has_age = age is not None
        
        if has_name and has_age:
            summary = f"{name}_{age}"
        elif has_name:
            summary = f"{name}_no_age"
        elif has_age:
            summary = f"no_name_{age}"
        else:
            summary = "all_fields_null"
        
        yield (has_name, has_age, summary)


def process_empty_table(value):
    '''This should never be called for empty table'''
    if value is not None:
        yield (value * 2,)


def process_single_row(value):
    '''Process single row input'''
    if value is not None:
        for i in range(3):
            yield (value, value + i)


def process_long_string(text):
    '''Process very long string'''
    if text is not None:
        length = len(text)
        first_10 = text[:10] if length >= 10 else text
        last_10 = text[-10:] if length >= 10 else text
        yield (length, first_10, last_10)


def process_large_array(arr):
    '''Process large array - compute statistics instead of exploding'''
    if arr is not None and len(arr) > 0:
        total = len(arr)
        total_sum = sum(x for x in arr if x is not None)
        first = arr[0] if len(arr) > 0 else None
        last = arr[-1] if len(arr) > 0 else None
        yield (total, total_sum, first, last)


def output_explosion(n):
    '''Generate many outputs from single input (controlled explosion)'''
    if n is not None and 0 < n <= 100:  # Safety limit
        for i in range(n):
            yield (i,)


def process_special_numbers(value):
    '''Categorize special numeric values'''
    INT_MIN = -2147483648
    INT_MAX = 2147483647
    
    if value is None:
        yield (None, 'NULL', False)
    elif value == 0:
        yield (value, 'ZERO', False)
    elif value == INT_MIN or value == INT_MAX:
        category = 'POSITIVE' if value > 0 else 'NEGATIVE'
        yield (value, category, True)  # is_boundary = True
    elif value > 0:
        yield (value, 'POSITIVE', False)
    else:
        yield (value, 'NEGATIVE', False)


def process_special_doubles(value):
    '''Classify special double values'''
    if value is None:
        yield (None, 'NULL')
    elif math.isnan(value):
        yield (value, 'NAN')
    elif math.isinf(value):
        if value > 0:
            yield (value, 'POSITIVE_INF')
        else:
            yield (value, 'NEGATIVE_INF')
    elif value == 0.0:
        yield (value, 'ZERO')
    elif abs(value) < 1e-10:
        yield (value, 'VERY_SMALL')
    elif abs(value) > 1e10:
        yield (value, 'VERY_LARGE')
    else:
        yield (value, 'NORMAL')


def process_special_strings(text):
    '''Process strings with special characters'''
    if text is None:
        yield (0, False, 'NULL')
    elif text == '':
        yield (0, False, 'EMPTY')
    else:
        length = len(text)
        has_special = any(ord(c) > 127 for c in text)
        
        if has_special:
            desc = 'HAS_UNICODE'
        elif any(c in text for c in ['\n', '\t', '\r']):
            desc = 'HAS_WHITESPACE'
        elif any(c in text for c in ['!', '@', '#', '$', '%']):
            desc = 'HAS_SYMBOLS'
        else:
            desc = 'NORMAL'
        
        yield (length, has_special, desc)


def process_boundary_dates(dt):
    '''Process boundary date values'''
    if dt is None:
        yield (None, 0, False)
    else:
        year = dt.year
        # Check if it's a boundary date
        is_boundary = year in [1970, 9999] or (year == 1970 and dt.month == 1 and dt.day == 1)
        yield (dt, year, is_boundary)
