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

"""Exception handling UDTF implementations"""

import math


def safe_divide(a, b):
    '''Safe division with error handling'''
    try:
        if b == 0:
            yield (a, b, None, 'division_by_zero')
        else:
            result = a / b
            yield (a, b, result, 'success')
    except Exception as e:
        yield (a, b, None, f'error_{type(e).__name__}')


def check_overflow(value):
    '''Check for potential overflow in operations'''
    if value is None:
        yield (None, None, 'null_input')
    else:
        # BIGINT range: -2^63 to 2^63-1
        MAX_BIGINT = 9223372036854775807
        MIN_BIGINT = -9223372036854775808
        
        doubled = value * 2
        
        # Check if doubled value is within safe range
        if doubled > MAX_BIGINT or doubled < MIN_BIGINT:
            yield (value, None, 'would_overflow')
        else:
            yield (value, doubled, 'safe')


def parse_number(text):
    '''Parse string to number with error handling'''
    if text is None:
        yield (None, None, False)
    else:
        try:
            num = float(text)
            yield (text, num, True)
        except ValueError:
            yield (text, None, False)


def check_type(value):
    '''Check and report value type'''
    type_name = type(value).__name__
    
    if value is None:
        yield (None, 'NoneType', 0)
    elif isinstance(value, str):
        yield (value, type_name, len(value))
    else:
        # Unexpected type - convert to string
        yield (str(value), type_name, len(str(value)))


def safe_array_access(arr, position):
    '''Safe array element access'''
    if arr is None:
        yield (0, position, None, 'null_array')
    elif len(arr) == 0:
        yield (0, position, None, 'empty_array')
    elif position < 0 or position >= len(arr):
        yield (len(arr), position, None, 'out_of_bounds')
    else:
        yield (len(arr), position, arr[position], 'success')


def compute_stats(arr):
    '''Compute statistics with empty array handling'''
    if arr is None:
        yield (0, 0, 0.0, 'null_array')
    elif len(arr) == 0:
        yield (0, 0, 0.0, 'empty_array')
    else:
        count = len(arr)
        total = sum(x for x in arr if x is not None)
        avg = total / count if count > 0 else 0.0
        yield (count, total, avg, 'computed')


def access_struct_fields(person):
    '''Safe STRUCT field access'''
    if person is None:
        yield (False, False, None, None)
    else:
        # Use .get() to safely access dictionary keys
        name = person.get('name')
        age = person.get('age')
        
        has_name = name is not None
        has_age = age is not None
        
        yield (has_name, has_age, name, age)


def slice_string(text, start, end):
    '''Safe string slicing'''
    if text is None:
        yield (None, start, end, None, 'null_string')
    elif start is None or end is None:
        yield (text, start, end, None, 'null_index')
    else:
        length = len(text)
        
        # Clamp indices to valid range
        safe_start = max(0, min(start, length))
        safe_end = max(0, min(end, length))
        
        if safe_start >= safe_end:
            yield (text, start, end, '', 'empty_slice')
        else:
            result = text[safe_start:safe_end]
            yield (text, start, end, result, 'success')


def check_text_encoding(text):
    '''Check string encoding properties'''
    if text is None:
        yield (None, 0, 0, False)
    else:
        byte_len = len(text.encode('utf-8'))
        char_len = len(text)
        has_unicode = byte_len > char_len
        
        yield (text, byte_len, char_len, has_unicode)


def process_conditional(value):
    '''Process value based on multiple conditions'''
    if value is None:
        yield (None, 'null', 0)
    elif value < 0:
        # For negative: take absolute value
        yield (value, 'negative', abs(value))
    elif value == 0:
        # Zero case: return 1
        yield (value, 'zero', 1)
    elif value > 0 and value <= 100:
        # Small positive: double it
        yield (value, 'small_positive', value * 2)
    else:
        # Large positive: return as-is
        yield (value, 'large_positive', value)


def conditional_yield(value):
    '''Only yield for even positive numbers'''
    if value is not None and value > 0 and value % 2 == 0:
        yield (value,)
    # For other cases, yield nothing (filter out)


def classify_number_range(value):
    '''Classify number by magnitude'''
    if value is None:
        yield (None, 'null', True)
    elif math.isnan(value):
        yield (value, 'nan', False)
    elif math.isinf(value):
        yield (value, 'infinity', False)
    elif value == 0.0:
        yield (value, 'zero', True)
    elif abs(value) < 1e-100:
        yield (value, 'extremely_small', True)
    elif abs(value) > 1e100:
        yield (value, 'extremely_large', True)
    elif abs(value) < 1.0:
        yield (value, 'small', True)
    else:
        yield (value, 'normal', True)


def validate_date(dt):
    '''Validate and classify dates'''
    if dt is None:
        yield (None, 0, False, 'null_date')
    else:
        year = dt.year
        
        # Check if leap year
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        
        # Classify date
        if year < 1900:
            status = 'very_old'
        elif year > 2100:
            status = 'far_future'
        else:
            status = 'normal'
        
        yield (dt, year, is_leap, status)
