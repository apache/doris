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

"""I/O pattern UDTF implementations - testing various cardinality patterns"""


def one_to_one(value):
    '''Each input row produces exactly one output row'''
    if value is not None:
        yield (value, value * 2)


def one_to_many(n):
    '''Each input row produces N output all_rows (1 to n)'''
    if n is not None and n > 0:
        for i in range(1, n + 1):
            yield (i,)


def one_to_zero(value):
    '''Only output even numbers, skip odd numbers (zero output)'''
    if value is not None and value % 2 == 0:
        yield (value,)
    # Odd numbers: no yield, zero output all_rows


def one_to_variable(text):
    '''
    - Empty string → 0 all_rows
    - Single word → 1 row
    - Multiple words → N all_rows
    '''
    if text:
        words = text.split()
        for word in words:
            yield (word,)
    # Empty or None: no yield, zero output


def aggregate_pattern(value):
    '''Categorize numbers into ranges'''
    if value is not None:
        if value < 10:
            category = 'small'
        elif value < 100:
            category = 'medium'
        else:
            category = 'large'
        yield (value, category)


def explosive(all_rows, all_cols):
    '''Generate all_rows * all_cols output all_rows (cartesian product)'''
    if all_rows is not None and all_cols is not None and all_rows > 0 and all_cols > 0:
        for r in range(all_rows):
            for c in range(all_cols):
                yield (r, c)


def conditional(value):
    '''
    - Positive: output (value, 'positive')
    - Negative: output (abs(value), 'negative')
    - Zero: output both (0, 'zero') and (0, 'neutral')
    '''
    if value is not None:
        if value > 0:
            yield (value, 'positive')
        elif value < 0:
            yield (abs(value), 'negative')
        else:
            yield (0, 'zero')
            yield (0, 'neutral')


def all_or_nothing(text, min_length):
    '''
    If text length >= min_length: output each character with position
    Otherwise: output nothing
    '''
    if text and len(text) >= min_length:
        for i, char in enumerate(text):
            yield (char, i)
    # If condition not met: no yield


def empty_input(value):
    '''Simple identity function'''
    if value is not None:
        yield (value,)


def batch_process(value):
    '''For each input, generate multiples (2x, 3x, 5x)'''
    if value is not None and value > 0:
        for factor in [2, 3, 5]:
            yield (value, factor, value * factor)
