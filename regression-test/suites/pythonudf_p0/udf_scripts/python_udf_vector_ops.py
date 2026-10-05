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

"""
Vector Python UDF operations using pandas.Series
"""

import pandas as pd
import numpy as np


def add_constant(a: pd.Series, constant: pd.Series) -> pd.Series:
    """Add a constant to series"""
    # constant is a series but we use the first value
    const_val = constant.iloc[0] if len(constant) > 0 else 0
    return a + const_val


def multiply_by_constant(a: pd.Series, constant: pd.Series) -> pd.Series:
    """Multiply series by a constant"""
    const_val = constant.iloc[0] if len(constant) > 0 else 1
    return a * const_val


def calculate_discount(price: pd.Series, discount_percent: pd.Series) -> pd.Series:
    """Calculate price after discount"""
    return price * (1 - discount_percent)


def string_length(s: pd.Series) -> pd.Series:
    """Calculate length of each string in series"""
    return s.str.len()


def to_uppercase(s: pd.Series) -> pd.Series:
    """Convert strings to uppercase"""
    return s.str.upper()


def vec_add_with_constant(a: pd.Series, b: pd.Series) -> pd.Series:
    """Add two series and add a constant"""
    return a + b + 100


def vec_multiply_and_round(a: pd.Series, b: pd.Series) -> pd.Series:
    """Multiply two series and round to 2 decimal places"""
    return (a * b).round(2)


def vec_string_concat_with_separator(s1: pd.Series, s2: pd.Series) -> pd.Series:
    """Concatenate two string series with a separator"""
    return s1 + ' | ' + s2


def vec_string_title_case(s: pd.Series) -> pd.Series:
    """Convert string series to title case"""
    return s.str.title()


def vec_conditional_value(a: pd.Series, b: pd.Series) -> pd.Series:
    """Return a if a > b, else return b"""
    return pd.Series(np.where(a > b, a, b))


def vec_percentage_calculation(part: pd.Series, total: pd.Series) -> pd.Series:
    """Calculate percentage: (part / total) * 100"""
    return (part / total * 100).round(2)


def vec_is_in_range(value: pd.Series, min_val: pd.Series, max_val: pd.Series) -> pd.Series:
    """Check if value is between min_val and max_val"""
    return (value >= min_val) & (value <= max_val)


def vec_safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Safe division, return 0 when denominator is 0 or None"""
    result = numerator / denominator
    # Replace inf and -inf with 0
    result = result.replace([np.inf, -np.inf], 0)
    # Fill NaN with 0
    return result.fillna(0)


def vec_exponential_decay(value: pd.Series, days: pd.Series) -> pd.Series:
    """Calculate exponential decay: value * exp(-days/30)"""
    return value * np.exp(-days / 30.0)


def vec_string_extract_first_word(s: pd.Series) -> pd.Series:
    """Extract the first word from a string"""
    return s.str.split().str[0]


def vec_normalize_to_range(value: pd.Series) -> pd.Series:
    """Normalize values to 0-1 range using min-max normalization"""
    min_val = value.min()
    max_val = value.max()
    if max_val == min_val:
        return pd.Series([0.5] * len(value))
    return (value - min_val) / (max_val - min_val)


def vec_moving_average(value: pd.Series) -> pd.Series:
    """Calculate 3-point moving average"""
    return value.rolling(window=3, min_periods=1).mean()


def vec_z_score(value: pd.Series) -> pd.Series:
    """Calculate z-score: (value - mean) / std"""
    mean = value.mean()
    std = value.std()
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(value))
    return (value - mean) / std


def vec_clip_values(value: pd.Series, min_val: pd.Series, max_val: pd.Series) -> pd.Series:
    """Clip values to be within min_val and max_val"""
    return value.clip(lower=min_val, upper=max_val)


def vec_boolean_and(a: pd.Series, b: pd.Series) -> pd.Series:
    """Logical AND operation on two boolean series"""
    return a & b


def vec_boolean_or(a: pd.Series, b: pd.Series) -> pd.Series:
    """Logical OR operation on two boolean series"""
    return a | b


def vec_string_contains(s: pd.Series, pattern: pd.Series) -> pd.Series:
    """Check if string contains pattern (case-insensitive)"""
    # For simplicity, use the first pattern value for all rows
    if len(pattern) > 0 and not pd.isna(pattern.iloc[0]):
        pattern_str = str(pattern.iloc[0])
        return s.str.contains(pattern_str, case=False, na=False)
    return pd.Series([False] * len(s))


def vec_abs_difference(a: pd.Series, b: pd.Series) -> pd.Series:
    """Calculate absolute difference between two series"""
    return (a - b).abs()


def vec_power(base: pd.Series, exponent: pd.Series) -> pd.Series:
    """Calculate base raised to the power of exponent"""
    return base ** exponent


def vec_log_transform(value: pd.Series) -> pd.Series:
    """Calculate natural logarithm, return 0 for non-positive values"""
    result = np.log(value)
    return result.replace([np.inf, -np.inf], 0).fillna(0)
