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
Complex UDAF (User Defined Aggregate Functions) using pandas and numpy

Dependencies:
- pandas
- numpy

These UDAFs perform advanced aggregations that leverage
pandas/numpy's optimized operations.
"""

import json

try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def _check_pandas():
    """Check if pandas is available, raise error if not"""
    if not PANDAS_AVAILABLE:
        raise ImportError('pandas package not installed. Run: pip install pandas numpy')


# ==================== Advanced Statistical Aggregations ====================

class WeightedAverageUDAF:
    """
    Calculate weighted average

    Usage: weighted_avg(value, weight)
    """
    def __init__(self):
        _check_pandas()
        self.values = []
        self.weights = []

    @property
    def aggregate_state(self):
        return {'values': self.values, 'weights': self.weights}

    def accumulate(self, value, weight):
        if value is not None and weight is not None:
            self.values.append(float(value))
            self.weights.append(float(weight))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state.get('values', []))
            self.weights.extend(other_state.get('weights', []))

    def finish(self):
        if not self.values or not self.weights:
            return None

        values = np.array(self.values)
        weights = np.array(self.weights)

        total_weight = np.sum(weights)
        if total_weight == 0:
            return None

        weighted_avg = np.sum(values * weights) / total_weight
        return round(float(weighted_avg), 6)


class PercentileUDAF:
    """
    Calculate specified percentile

    Usage: percentile_agg(value, percentile)
    Note: percentile should be 0-100
    """
    def __init__(self):
        _check_pandas()
        self.values = []
        self.percentile = 50  # Default to median

    @property
    def aggregate_state(self):
        return {'values': self.values, 'percentile': self.percentile}

    def accumulate(self, value, percentile=50):
        if value is not None:
            self.values.append(float(value))
        if percentile is not None:
            self.percentile = float(percentile)

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state.get('values', []))
            self.percentile = other_state.get('percentile', self.percentile)

    def finish(self):
        if not self.values:
            return None

        arr = np.array(self.values)
        result = np.percentile(arr, self.percentile)
        return round(float(result), 6)


class SkewnessUDAF:
    """
    Calculate skewness of distribution

    Positive skew: tail on right
    Negative skew: tail on left
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if len(self.values) < 3:
            return None

        series = pd.Series(self.values)
        skew = series.skew()

        if pd.isna(skew):
            return None

        return round(float(skew), 6)


class KurtosisUDAF:
    """
    Calculate kurtosis of distribution

    High kurtosis: heavy tails, sharp peak
    Low kurtosis: light tails, flat peak
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if len(self.values) < 4:
            return None

        series = pd.Series(self.values)
        kurt = series.kurtosis()

        if pd.isna(kurt):
            return None

        return round(float(kurt), 6)


class ModeUDAF:
    """
    Calculate mode (most frequent value)

    Returns the first mode if multiple exist
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(value)

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if not self.values:
            return None

        series = pd.Series(self.values)
        mode_result = series.mode()

        if len(mode_result) == 0:
            return None

        return mode_result.iloc[0]


class CorrelationUDAF:
    """
    Calculate Pearson correlation between two series

    Usage: correlation_agg(x, y)
    """
    def __init__(self):
        _check_pandas()
        self.x_values = []
        self.y_values = []

    @property
    def aggregate_state(self):
        return {'x': self.x_values, 'y': self.y_values}

    def accumulate(self, x, y):
        if x is not None and y is not None:
            self.x_values.append(float(x))
            self.y_values.append(float(y))

    def merge(self, other_state):
        if other_state:
            self.x_values.extend(other_state.get('x', []))
            self.y_values.extend(other_state.get('y', []))

    def finish(self):
        if len(self.x_values) < 2:
            return None

        x = np.array(self.x_values)
        y = np.array(self.y_values)

        corr = np.corrcoef(x, y)[0, 1]

        if np.isnan(corr):
            return None

        return round(float(corr), 6)


class CovarianceUDAF:
    """
    Calculate covariance between two series

    Usage: covariance_agg(x, y)
    """
    def __init__(self):
        _check_pandas()
        self.x_values = []
        self.y_values = []

    @property
    def aggregate_state(self):
        return {'x': self.x_values, 'y': self.y_values}

    def accumulate(self, x, y):
        if x is not None and y is not None:
            self.x_values.append(float(x))
            self.y_values.append(float(y))

    def merge(self, other_state):
        if other_state:
            self.x_values.extend(other_state.get('x', []))
            self.y_values.extend(other_state.get('y', []))

    def finish(self):
        if len(self.x_values) < 2:
            return None

        x = np.array(self.x_values)
        y = np.array(self.y_values)

        cov = np.cov(x, y)[0, 1]

        if np.isnan(cov):
            return None

        return round(float(cov), 6)


# ==================== Business Analytics UDAFs ====================

class RetentionRateUDAF:
    """
    Calculate retention rate

    Usage: retention_rate_agg(is_retained)
    where is_retained is 1/0 or true/false
    """
    def __init__(self):
        self.retained = 0
        self.total = 0

    @property
    def aggregate_state(self):
        return {'retained': self.retained, 'total': self.total}

    def accumulate(self, is_retained):
        if is_retained is not None:
            self.total += 1
            if is_retained:
                self.retained += 1

    def merge(self, other_state):
        if other_state:
            self.retained += other_state.get('retained', 0)
            self.total += other_state.get('total', 0)

    def finish(self):
        if self.total == 0:
            return None

        rate = self.retained / self.total
        return round(float(rate), 6)


class ConversionFunnelUDAF:
    """
    Calculate conversion rate through funnel stages

    Usage: funnel_conversion_agg(stage_reached)
    stage_reached: integer representing the furthest stage reached (1, 2, 3, ...)
    """
    def __init__(self):
        self.stage_counts = {}

    @property
    def aggregate_state(self):
        return self.stage_counts

    def accumulate(self, stage_reached):
        if stage_reached is not None:
            stage = int(stage_reached)
            # Count all stages up to and including the reached stage
            for s in range(1, stage + 1):
                self.stage_counts[s] = self.stage_counts.get(s, 0) + 1

    def merge(self, other_state):
        if other_state:
            for stage, count in other_state.items():
                self.stage_counts[stage] = self.stage_counts.get(stage, 0) + count

    def finish(self):
        if not self.stage_counts:
            return None

        # Return JSON with stage counts and conversion rates
        stages = sorted(self.stage_counts.keys())
        if not stages:
            return None

        first_stage_count = self.stage_counts.get(stages[0], 0)
        if first_stage_count == 0:
            return None

        result = {}
        for stage in stages:
            count = self.stage_counts[stage]
            rate = count / first_stage_count
            result[f'stage_{stage}'] = {
                'count': count,
                'rate': round(rate, 4)
            }

        return json.dumps(result)


class GiniCoefficientUDAF:
    """
    Calculate Gini coefficient (inequality measure)

    0 = perfect equality
    1 = perfect inequality
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None and float(value) >= 0:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if not self.values:
            return None

        arr = np.array(sorted(self.values))
        n = len(arr)
        total = np.sum(arr)

        if total == 0:
            return 0.0

        # Gini calculation
        cumsum = np.cumsum(arr)
        gini = (n + 1 - 2 * np.sum(cumsum) / total) / n

        return round(float(gini), 6)


class HerfindahlIndexUDAF:
    """
    Calculate Herfindahl-Hirschman Index (market concentration)

    Values: 0 to 10000
    < 1500: competitive
    1500-2500: moderate concentration
    > 2500: high concentration
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, market_share):
        if market_share is not None:
            self.values.append(float(market_share))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if not self.values:
            return None

        arr = np.array(self.values)
        total = np.sum(arr)

        if total == 0:
            return None

        # Normalize to percentages and calculate HHI
        shares = (arr / total) * 100
        hhi = np.sum(shares ** 2)

        return round(float(hhi), 2)


# ==================== Time Series UDAFs ====================

class TrendSlopeUDAF:
    """
    Calculate trend slope using linear regression

    Positive slope: upward trend
    Negative slope: downward trend
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if len(self.values) < 2:
            return None

        y = np.array(self.values)
        x = np.arange(len(y))

        # Linear regression
        coeffs = np.polyfit(x, y, 1)
        slope = coeffs[0]

        return round(float(slope), 6)


class SeasonalityStrengthUDAF:
    """
    Estimate seasonality strength from time series

    Returns: strength (0 to 1, higher = more seasonal)

    Note: This is a simplified estimation based on autocorrelation
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if len(self.values) < 10:
            return None

        series = pd.Series(self.values)

        # Calculate autocorrelation at different lags
        max_lag = min(len(series) // 2, 30)
        autocorrs = []

        for lag in range(1, max_lag + 1):
            autocorr = series.autocorr(lag=lag)
            if not pd.isna(autocorr):
                autocorrs.append(abs(autocorr))

        if not autocorrs:
            return None

        # Seasonality strength = max autocorrelation
        strength = max(autocorrs)
        return round(float(strength), 4)


class VolatilityUDAF:
    """
    Calculate volatility (standard deviation of returns)

    Common in financial analysis
    """
    def __init__(self):
        _check_pandas()
        self.values = []

    @property
    def aggregate_state(self):
        return self.values

    def accumulate(self, value):
        if value is not None:
            self.values.append(float(value))

    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)

    def finish(self):
        if len(self.values) < 2:
            return None

        arr = np.array(self.values)

        # Calculate returns (percentage change)
        returns = np.diff(arr) / arr[:-1]
        returns = returns[~np.isnan(returns) & ~np.isinf(returns)]

        if len(returns) < 2:
            return None

        volatility = np.std(returns, ddof=1)
        return round(float(volatility), 6)


# ==================== Text Aggregation UDAFs ====================

class MostCommonWordUDAF:
    """
    Find most common word across all text inputs
    """
    def __init__(self):
        self.word_counts = {}

    @property
    def aggregate_state(self):
        return self.word_counts

    def accumulate(self, text):
        if text is not None:
            words = str(text).lower().split()
            for word in words:
                # Remove punctuation
                word = ''.join(c for c in word if c.isalnum())
                if word:
                    self.word_counts[word] = self.word_counts.get(word, 0) + 1

    def merge(self, other_state):
        if other_state:
            for word, count in other_state.items():
                self.word_counts[word] = self.word_counts.get(word, 0) + count

    def finish(self):
        if not self.word_counts:
            return None

        most_common = max(self.word_counts.items(), key=lambda x: x[1])
        return most_common[0]


class TextConcatUDAF:
    """
    Concatenate text values with separator
    """
    def __init__(self):
        self.texts = []
        self.separator = ', '

    @property
    def aggregate_state(self):
        return {'texts': self.texts, 'separator': self.separator}

    def accumulate(self, text, separator=', '):
        if text is not None:
            self.texts.append(str(text))
        if separator is not None:
            self.separator = str(separator)

    def merge(self, other_state):
        if other_state:
            self.texts.extend(other_state.get('texts', []))
            self.separator = other_state.get('separator', self.separator)

    def finish(self):
        if not self.texts:
            return None

        return self.separator.join(self.texts)


class JsonArrayAggUDAF:
    """
    Aggregate values into a JSON array
    """
    def __init__(self):
        self.items = []

    @property
    def aggregate_state(self):
        return self.items

    def accumulate(self, value):
        if value is not None:
            self.items.append(value)

    def merge(self, other_state):
        if other_state:
            self.items.extend(other_state)

    def finish(self):
        if not self.items:
            return '[]'

        return json.dumps(self.items, ensure_ascii=False)


class JsonObjectAggUDAF:
    """
    Aggregate key-value pairs into a JSON object
    """
    def __init__(self):
        self.data = {}

    @property
    def aggregate_state(self):
        return self.data

    def accumulate(self, key, value):
        if key is not None:
            self.data[str(key)] = value

    def merge(self, other_state):
        if other_state:
            self.data.update(other_state)

    def finish(self):
        if not self.data:
            return '{}'

        return json.dumps(self.data, ensure_ascii=False)
