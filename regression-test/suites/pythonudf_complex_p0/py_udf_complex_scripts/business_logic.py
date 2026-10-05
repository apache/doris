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
Complex Business Logic using pandas and numpy

Dependencies:
- pandas
- numpy

These are calculations that benefit from pandas/numpy's optimized operations
and would be complex to implement in pure SQL or Doris native functions.
"""

import json

# Import pandas and numpy
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


# ==================== Time Series Analysis ====================

def calculate_moving_average(values_json, window_size=7):
    """
    Calculate moving average for time series data

    Args:
        values_json: JSON array of numeric values
        window_size: Window size for moving average

    Returns:
        JSON array of moving averages (None for first window_size-1 elements)
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed. Run: pip install pandas'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        series = pd.Series(values)
        ma = series.rolling(window=int(window_size), min_periods=1).mean()
        return json.dumps([round(v, 4) if not pd.isna(v) else None for v in ma.tolist()])

    except Exception as e:
        return json.dumps({'error': str(e)})


def calculate_ewma(values_json, span=7):
    """
    Calculate Exponentially Weighted Moving Average

    Args:
        values_json: JSON array of numeric values
        span: Span for EWMA calculation

    Returns:
        JSON array of EWMA values
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        series = pd.Series(values)
        ewma = series.ewm(span=int(span)).mean()
        return json.dumps([round(v, 4) if not pd.isna(v) else None for v in ewma.tolist()])

    except Exception as e:
        return json.dumps({'error': str(e)})


def detect_anomalies_zscore(values_json, threshold=3.0):
    """
    Detect anomalies using Z-score method

    Args:
        values_json: JSON array of numeric values
        threshold: Z-score threshold (default 3.0)

    Returns:
        JSON array of booleans (True = anomaly)
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        arr = np.array(values, dtype=float)

        mean = np.nanmean(arr)
        std = np.nanstd(arr)

        if std == 0:
            return json.dumps([False] * len(arr))

        z_scores = np.abs((arr - mean) / std)
        anomalies = z_scores > float(threshold)

        return json.dumps(anomalies.tolist())

    except Exception as e:
        return json.dumps({'error': str(e)})


def calculate_trend(values_json):
    """
    Calculate linear trend (slope) of time series

    Args:
        values_json: JSON array of numeric values

    Returns:
        JSON with slope, intercept, r_squared
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        y = np.array(values, dtype=float)
        x = np.arange(len(y))

        # Remove NaN values
        mask = ~np.isnan(y)
        x_clean = x[mask]
        y_clean = y[mask]

        if len(x_clean) < 2:
            return json.dumps({'error': 'Need at least 2 non-null values'})

        # Linear regression
        coeffs = np.polyfit(x_clean, y_clean, 1)
        slope, intercept = coeffs

        # R-squared
        y_pred = slope * x_clean + intercept
        ss_res = np.sum((y_clean - y_pred) ** 2)
        ss_tot = np.sum((y_clean - np.mean(y_clean)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0

        return json.dumps({
            'slope': round(slope, 6),
            'intercept': round(intercept, 6),
            'r_squared': round(r_squared, 6),
            'trend': 'increasing' if slope > 0 else 'decreasing' if slope < 0 else 'flat'
        })

    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Statistical Analysis ====================

def calculate_percentiles(values_json, percentiles_json='[25, 50, 75]'):
    """
    Calculate multiple percentiles at once

    Args:
        values_json: JSON array of numeric values
        percentiles_json: JSON array of percentiles to calculate (0-100)

    Returns:
        JSON object mapping percentile to value
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        percentiles = json.loads(percentiles_json) if percentiles_json else [25, 50, 75]

        arr = np.array(values, dtype=float)
        arr = arr[~np.isnan(arr)]

        if len(arr) == 0:
            return json.dumps({})

        result = {}
        for p in percentiles:
            result[f'p{p}'] = round(float(np.percentile(arr, p)), 4)

        return json.dumps(result)

    except Exception as e:
        return json.dumps({'error': str(e)})


def calculate_statistics(values_json):
    """
    Calculate comprehensive statistics for a dataset

    Returns:
        JSON with count, mean, std, min, max, median, skew, kurtosis
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        series = pd.Series(values, dtype=float)

        stats = {
            'count': int(series.count()),
            'mean': round(float(series.mean()), 4) if not pd.isna(series.mean()) else None,
            'std': round(float(series.std()), 4) if not pd.isna(series.std()) else None,
            'min': round(float(series.min()), 4) if not pd.isna(series.min()) else None,
            'max': round(float(series.max()), 4) if not pd.isna(series.max()) else None,
            'median': round(float(series.median()), 4) if not pd.isna(series.median()) else None,
            'p25': round(float(series.quantile(0.25)), 4) if not pd.isna(series.quantile(0.25)) else None,
            'p75': round(float(series.quantile(0.75)), 4) if not pd.isna(series.quantile(0.75)) else None,
        }

        # Skewness and kurtosis (need at least 3 values)
        if len(series.dropna()) >= 3:
            stats['skew'] = round(float(series.skew()), 4)
            stats['kurtosis'] = round(float(series.kurtosis()), 4)

        return json.dumps(stats)

    except Exception as e:
        return json.dumps({'error': str(e)})


def calculate_correlation(values1_json, values2_json):
    """
    Calculate Pearson correlation between two series

    Returns:
        Correlation coefficient (-1 to 1), or None on error
    """
    if not PANDAS_AVAILABLE:
        return None

    if values1_json is None or values2_json is None:
        return None

    try:
        values1 = json.loads(values1_json)
        values2 = json.loads(values2_json)

        if len(values1) != len(values2):
            return None

        arr1 = np.array(values1, dtype=float)
        arr2 = np.array(values2, dtype=float)

        # Remove pairs where either is NaN
        mask = ~(np.isnan(arr1) | np.isnan(arr2))
        arr1_clean = arr1[mask]
        arr2_clean = arr2[mask]

        if len(arr1_clean) < 2:
            return None

        corr = np.corrcoef(arr1_clean, arr2_clean)[0, 1]
        return round(float(corr), 6) if not np.isnan(corr) else None

    except Exception:
        return None


# ==================== Customer Analytics ====================

def calculate_rfm_score(recency_days, frequency, monetary):
    """
    Calculate RFM (Recency, Frequency, Monetary) score

    Args:
        recency_days: Days since last purchase (lower is better)
        frequency: Number of purchases (higher is better)
        monetary: Total spend (higher is better)

    Returns:
        JSON with R, F, M scores (1-5) and total RFM score
    """
    if recency_days is None or frequency is None or monetary is None:
        return None

    try:
        # Simple scoring logic (in practice, would use quintiles from full dataset)
        # R score (lower recency = higher score)
        if recency_days <= 7:
            r_score = 5
        elif recency_days <= 30:
            r_score = 4
        elif recency_days <= 90:
            r_score = 3
        elif recency_days <= 180:
            r_score = 2
        else:
            r_score = 1

        # F score
        if frequency >= 20:
            f_score = 5
        elif frequency >= 10:
            f_score = 4
        elif frequency >= 5:
            f_score = 3
        elif frequency >= 2:
            f_score = 2
        else:
            f_score = 1

        # M score
        if monetary >= 10000:
            m_score = 5
        elif monetary >= 5000:
            m_score = 4
        elif monetary >= 1000:
            m_score = 3
        elif monetary >= 100:
            m_score = 2
        else:
            m_score = 1

        rfm_score = r_score * 100 + f_score * 10 + m_score

        return json.dumps({
            'r_score': r_score,
            'f_score': f_score,
            'm_score': m_score,
            'rfm_score': rfm_score,
            'segment': _get_rfm_segment(r_score, f_score, m_score)
        })

    except Exception as e:
        return json.dumps({'error': str(e)})


def _get_rfm_segment(r, f, m):
    """Determine customer segment based on RFM scores"""
    avg = (r + f + m) / 3

    if r >= 4 and f >= 4:
        return 'Champions'
    elif r >= 4 and f >= 2:
        return 'Loyal Customers'
    elif r >= 3 and f >= 3:
        return 'Potential Loyalists'
    elif r >= 4 and f == 1:
        return 'New Customers'
    elif r <= 2 and f >= 4:
        return 'At Risk'
    elif r <= 2 and f <= 2:
        return 'Lost'
    elif avg >= 3:
        return 'Promising'
    else:
        return 'Need Attention'


def calculate_ltv(avg_order_value, purchase_frequency, customer_lifespan_years, profit_margin=0.2):
    """
    Calculate Customer Lifetime Value (LTV)

    Args:
        avg_order_value: Average order value
        purchase_frequency: Purchases per year
        customer_lifespan_years: Expected customer lifespan in years
        profit_margin: Profit margin (default 20%)

    Returns:
        Lifetime value
    """
    if avg_order_value is None or purchase_frequency is None or customer_lifespan_years is None:
        return None

    try:
        ltv = float(avg_order_value) * float(purchase_frequency) * float(customer_lifespan_years) * float(profit_margin)
        return round(ltv, 2)
    except Exception:
        return None


def calculate_churn_probability(days_since_last_activity, avg_days_between_activities, total_activities):
    """
    Calculate churn probability based on activity patterns

    Returns:
        Churn probability (0-1)
    """
    if days_since_last_activity is None or avg_days_between_activities is None:
        return None

    try:
        days_since = float(days_since_last_activity)
        avg_days = float(avg_days_between_activities)
        total = int(total_activities) if total_activities else 1

        if avg_days <= 0:
            avg_days = 30  # Default

        # Simple exponential decay model
        ratio = days_since / avg_days
        base_prob = 1 - np.exp(-ratio / 2)

        # Adjust for total activities (more active users are less likely to churn)
        loyalty_factor = 1 - min(total / 100, 0.5)  # Max 50% reduction

        churn_prob = base_prob * loyalty_factor
        return round(min(max(churn_prob, 0), 1), 4)

    except Exception:
        return None


# ==================== Financial Calculations ====================

def calculate_npv(cash_flows_json, discount_rate):
    """
    Calculate Net Present Value

    Args:
        cash_flows_json: JSON array of cash flows (first is initial investment, usually negative)
        discount_rate: Discount rate as decimal (e.g., 0.1 for 10%)

    Returns:
        NPV value
    """
    if not PANDAS_AVAILABLE:
        return None

    if cash_flows_json is None or discount_rate is None:
        return None

    try:
        cash_flows = json.loads(cash_flows_json)
        rate = float(discount_rate)

        npv = sum(cf / ((1 + rate) ** i) for i, cf in enumerate(cash_flows))
        return round(npv, 2)

    except Exception:
        return None


def calculate_irr(cash_flows_json, max_iterations=1000, tolerance=1e-6):
    """
    Calculate Internal Rate of Return using Newton-Raphson method

    Args:
        cash_flows_json: JSON array of cash flows

    Returns:
        IRR as decimal (e.g., 0.15 for 15%)
    """
    if not PANDAS_AVAILABLE:
        return None

    if cash_flows_json is None:
        return None

    try:
        cash_flows = np.array(json.loads(cash_flows_json), dtype=float)

        # Initial guess
        irr = 0.1

        for _ in range(max_iterations):
            # NPV and its derivative
            npv = sum(cf / ((1 + irr) ** i) for i, cf in enumerate(cash_flows))
            npv_derivative = sum(-i * cf / ((1 + irr) ** (i + 1)) for i, cf in enumerate(cash_flows))

            if abs(npv_derivative) < tolerance:
                break

            irr_new = irr - npv / npv_derivative

            if abs(irr_new - irr) < tolerance:
                return round(irr_new, 6)

            irr = irr_new

        return round(irr, 6)

    except Exception:
        return None


def calculate_sharpe_ratio(returns_json, risk_free_rate=0.02):
    """
    Calculate Sharpe Ratio for investment returns

    Args:
        returns_json: JSON array of periodic returns (as decimals)
        risk_free_rate: Risk-free rate (default 2%)

    Returns:
        Sharpe ratio
    """
    if not PANDAS_AVAILABLE:
        return None

    if returns_json is None:
        return None

    try:
        returns = np.array(json.loads(returns_json), dtype=float)
        returns = returns[~np.isnan(returns)]

        if len(returns) < 2:
            return None

        excess_returns = returns - float(risk_free_rate) / len(returns)
        mean_excess = np.mean(excess_returns)
        std_excess = np.std(excess_returns, ddof=1)

        if std_excess == 0:
            return None

        sharpe = mean_excess / std_excess * np.sqrt(len(returns))
        return round(sharpe, 4)

    except Exception:
        return None


# ==================== Data Transformation ====================

def normalize_values(values_json, method='minmax'):
    """
    Normalize values using specified method

    Args:
        values_json: JSON array of values
        method: 'minmax' (0-1), 'zscore' (mean=0, std=1), 'robust' (median-based)

    Returns:
        JSON array of normalized values
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = np.array(json.loads(values_json), dtype=float)

        if method == 'zscore':
            mean = np.nanmean(values)
            std = np.nanstd(values)
            if std == 0:
                normalized = np.zeros_like(values)
            else:
                normalized = (values - mean) / std

        elif method == 'robust':
            median = np.nanmedian(values)
            q1 = np.nanpercentile(values, 25)
            q3 = np.nanpercentile(values, 75)
            iqr = q3 - q1
            if iqr == 0:
                normalized = np.zeros_like(values)
            else:
                normalized = (values - median) / iqr

        else:  # minmax
            min_val = np.nanmin(values)
            max_val = np.nanmax(values)
            if max_val == min_val:
                normalized = np.zeros_like(values)
            else:
                normalized = (values - min_val) / (max_val - min_val)

        result = [round(v, 6) if not np.isnan(v) else None for v in normalized]
        return json.dumps(result)

    except Exception as e:
        return json.dumps({'error': str(e)})


def bin_values(values_json, num_bins=5, labels_json=None):
    """
    Bin continuous values into discrete categories

    Args:
        values_json: JSON array of values
        num_bins: Number of bins
        labels_json: Optional JSON array of labels for bins

    Returns:
        JSON array of bin assignments
    """
    if not PANDAS_AVAILABLE:
        return json.dumps({'error': 'pandas package not installed'})

    if values_json is None:
        return None

    try:
        values = json.loads(values_json)
        series = pd.Series(values)

        labels = json.loads(labels_json) if labels_json else None

        if labels and len(labels) != int(num_bins):
            labels = None

        binned = pd.cut(series, bins=int(num_bins), labels=labels)

        result = [str(v) if pd.notna(v) else None for v in binned]
        return json.dumps(result)

    except Exception as e:
        return json.dumps({'error': str(e)})
