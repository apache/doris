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
Complex UDTF (User Defined Table Functions) using pandas

Dependencies:
- pandas
- numpy

These UDTFs expand single rows into multiple rows,
leveraging pandas for complex data transformations.
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
        raise ImportError('pandas package not installed. Run: pip install pandas')


# ==================== Time Series Expansion ====================

def expand_date_range(start_date, end_date, freq='D'):
    """
    Expand date range into individual rows

    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        freq: Frequency - 'D' (day), 'W' (week), 'M' (month), 'H' (hour)

    Yields:
        (date_string, day_of_week, is_weekend)
    """
    _check_pandas()

    if start_date is None or end_date is None:
        yield (None, None, None)
        return

    try:
        dates = pd.date_range(start=start_date, end=end_date, freq=freq)

        for dt in dates:
            date_str = dt.strftime('%Y-%m-%d')
            if freq == 'H':
                date_str = dt.strftime('%Y-%m-%d %H:%M:%S')

            day_of_week = dt.day_name()
            is_weekend = dt.dayofweek >= 5

            yield (date_str, day_of_week, is_weekend)

    except Exception as e:
        raise ValueError(f'Error expanding date range: {str(e)}')


def expand_time_slots(start_time, end_time, interval_minutes=30):
    """
    Expand time range into time slots

    Args:
        start_time: Start time (HH:MM)
        end_time: End time (HH:MM)
        interval_minutes: Slot interval in minutes

    Yields:
        (slot_start, slot_end, slot_index)
    """
    if start_time is None or end_time is None:
        yield (None, None, None)
        return

    try:
        from datetime import datetime, timedelta

        start = datetime.strptime(str(start_time), '%H:%M')
        end = datetime.strptime(str(end_time), '%H:%M')

        interval = timedelta(minutes=int(interval_minutes))
        slot_index = 0

        current = start
        while current < end:
            slot_start = current.strftime('%H:%M')
            next_slot = min(current + interval, end)
            slot_end = next_slot.strftime('%H:%M')

            yield (slot_start, slot_end, slot_index)

            current = next_slot
            slot_index += 1

    except Exception as e:
        raise ValueError(f'Error expanding time slots: {str(e)}')


# ==================== JSON Data Expansion ====================

def expand_json_array(json_array_str):
    """
    Expand JSON array into individual rows

    Args:
        json_array_str: JSON array string

    Yields:
        (index, element_as_json)
    """
    if json_array_str is None:
        yield (None, None)
        return

    try:
        arr = json.loads(json_array_str)
        if not isinstance(arr, list):
            raise ValueError('Input must be a JSON array')

        for idx, item in enumerate(arr):
            if isinstance(item, (dict, list)):
                yield (idx, json.dumps(item, ensure_ascii=False))
            else:
                yield (idx, str(item) if item is not None else None)

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')


def expand_json_object_keys(json_obj_str):
    """
    Expand JSON object keys into rows

    Args:
        json_obj_str: JSON object string

    Yields:
        (key, value_as_string, value_type)
    """
    if json_obj_str is None:
        yield (None, None, None)
        return

    try:
        obj = json.loads(json_obj_str)
        if not isinstance(obj, dict):
            raise ValueError('Input must be a JSON object')

        for key, value in obj.items():
            value_type = type(value).__name__
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, ensure_ascii=False)
            else:
                value_str = str(value) if value is not None else None

            yield (key, value_str, value_type)

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')


def flatten_nested_json(json_str, max_depth=10):
    """
    Flatten nested JSON into key-value pairs with dot notation

    Args:
        json_str: JSON string
        max_depth: Maximum nesting depth

    Yields:
        (flat_key, value_as_string, depth)
    """
    if json_str is None:
        yield (None, None, None)
        return

    def _flatten(obj, prefix='', depth=0):
        if depth > max_depth:
            yield (prefix, json.dumps(obj, ensure_ascii=False) if isinstance(obj, (dict, list)) else str(obj), depth)
            return

        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}.{key}" if prefix else key
                yield from _flatten(value, new_key, depth + 1)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                new_key = f"{prefix}[{idx}]"
                yield from _flatten(item, new_key, depth + 1)
        else:
            yield (prefix, str(obj) if obj is not None else None, depth)

    try:
        data = json.loads(json_str)
        yield from _flatten(data)
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')


# ==================== Statistical Expansion ====================

def expand_histogram_bins(values_json, num_bins=10):
    """
    Create histogram bins from values

    Args:
        values_json: JSON array of numeric values
        num_bins: Number of bins

    Yields:
        (bin_index, bin_start, bin_end, count, percentage)
    """
    _check_pandas()

    if values_json is None:
        yield (None, None, None, None, None)
        return

    try:
        values = np.array(json.loads(values_json), dtype=float)
        values = values[~np.isnan(values)]

        if len(values) == 0:
            yield (None, None, None, 0, 0.0)
            return

        counts, bin_edges = np.histogram(values, bins=int(num_bins))
        total = len(values)

        for i in range(len(counts)):
            bin_start = round(bin_edges[i], 4)
            bin_end = round(bin_edges[i + 1], 4)
            count = int(counts[i])
            percentage = round(count / total * 100, 2)

            yield (i, bin_start, bin_end, count, percentage)

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')
    except Exception as e:
        raise ValueError(f'Error creating histogram: {str(e)}')


def expand_quartile_distribution(values_json):
    """
    Expand values into quartile distribution

    Args:
        values_json: JSON array of values

    Yields:
        (quartile_name, min_value, max_value, count, mean_value)
    """
    _check_pandas()

    if values_json is None:
        yield (None, None, None, None, None)
        return

    try:
        values = pd.Series(json.loads(values_json), dtype=float).dropna()

        if len(values) == 0:
            yield ('Empty', None, None, 0, None)
            return

        q1 = values.quantile(0.25)
        q2 = values.quantile(0.50)
        q3 = values.quantile(0.75)

        quartiles = [
            ('Q1 (0-25%)', values.min(), q1),
            ('Q2 (25-50%)', q1, q2),
            ('Q3 (50-75%)', q2, q3),
            ('Q4 (75-100%)', q3, values.max())
        ]

        for name, low, high in quartiles:
            if name == 'Q4 (75-100%)':
                mask = (values >= low) & (values <= high)
            else:
                mask = (values >= low) & (values < high)

            subset = values[mask]
            count = len(subset)
            mean_val = round(subset.mean(), 4) if count > 0 else None

            yield (name, round(low, 4), round(high, 4), count, mean_val)

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')
    except Exception as e:
        raise ValueError(f'Error calculating quartiles: {str(e)}')


# ==================== Text Expansion ====================

def expand_ngrams(text, n=2):
    """
    Expand text into n-grams

    Args:
        text: Input text
        n: N-gram size

    Yields:
        (ngram_index, ngram_text)
    """
    if text is None:
        yield (None, None)
        return

    try:
        words = str(text).split()
        n = int(n)

        if len(words) < n:
            yield (0, ' '.join(words))
            return

        for i in range(len(words) - n + 1):
            ngram = ' '.join(words[i:i + n])
            yield (i, ngram)

    except Exception as e:
        raise ValueError(f'Error creating n-grams: {str(e)}')


def expand_sentences(text):
    """
    Expand text into individual sentences

    Args:
        text: Input text

    Yields:
        (sentence_index, sentence, word_count, char_count)
    """
    if text is None:
        yield (None, None, None, None)
        return

    try:
        import re
        # Split by sentence-ending punctuation
        sentences = re.split(r'[.!?。！？]+', str(text))
        sentences = [s.strip() for s in sentences if s.strip()]

        if not sentences:
            yield (0, '', 0, 0)
            return

        for idx, sentence in enumerate(sentences):
            word_count = len(sentence.split())
            char_count = len(sentence)
            yield (idx, sentence, word_count, char_count)

    except Exception as e:
        raise ValueError(f'Error splitting sentences: {str(e)}')


# ==================== Business Data Expansion ====================

def expand_product_variants(base_product_json, variants_json):
    """
    Expand product with all variant combinations

    Args:
        base_product_json: Base product info {"name": "...", "base_price": ...}
        variants_json: Variants {"size": ["S", "M", "L"], "color": ["Red", "Blue"]}

    Yields:
        (product_name, variant_key, variant_value, sku_suffix)
    """
    if base_product_json is None or variants_json is None:
        yield (None, None, None, None)
        return

    try:
        base = json.loads(base_product_json)
        variants = json.loads(variants_json)

        product_name = base.get('name', 'Product')

        from itertools import product as iterproduct

        keys = list(variants.keys())
        values = [variants[k] for k in keys]

        if not keys or not values:
            yield (product_name, None, None, '')
            return

        for combo in iterproduct(*values):
            for i, key in enumerate(keys):
                sku_parts = [str(v)[:3].upper() for v in combo]
                sku_suffix = '-'.join(sku_parts)
                yield (product_name, key, combo[i], sku_suffix)

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')
    except Exception as e:
        raise ValueError(f'Error expanding variants: {str(e)}')


def expand_order_items(order_json):
    """
    Expand order into individual line items

    Args:
        order_json: Order with items array
            {"order_id": "...", "items": [{"sku": "...", "qty": 1, "price": 10.0}, ...]}

    Yields:
        (order_id, item_index, sku, quantity, unit_price, line_total)
    """
    if order_json is None:
        yield (None, None, None, None, None, None)
        return

    try:
        order = json.loads(order_json)
        order_id = order.get('order_id', 'unknown')
        items = order.get('items', [])

        if not items:
            yield (order_id, None, None, None, None, None)
            return

        for idx, item in enumerate(items):
            sku = item.get('sku', '')
            qty = item.get('qty', item.get('quantity', 1))
            price = item.get('price', item.get('unit_price', 0))
            line_total = float(qty) * float(price)

            yield (order_id, idx, sku, int(qty), float(price), round(line_total, 2))

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')
    except Exception as e:
        raise ValueError(f'Error expanding order: {str(e)}')


def expand_user_events(events_json):
    """
    Expand user event log into individual events

    Args:
        events_json: Array of events with timestamps
            [{"event": "click", "timestamp": "...", "data": {...}}, ...]

    Yields:
        (event_index, event_type, timestamp, event_data_json)
    """
    if events_json is None:
        yield (None, None, None, None)
        return

    try:
        events = json.loads(events_json)
        if not isinstance(events, list):
            raise ValueError('Input must be a JSON array of events')

        if not events:
            yield (None, None, None, None)
            return

        for idx, event in enumerate(events):
            event_type = event.get('event', event.get('type', 'unknown'))
            timestamp = event.get('timestamp', event.get('time', ''))
            data = event.get('data', event.get('properties', {}))

            yield (idx, event_type, str(timestamp), json.dumps(data, ensure_ascii=False))

    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid JSON: {str(e)}')
    except Exception as e:
        raise ValueError(f'Error expanding events: {str(e)}')


# ==================== Geographic Expansion ====================

def expand_bbox_to_tiles(min_lat, min_lon, max_lat, max_lon, tile_size=0.1):
    """
    Expand bounding box into grid tiles

    Args:
        min_lat, min_lon, max_lat, max_lon: Bounding box coordinates
        tile_size: Size of each tile in degrees

    Yields:
        (tile_row, tile_col, tile_min_lat, tile_min_lon, tile_max_lat, tile_max_lon)
    """
    if any(v is None for v in [min_lat, min_lon, max_lat, max_lon]):
        yield (None, None, None, None, None, None)
        return

    try:
        min_lat, min_lon = float(min_lat), float(min_lon)
        max_lat, max_lon = float(max_lat), float(max_lon)
        tile_size = float(tile_size)

        if tile_size <= 0:
            raise ValueError('tile_size must be positive')

        row = 0
        lat = min_lat
        while lat < max_lat:
            col = 0
            lon = min_lon
            while lon < max_lon:
                tile_max_lat = min(lat + tile_size, max_lat)
                tile_max_lon = min(lon + tile_size, max_lon)

                yield (
                    row, col,
                    round(lat, 6), round(lon, 6),
                    round(tile_max_lat, 6), round(tile_max_lon, 6)
                )

                lon += tile_size
                col += 1
            lat += tile_size
            row += 1

    except Exception as e:
        raise ValueError(f'Error expanding bbox: {str(e)}')
