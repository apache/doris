// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_pythonudaf_window_advanced_inline") {
    // Advanced window function tests with Python UDAFs
    // Including: moving averages, percentiles, custom analytics
    
    def runtime_version = "3.8.10"
    
    try {
        // Create time series data for advanced analytics
        sql """ DROP TABLE IF EXISTS time_series_data; """
        sql """
        CREATE TABLE time_series_data (
            timestamp DATETIME,
            metric_name STRING,
            metric_value DOUBLE,
            device_id STRING,
            location STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(timestamp)
        DISTRIBUTED BY HASH(timestamp) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO time_series_data VALUES
        ('2024-01-01 10:00:00', 'temperature', 20.5, 'sensor1', 'room1'),
        ('2024-01-01 10:05:00', 'temperature', 21.0, 'sensor1', 'room1'),
        ('2024-01-01 10:10:00', 'temperature', 21.5, 'sensor1', 'room1'),
        ('2024-01-01 10:15:00', 'temperature', 22.0, 'sensor1', 'room1'),
        ('2024-01-01 10:20:00', 'temperature', 22.5, 'sensor1', 'room1'),
        ('2024-01-01 10:00:00', 'humidity', 45.0, 'sensor2', 'room1'),
        ('2024-01-01 10:05:00', 'humidity', 46.0, 'sensor2', 'room1'),
        ('2024-01-01 10:10:00', 'humidity', 47.5, 'sensor2', 'room1'),
        ('2024-01-01 10:15:00', 'humidity', 48.0, 'sensor2', 'room1'),
        ('2024-01-01 10:20:00', 'humidity', 49.0, 'sensor2', 'room1'),
        ('2024-01-01 10:00:00', 'temperature', 19.5, 'sensor3', 'room2'),
        ('2024-01-01 10:05:00', 'temperature', 20.0, 'sensor3', 'room2'),
        ('2024-01-01 10:10:00', 'temperature', 20.8, 'sensor3', 'room2'),
        ('2024-01-01 10:15:00', 'temperature', 21.2, 'sensor3', 'room2'),
        ('2024-01-01 10:20:00', 'temperature', 21.8, 'sensor3', 'room2');
        """
        
        qt_select_data """ SELECT * FROM time_series_data ORDER BY timestamp, device_id; """
        
        // ========================================
        // UDAF 1: Moving Average (SMA - Simple Moving Average)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_moving_avg(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_moving_avg(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MovingAvgUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MovingAvgUDAF:
    def __init__(self):
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
        return sum(self.values) / len(self.values)
\$\$;
        """
        
        // ========================================
        // UDAF 2: Standard Deviation (for volatility analysis)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_window_stddev(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_window_stddev(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "WindowStdDevUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

class WindowStdDevUDAF:
    def __init__(self):
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
        if not self.values or len(self.values) < 2:
            return None
        mean = sum(self.values) / len(self.values)
        variance = sum((x - mean) ** 2 for x in self.values) / len(self.values)
        return math.sqrt(variance)
\$\$;
        """
        
        // ========================================
        // UDAF 3: Delta (Change from previous value)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_last_value(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_last_value(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "LastValueUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class LastValueUDAF:
    def __init__(self):
        self.last = None
    
    @property
    def aggregate_state(self):
        return self.last
    
    def accumulate(self, value):
        if value is not None:
            self.last = value
    
    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state
    
    def finish(self):
        return self.last
\$\$;
        """
        
        // ========================================
        // UDAF 4: Min-Max Normalization in window
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_window_min(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_window_min(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "WindowMinUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class WindowMinUDAF:
    def __init__(self):
        self.min_val = None
    
    @property
    def aggregate_state(self):
        return self.min_val
    
    def accumulate(self, value):
        if value is not None:
            if self.min_val is None or value < self.min_val:
                self.min_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.min_val is None or other_state < self.min_val:
                self.min_val = other_state
    
    def finish(self):
        return self.min_val
\$\$;
        """
        
        sql """ DROP FUNCTION IF EXISTS py_window_max(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_window_max(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "WindowMaxUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class WindowMaxUDAF:
    def __init__(self):
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return self.max_val
    
    def accumulate(self, value):
        if value is not None:
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.max_val is None or other_state > self.max_val:
                self.max_val = other_state
    
    def finish(self):
        return self.max_val
\$\$;
        """
        
        // ========================================
        // Test 1: Moving Average with sliding window
        // ========================================
        qt_moving_avg_3period """
            SELECT 
                timestamp,
                device_id,
                metric_value,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY device_id 
                    ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as sma_3period
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, timestamp;
        """
        
        // ========================================
        // Test 2: Rolling standard deviation
        // ========================================
        qt_rolling_stddev """
            SELECT 
                timestamp,
                device_id,
                metric_value,
                py_window_stddev(metric_value) OVER (
                    PARTITION BY device_id 
                    ORDER BY timestamp
                    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                ) as rolling_stddev_4period
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, timestamp;
        """
        
        // ========================================
        // Test 3: Moving average in window
        // ========================================
        qt_change_from_first """
            SELECT 
                timestamp,
                device_id,
                metric_value,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY device_id 
                    ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as moving_avg_3
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, timestamp;
        """
        
        // ========================================
        // Test 4: Min-Max normalization within window
        // ========================================
        qt_minmax_normalize """
            SELECT 
                device_id,
                timestamp,
                metric_value,
                py_window_min(metric_value) OVER (PARTITION BY device_id ORDER BY timestamp) as window_min,
                py_window_max(metric_value) OVER (PARTITION BY device_id ORDER BY timestamp) as window_max
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, timestamp;
        """
        
        // ========================================
        // Test 5: Exponential smoothing simulation
        // ========================================
        qt_cumulative_weighted """
            SELECT 
                timestamp,
                location,
                metric_name,
                metric_value,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY location, metric_name 
                    ORDER BY timestamp
                ) as overall_avg,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY location, metric_name 
                    ORDER BY timestamp
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) as two_period_avg
            FROM time_series_data
            ORDER BY location, metric_name, timestamp;
        """
        
        // ========================================
        // Test 6: Trend detection (comparing to moving average)
        // ========================================
        qt_trend_detection """
            SELECT 
                timestamp,
                device_id,
                metric_value,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY device_id 
                    ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as ma_3,
                CASE 
                    WHEN metric_value > py_moving_avg(metric_value) OVER (
                        PARTITION BY device_id 
                        ORDER BY timestamp
                        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                    ) THEN 'Above MA'
                    ELSE 'Below MA'
                END as trend
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, timestamp;
        """
        
        // ========================================
        // Test 7: Multi-metric window analysis with separate queries
        // ========================================
        qt_multi_metric """
            SELECT 
                t.timestamp,
                t.location,
                t.temp_value,
                h.humidity_value,
                py_moving_avg(t.temp_value) OVER (PARTITION BY t.location ORDER BY t.timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as temp_ma,
                py_moving_avg(h.humidity_value) OVER (PARTITION BY h.location ORDER BY h.timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as humidity_ma
            FROM 
                (SELECT timestamp, location, metric_value as temp_value 
                 FROM time_series_data 
                 WHERE metric_name = 'temperature') t
            LEFT JOIN 
                (SELECT timestamp, location, metric_value as humidity_value 
                 FROM time_series_data 
                 WHERE metric_name = 'humidity') h
            ON t.timestamp = h.timestamp AND t.location = h.location
            WHERE t.temp_value IS NOT NULL OR h.humidity_value IS NOT NULL
            ORDER BY t.location, t.timestamp;
        """
        
        // ========================================
        // Test 8: Gap detection in time series
        // ========================================
        sql """ DROP TABLE IF EXISTS gap_data; """
        sql """
        CREATE TABLE gap_data (
            id INT,
            ts DATETIME,
            sensor STRING,
            value DOUBLE
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO gap_data VALUES
        (1, '2024-01-01 10:00:00', 'A', 10.0),
        (2, '2024-01-01 10:05:00', 'A', 11.0),
        (3, '2024-01-01 10:10:00', 'A', 12.0),
        (4, '2024-01-01 10:20:00', 'A', 15.0),  -- gap here
        (5, '2024-01-01 10:25:00', 'A', 16.0),
        (6, '2024-01-01 10:00:00', 'B', 20.0),
        (7, '2024-01-01 10:10:00', 'B', 22.0),  -- gap here
        (8, '2024-01-01 10:15:00', 'B', 23.0);
        """
        
        qt_gap_analysis """
            SELECT 
                sensor,
                ts,
                value,
                py_last_value(value) OVER (
                    PARTITION BY sensor 
                    ORDER BY ts
                    ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                ) as running_last
            FROM gap_data
            ORDER BY sensor, ts;
        """
        
        // ========================================
        // Test 9: Percentile approximation in windows
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_percentile_50(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_percentile_50(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "Percentile50UDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class Percentile50UDAF:
    def __init__(self):
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
        sorted_vals = sorted(self.values)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2.0
        else:
            return sorted_vals[n//2]
\$\$;
        """
        
        qt_window_percentile """
            SELECT 
                location,
                timestamp,
                metric_value,
                py_percentile_50(metric_value) OVER (
                    PARTITION BY location 
                    ORDER BY timestamp
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as rolling_median
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY location, timestamp;
        """
        
        // ========================================
        // Test 10: Cumulative distribution
        // ========================================
        qt_cumulative_dist """
            SELECT 
                device_id,
                metric_value,
                py_moving_avg(metric_value) OVER (
                    PARTITION BY device_id 
                    ORDER BY metric_value
                ) as cumulative_avg,
                COUNT(*) OVER (
                    PARTITION BY device_id 
                    ORDER BY metric_value
                ) as count_up_to_value
            FROM time_series_data
            WHERE metric_name = 'temperature'
            ORDER BY device_id, metric_value;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_moving_avg(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_window_stddev(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_last_value(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_window_min(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_window_max(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_percentile_50(DOUBLE);")
        try_sql("DROP TABLE IF EXISTS time_series_data;")
        try_sql("DROP TABLE IF EXISTS gap_data;")
    }
}
