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

suite("test_pythonudaf_complex_aggregation_inline") {
    // Test complex aggregation scenarios with Python UDAFs
    // Including: variance, standard deviation, median, percentile, collect_list
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table with statistical data
        sql """ DROP TABLE IF EXISTS stats_test; """
        sql """
        CREATE TABLE stats_test (
            id INT,
            category STRING,
            value DOUBLE,
            score INT,
            tag STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO stats_test VALUES
        (1, 'A', 10.5, 85, 'alpha'),
        (2, 'A', 20.3, 92, 'beta'),
        (3, 'A', 15.7, 78, 'gamma'),
        (4, 'A', 30.2, 95, 'delta'),
        (5, 'A', 25.1, 88, 'alpha'),
        (6, 'B', 12.4, 70, 'beta'),
        (7, 'B', 18.9, 85, 'gamma'),
        (8, 'B', 22.5, 90, 'alpha'),
        (9, 'B', 16.3, 82, 'beta'),
        (10, 'C', 35.7, 98, 'delta'),
        (11, 'C', 28.4, 91, 'gamma'),
        (12, 'C', 31.2, 87, 'alpha'),
        (13, 'C', 26.8, 93, 'beta'),
        (14, 'C', 29.5, 89, 'delta'),
        (15, 'C', 33.1, 95, 'gamma');
        """
        
        qt_select_data """ SELECT * FROM stats_test ORDER BY id; """
        
        // ========================================
        // Test 1: Variance UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_variance(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_variance(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "VarianceUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class VarianceUDAF:
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count, other_sum, other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return variance
\$\$;
        """
        
        qt_variance_all """ SELECT py_variance(value) as variance FROM stats_test; """
        qt_variance_group """ SELECT category, py_variance(value) as variance 
                             FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 2: Standard Deviation UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_stddev(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_stddev(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "StdDevUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

class StdDevUDAF:
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count, other_sum, other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return math.sqrt(max(0, variance))
\$\$;
        """
        
        qt_stddev_all """ SELECT py_stddev(value) as stddev FROM stats_test; """
        qt_stddev_group """ SELECT category, py_stddev(value) as stddev 
                            FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 3: Median UDAF (Approximate using sorted list)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_median(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_median(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MedianUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class MedianUDAF:
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
        
        qt_median_all """ SELECT py_median(value) as median FROM stats_test; """
        qt_median_group """ SELECT category, py_median(value) as median 
                            FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 4: Collect List UDAF (String concatenation)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_collect_list(STRING); """
        sql """
        CREATE AGGREGATE FUNCTION py_collect_list(STRING)
        RETURNS STRING
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CollectListUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class CollectListUDAF:
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
            return None
        return ','.join(sorted(self.items))
\$\$;
        """
        
        qt_collect_all """ SELECT py_collect_list(tag) as tags FROM stats_test; """
        qt_collect_group """ SELECT category, py_collect_list(tag) as tags 
                             FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 5: Min-Max Range UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_range(INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_range(INT)
        RETURNS INT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "RangeUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class RangeUDAF:
    def __init__(self):
        self.min_val = None
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return (self.min_val, self.max_val)
    
    def accumulate(self, value):
        if value is not None:
            if self.min_val is None or value < self.min_val:
                self.min_val = value
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        other_min, other_max = other_state
        if other_min is not None:
            if self.min_val is None or other_min < self.min_val:
                self.min_val = other_min
        if other_max is not None:
            if self.max_val is None or other_max > self.max_val:
                self.max_val = other_max
    
    def finish(self):
        if self.min_val is None or self.max_val is None:
            return None
        return self.max_val - self.min_val
\$\$;
        """
        
        qt_range_all """ SELECT py_range(score) as score_range FROM stats_test; """
        qt_range_group """ SELECT category, py_range(score) as score_range 
                           FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 6: Geometric Mean UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_geomean(DOUBLE); """
        sql """
        CREATE AGGREGATE FUNCTION py_geomean(DOUBLE)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "GeometricMeanUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
import math

class GeometricMeanUDAF:
    def __init__(self):
        self.log_sum = 0.0
        self.count = 0
    
    @property
    def aggregate_state(self):
        return (self.log_sum, self.count)
    
    def accumulate(self, value):
        if value is not None and value > 0:
            self.log_sum += math.log(value)
            self.count += 1
    
    def merge(self, other_state):
        other_log_sum, other_count = other_state
        self.log_sum += other_log_sum
        self.count += other_count
    
    def finish(self):
        if self.count == 0:
            return None
        return math.exp(self.log_sum / self.count)
\$\$;
        """
        
        qt_geomean_all """ SELECT py_geomean(value) as geomean FROM stats_test; """
        qt_geomean_group """ SELECT category, py_geomean(value) as geomean 
                             FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 7: Weighted Average UDAF
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_weighted_avg(DOUBLE, INT)
        RETURNS DOUBLE
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "WeightedAvgUDAF",
            "runtime_version" = "3.8.10"
        )
        AS \$\$
class WeightedAvgUDAF:
    def __init__(self):
        self.weighted_sum = 0.0
        self.weight_sum = 0
    
    @property
    def aggregate_state(self):
        return (self.weighted_sum, self.weight_sum)
    
    def accumulate(self, value, weight):
        if value is not None and weight is not None and weight > 0:
            self.weighted_sum += value * weight
            self.weight_sum += weight
    
    def merge(self, other_state):
        other_weighted_sum, other_weight_sum = other_state
        self.weighted_sum += other_weighted_sum
        self.weight_sum += other_weight_sum
    
    def finish(self):
        if self.weight_sum == 0:
            return None
        return self.weighted_sum / self.weight_sum
\$\$;
        """
        
        qt_weighted_avg_all """ SELECT py_weighted_avg(value, score) as weighted_avg FROM stats_test; """
        qt_weighted_avg_group """ SELECT category, py_weighted_avg(value, score) as weighted_avg 
                                  FROM stats_test GROUP BY category ORDER BY category; """
        
        // ========================================
        // Test 8: Multiple aggregations in one query
        // ========================================
        qt_multi_agg """ 
            SELECT 
                category,
                py_variance(value) as variance,
                py_stddev(value) as stddev,
                py_median(value) as median,
                py_range(score) as score_range,
                py_geomean(value) as geomean,
                py_weighted_avg(value, score) as weighted_avg
            FROM stats_test 
            GROUP BY category 
            ORDER BY category;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_variance(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_stddev(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_median(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_collect_list(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_range(INT);")
        try_sql("DROP FUNCTION IF EXISTS py_geomean(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS py_weighted_avg(DOUBLE, INT);")
        try_sql("DROP TABLE IF EXISTS stats_test;")
    }
}
