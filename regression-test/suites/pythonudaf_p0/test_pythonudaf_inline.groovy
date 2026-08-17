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

suite("test_pythonudaf_inline") {
    // Test Python UDAF using Inline mode
    
    def runtime_version = getPythonUdfRuntimeVersion()
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS test_pythonudaf_inline_table """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudaf_inline_table (
            `id`       INT NOT NULL COMMENT "ID",
            `value`    INT COMMENT "Value",
            `amount`   DOUBLE COMMENT "Amount",
            `category` VARCHAR(10) NOT NULL COMMENT "Category"
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        
        // Insert test data
        sql """ INSERT INTO test_pythonudaf_inline_table VALUES
                (1, 10, 10.5, 'A'),
                (2, 20, 20.5, 'A'),
                (3, 30, 30.5, 'B'),
                (4, 40, 40.5, 'B'),
                (5, 50, 50.5, 'C');
            """
        
        qt_select_data """ SELECT * FROM test_pythonudaf_inline_table ORDER BY id; """

        // ========================================
        // Test 1: Simple Sum UDAF (Inline)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_sum_inline(INT); """
        
        sql """
        CREATE AGGREGATE FUNCTION udaf_sum_inline(INT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class SumUDAF:
    def __init__(self):
        self.sum = 0
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        if other_state is not None:
            self.sum += other_state
    
    def finish(self):
        return self.sum
    
    @property
    def aggregate_state(self):
        return self.sum
\$\$;
        """

        // Test basic aggregation
        qt_test1 """ SELECT udaf_sum_inline(value) as total, sum(value) as native_sum FROM test_pythonudaf_inline_table; """
        
        // Test with GROUP BY
        qt_test2 """ SELECT category,
                     udaf_sum_inline(value) as sum_val,
                     sum(value) as native_sum
                     FROM test_pythonudaf_inline_table 
                     GROUP BY category 
                     ORDER BY category; """

        // ========================================
        // Test 2: Average UDAF (Inline)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_avg_inline(DOUBLE); """
        
        sql """
        CREATE AGGREGATE FUNCTION udaf_avg_inline(DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "AvgUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class AvgUDAF:
    def __init__(self):
        self.count = 0
        self.sum = 0.0
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum += value
    
    def merge(self, other_state):
        if other_state is not None:
            other_count, other_sum = other_state
            self.count += other_count
            self.sum += other_sum
    
    def finish(self):
        if self.count == 0:
            return None
        return self.sum / self.count
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum)
\$\$;
        """

        qt_test3 """ SELECT udaf_avg_inline(amount) as avg_amount, avg(amount) as native_avg FROM test_pythonudaf_inline_table; """
        
        qt_test4 """ SELECT category, 
                            udaf_avg_inline(amount) as py_avg,
                            avg(amount) as native_avg
                     FROM test_pythonudaf_inline_table 
                     GROUP BY category 
                     ORDER BY category; """

        // ========================================
        // Test 3: Count UDAF (Inline)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_count_inline(INT); """
        
        sql """
        CREATE AGGREGATE FUNCTION udaf_count_inline(INT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class CountUDAF:
    def __init__(self):
        self.count = 0
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
    
    def merge(self, other_state):
        if other_state is not None:
            self.count += other_state
    
    def finish(self):
        return self.count
    
    @property
    def aggregate_state(self):
        return self.count
\$\$;
        """

        qt_test5 """ SELECT udaf_count_inline(value) as total_count, count(value) as native_count FROM test_pythonudaf_inline_table; """
        
        qt_test6 """ SELECT category, 
                            udaf_count_inline(value) as py_count,
                            count(value) as native_count
                     FROM test_pythonudaf_inline_table 
                     GROUP BY category 
                     ORDER BY category; """

        // ========================================
        // Test 4: Max UDAF (Inline)
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_max_inline(INT); """
        
        sql """
        CREATE AGGREGATE FUNCTION udaf_max_inline(INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "MaxUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class MaxUDAF:
    def __init__(self):
        self.max_value = None
    
    def accumulate(self, value):
        if value is not None:
            if self.max_value is None or value > self.max_value:
                self.max_value = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.max_value is None or other_state > self.max_value:
                self.max_value = other_state
    
    def finish(self):
        return self.max_value
    
    @property
    def aggregate_state(self):
        return self.max_value
\$\$;
        """

        qt_test7 """ SELECT udaf_max_inline(value) as max_value, max(value) as native_max FROM test_pythonudaf_inline_table; """
        
        qt_test8 """ SELECT category, 
                            udaf_max_inline(value) as py_max,
                            max(value) as native_max
                     FROM test_pythonudaf_inline_table 
                     GROUP BY category 
                     ORDER BY category; """

        // ========================================
        // Test 5: NULL handling
        // ========================================
        sql """ INSERT INTO test_pythonudaf_inline_table VALUES (6, NULL, 60.5, 'A'); """
        sql """ INSERT INTO test_pythonudaf_inline_table VALUES (7, NULL, 70.5, 'B'); """
        
        qt_test_null1 """ SELECT udaf_sum_inline(value) as total, sum(value) as native_sum FROM test_pythonudaf_inline_table; """
        qt_test_null2 """ SELECT udaf_count_inline(value) as count, count(value) as native_count FROM test_pythonudaf_inline_table; """
        qt_test_null3 """ SELECT category,
                          udaf_sum_inline(value) as sum_val,
                          sum(value) as native_sum
                          FROM test_pythonudaf_inline_table 
                          GROUP BY category 
                          ORDER BY category; """

        // ========================================
        // Test 6: Window Functions
        // ========================================
        qt_test_window1 """ SELECT id, category, value,
                            udaf_sum_inline(value) OVER(PARTITION BY category) as sum_by_cat,
                            sum(value) OVER(PARTITION BY category) as native_sum
                            FROM test_pythonudaf_inline_table 
                            WHERE value IS NOT NULL
                            ORDER BY category, id; """

        qt_test_window2 """ SELECT id, category, value,
                            udaf_sum_inline(value) OVER(PARTITION BY category ORDER BY id) as running_sum,
                            sum(value) OVER(PARTITION BY category ORDER BY id) as native_sum
                            FROM test_pythonudaf_inline_table 
                            WHERE value IS NOT NULL
                            ORDER BY category, id; """

        // ========================================
        // Test 7: Multiple UDAFs in one query
        // ========================================
        qt_test_multiple """ SELECT category,
                             udaf_sum_inline(value) as sum_val,
                             sum(value) as native_sum,
                             udaf_count_inline(value) as count_val,
                             count(value) as native_count,
                             udaf_max_inline(value) as max_val,
                             max(value) as native_max,
                             udaf_avg_inline(amount) as avg_amount,
                             avg(amount) as native_avg
                             FROM test_pythonudaf_inline_table
                             GROUP BY category
                             ORDER BY category; """

        // ========================================
        // Test 8: Global Function
        // ========================================
        sql """ DROP GLOBAL FUNCTION IF EXISTS udaf_sum_global(INT); """
        
        sql """
        CREATE GLOBAL AGGREGATE FUNCTION udaf_sum_global(INT) 
        RETURNS BIGINT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class SumUDAF:
    def __init__(self):
        self.sum = 0
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        if other_state is not None:
            self.sum += other_state
    
    def finish(self):
        return self.sum
    
    @property
    def aggregate_state(self):
        return self.sum
\$\$;
        """

        qt_test_global1 """ SELECT udaf_sum_global(value) as total, sum(value) as native_sum FROM test_pythonudaf_inline_table; """
        qt_test_global2 """ SELECT category,
                            udaf_sum_global(value) as sum_val,
                            sum(value) as native_sum
                            FROM test_pythonudaf_inline_table
                            GROUP BY category
                            ORDER BY category; """

        // Empty input with high pipeline parallelism should still succeed.
        qt_test_empty_parallel """ SELECT /*+SET_VAR(parallel_pipeline_task_num=8)*/
                                  udaf_sum_inline(value) as total
                                  FROM test_pythonudaf_inline_table
                                  WHERE id < 0; """
        qt_test_global_empty_parallel """ SELECT /*+SET_VAR(parallel_pipeline_task_num=8)*/
                                         udaf_sum_global(value) as total
                                         FROM test_pythonudaf_inline_table
                                         WHERE id < 0; """

        // ========================================
        // Test 9: LARGEINT Sum UDAF (Inline)
        // ========================================
        sql """ DROP TABLE IF EXISTS test_pythonudaf_convert_type_table """
        sql """
        CREATE TABLE IF NOT EXISTS test_pythonudaf_convert_type_table (
            `id`       INT NOT NULL,
            `val`      LARGEINT,
            `category` VARCHAR(10) NOT NULL,
            `ip_v4`    IPV4,
            `ip_v6`    IPV6
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """

        sql """ INSERT INTO test_pythonudaf_convert_type_table VALUES
                (1, 100, 'A', '192.168.1.1', '2001:db8::1'),
                (2, 200, 'A', '10.0.0.1', '::1'),
                (3, 300, 'B', '8.8.8.8', '2001:4860:4860::8888'),
                (4, 400, 'B', '172.16.0.1', 'fe80::1'),
                (5, NULL, 'A', NULL, NULL);
            """

        sql """ DROP FUNCTION IF EXISTS udaf_sum_largeint_inline(LARGEINT); """

        sql """
        CREATE AGGREGATE FUNCTION udaf_sum_largeint_inline(LARGEINT)
        RETURNS LARGEINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "SumLargeIntUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class SumLargeIntUDAF:
    def __init__(self):
        self.sum = 0

    def accumulate(self, value):
        if value is not None:
            self.sum += value

    def merge(self, other_state):
        if other_state is not None:
            self.sum += other_state

    def finish(self):
        return self.sum

    @property
    def aggregate_state(self):
        return self.sum
\$\$;
        """

        qt_test_largeint1 """ SELECT udaf_sum_largeint_inline(val) as total FROM test_pythonudaf_convert_type_table; """

        qt_test_largeint2 """ SELECT category,
                     udaf_sum_largeint_inline(val) as sum_val
                     FROM test_pythonudaf_convert_type_table
                     GROUP BY category
                     ORDER BY category; """

        // ========================================
        // Test 10: IPv4 UDAF input type conversion
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_count_private_ipv4_inline(IPV4); """

        sql """
        CREATE AGGREGATE FUNCTION udaf_count_private_ipv4_inline(IPV4)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountPrivateIPv4UDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class CountPrivateIPv4UDAF:
    def __init__(self):
        self.count = 0

    def accumulate(self, value):
        if value is not None and value.is_private:
            self.count += 1

    def merge(self, other_state):
        if other_state is not None:
            self.count += other_state

    def finish(self):
        return self.count

    @property
    def aggregate_state(self):
        return self.count
\$\$;
        """

        qt_test_ipv4_udaf1 """ SELECT udaf_count_private_ipv4_inline(ip_v4) as private_ipv4_count
                               FROM test_pythonudaf_convert_type_table; """
        qt_test_ipv4_udaf2 """ SELECT category,
                                      udaf_count_private_ipv4_inline(ip_v4) as private_ipv4_count
                               FROM test_pythonudaf_convert_type_table
                               GROUP BY category
                               ORDER BY category; """

        // ========================================
        // Test 11: IPv6 UDAF input type conversion
        // ========================================
        sql """ DROP FUNCTION IF EXISTS udaf_count_loopback_ipv6_inline(IPV6); """

        sql """
        CREATE AGGREGATE FUNCTION udaf_count_loopback_ipv6_inline(IPV6)
        RETURNS BIGINT
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CountLoopbackIPv6UDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class CountLoopbackIPv6UDAF:
    def __init__(self):
        self.count = 0

    def accumulate(self, value):
        if value is not None and value.is_loopback:
            self.count += 1

    def merge(self, other_state):
        if other_state is not None:
            self.count += other_state

    def finish(self):
        return self.count

    @property
    def aggregate_state(self):
        return self.count
\$\$;
        """

        qt_test_ipv6_udaf1 """ SELECT udaf_count_loopback_ipv6_inline(ip_v6) as loopback_ipv6_count
                               FROM test_pythonudaf_convert_type_table; """
        qt_test_ipv6_udaf2 """ SELECT category,
                                      udaf_count_loopback_ipv6_inline(ip_v6) as loopback_ipv6_count
                               FROM test_pythonudaf_convert_type_table
                               GROUP BY category
                               ORDER BY category; """

    } finally {
        try_sql("DROP GLOBAL FUNCTION IF EXISTS udaf_sum_global(INT);")
        try_sql("DROP FUNCTION IF EXISTS udaf_sum_inline(INT);")
        try_sql("DROP FUNCTION IF EXISTS udaf_avg_inline(DOUBLE);")
        try_sql("DROP FUNCTION IF EXISTS udaf_count_inline(INT);")
        try_sql("DROP FUNCTION IF EXISTS udaf_max_inline(INT);")
        try_sql("DROP FUNCTION IF EXISTS udaf_sum_largeint_inline(LARGEINT);")
        try_sql("DROP FUNCTION IF EXISTS udaf_count_private_ipv4_inline(IPV4);")
        try_sql("DROP FUNCTION IF EXISTS udaf_count_loopback_ipv6_inline(IPV6);")
        try_sql("DROP TABLE IF EXISTS test_pythonudaf_inline_table")
        try_sql("DROP TABLE IF EXISTS test_pythonudaf_convert_type_table")
    }
}
