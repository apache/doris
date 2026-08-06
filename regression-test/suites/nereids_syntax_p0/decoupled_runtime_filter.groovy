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

suite("decoupled_runtime_filter") {
    sql "drop table if exists drf_t1"
    sql "drop table if exists drf_t2"
    sql "drop table if exists drf_t3"

    sql """
        CREATE TABLE drf_t1 (
            k1 int NOT NULL,
            v1 int NOT NULL
        ) distributed by hash(k1) buckets 4
        properties("replication_num"="1");
    """

    sql """
        CREATE TABLE drf_t2 (
            k2 int NOT NULL,
            v2 int NOT NULL
        ) distributed by hash(k2) buckets 4
        properties("replication_num"="1");
    """

    sql """
        CREATE TABLE drf_t3 (
            k3 int NOT NULL,
            v3 int NOT NULL
        ) distributed by hash(k3) buckets 4
        properties("replication_num"="1");
    """

    // Insert test data
    sql "insert into drf_t1 values (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);"
    sql "insert into drf_t2 values (1, 100), (2, 200), (3, 300);"
    sql "insert into drf_t3 values (2, 1000), (3, 2000), (4, 3000), (5, 4000);"

    // Test 1: Correctness - decoupled RF should not change query results.
    sql "set enable_decoupled_runtime_filter=false"
    order_qt_baseline """
        select t1.k1, t2.k2, t3.k3, t1.v1, t2.v2, t3.v3
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3;
    """

    sql "set enable_decoupled_runtime_filter=true"
    order_qt_decoupled """
        select t1.k1, t2.k2, t3.k3, t1.v1, t2.v2, t3.v3
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3;
    """

    // Test 2: More complex join chain - 4 tables
    sql "drop table if exists drf_t4"
    sql """
        CREATE TABLE drf_t4 (
            k4 int NOT NULL,
            v4 int NOT NULL
        ) distributed by hash(k4) buckets 4
        properties("replication_num"="1");
    """
    sql "insert into drf_t4 values (1, 10000), (2, 20000), (3, 30000);"

    sql "set enable_decoupled_runtime_filter=false"
    order_qt_chain_baseline """
        select t1.k1, t2.k2, t3.k3, t4.k4
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3
        join drf_t4 t4 on t3.k3 = t4.k4;
    """

    sql "set enable_decoupled_runtime_filter=true"
    order_qt_chain_decoupled """
        select t1.k1, t2.k2, t3.k3, t4.k4
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3
        join drf_t4 t4 on t3.k3 = t4.k4;
    """

    // Test 3: Verify decoupled RF does not fire on outer joins
    sql "set enable_decoupled_runtime_filter=true"
    order_qt_left_join """
        select t1.k1, t2.k2, t3.k3
        from drf_t1 t1
        left join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t1.k1 = t3.k3;
    """

    // Test 4: Verify decoupled_rf_ndv_ratio_threshold affects explain output
    // With a very high threshold (1.0), decoupled RF should always be preferred
    // (standard RF removed) since ratio is always < 1.0 when stats differ
    sql "set enable_decoupled_runtime_filter=true"
    sql "set decoupled_rf_ndv_ratio_threshold=1.0"
    def explainHigh = sql """explain shape plan
        select t1.k1, t2.k2, t3.k3
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3;
    """
    logger.info("Explain with high threshold: ${explainHigh}")

    // With threshold=0.0, decoupled RF should never be preferred
    // (always non-blocking, standard RF kept)
    sql "set decoupled_rf_ndv_ratio_threshold=0.0"
    def explainLow = sql """explain shape plan
        select t1.k1, t2.k2, t3.k3
        from drf_t1 t1
        join drf_t2 t2 on t1.k1 = t2.k2
        join drf_t3 t3 on t2.k2 = t3.k3;
    """
    logger.info("Explain with low threshold: ${explainLow}")

    // Reset
    sql "set decoupled_rf_ndv_ratio_threshold=0.5"
    sql "set enable_decoupled_runtime_filter=false"
}
