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

/**
 * Regression test: old Coordinator + enable_local_shuffle_planner must not hang.
 *
 * When canUseNereidsDistributePlanner=false (e.g. proxyExecute forwarding),
 * FE uses old Coordinator which does not plan local exchange. If
 * enable_local_shuffle_planner=true was passed to BE, BE would skip its own
 * _plan_local_exchange, leaving no LE at all — causing pooling fragments to
 * hang on SHUFFLE_DATA_DEPENDENCY.
 *
 * Fix: old Coordinator forces enableLocalShufflePlanner=false in query options
 * when distributedPlans is null, so BE falls back to native LE planning.
 */
suite("test_old_coordinator_local_shuffle") {

    sql "DROP TABLE IF EXISTS oc_t0"
    sql "DROP TABLE IF EXISTS oc_t1"

    sql """
        CREATE TABLE oc_t0 (
            pk INT NULL,
            k1 INT NULL,
            v1 INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(pk, k1)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES (
            'replication_allocation' = 'tag.location.default: 1',
            'enable_unique_key_merge_on_write' = 'true'
        )
    """

    sql """
        CREATE TABLE oc_t1 (
            pk INT NULL,
            k1 INT NULL,
            v1 INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(pk, k1)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES (
            'replication_allocation' = 'tag.location.default: 1',
            'enable_unique_key_merge_on_write' = 'true'
        )
    """

    sql "INSERT INTO oc_t0 VALUES (1,1,10),(2,2,20),(3,3,30),(4,4,40),(5,5,50)"
    sql "INSERT INTO oc_t1 VALUES (1,1,100),(2,2,200),(3,3,300),(6,6,600),(7,7,700)"

    // Test 1: old Coordinator + default enable_local_shuffle_planner=true
    // This simulates the proxyExecute forwarding scenario where
    // canUseNereidsDistributePlanner=false.
    sql "SET enable_nereids_distribute_planner = false"
    sql "SET enable_local_shuffle = true"
    sql "SET ignore_storage_data_distribution = true"
    sql "SET query_timeout = 30"

    sql """
        MERGE INTO oc_t0 t USING oc_t1 s
        ON t.pk = s.pk AND t.k1 = s.k1
        WHEN MATCHED THEN UPDATE SET v1 = s.v1
        WHEN NOT MATCHED THEN INSERT (pk, k1, v1) VALUES (s.pk, s.k1, s.v1)
    """

    def result = sql "SELECT * FROM oc_t0 ORDER BY pk"
    assertEquals(7, result.size())

    // Test 2: INSERT INTO SELECT with FE-planned LE should not double-insert LE.
    // NereidsCoordinator has distributedPlans != null, so enableLocalShufflePlanner
    // stays true and BE does not add its own LE on top of FE's.
    sql "SET enable_nereids_distribute_planner = true"
    sql "SET enable_local_shuffle_planner = true"

    sql "TRUNCATE TABLE oc_t0"
    sql """
        INSERT INTO oc_t0
        SELECT number, number % 10, number * 100
        FROM numbers('number' = '50')
    """

    def cnt = sql "SELECT COUNT(*) FROM oc_t0"
    assertEquals(50, cnt[0][0] as int)
}
