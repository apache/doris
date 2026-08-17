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

suite("test_adaptive_random_bucket_load", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_adaptive_random_bucket_load"
    def enableAdaptiveRandomBucketConfig =
            sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_adaptive_random_bucket_load'; """
    String oldEnableAdaptiveRandomBucket = enableAdaptiveRandomBucketConfig[0][1]

    try {
        sql """ ADMIN SET FRONTEND CONFIG ('enable_adaptive_random_bucket_load' = 'true') """
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                dt date NOT NULL,
                k int NOT NULL,
                v string
            )
            DUPLICATE KEY(dt, k)
            AUTO PARTITION BY RANGE (date_trunc(dt, 'day')) ()
            DISTRIBUTED BY RANDOM BUCKETS 4
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            )
        """

        sql """
            INSERT INTO ${tableName} VALUES
            ('2026-07-01', 1, 'a'),
            ('2026-07-01', 2, 'b'),
            ('2026-07-02', 3, 'c'),
            ('2026-07-02', 4, 'd')
        """
        sql "sync"

        def totalCount = sql "SELECT count(*) FROM ${tableName}"
        assertEquals(totalCount[0][0], 4)

        def partitions = sql "SHOW PARTITIONS FROM ${tableName}"
        assertEquals(partitions.size(), 2)

        def tablets = sql "SHOW TABLETS FROM ${tableName}"
        int tabletsWithRows = 0
        int tabletRowCount = 0
        tablets.each { tablet ->
            def tabletCount = sql "SELECT count(*) FROM ${tableName} TABLET(${tablet[0]})"
            int count = tabletCount[0][0] as int
            if (count > 0) {
                tabletsWithRows++
                tabletRowCount += count
            }
        }
        assertTrue(tabletsWithRows > 0)
        assertEquals(tabletRowCount, 4)
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ('enable_adaptive_random_bucket_load' = '${oldEnableAdaptiveRandomBucket}') """
    }
}
