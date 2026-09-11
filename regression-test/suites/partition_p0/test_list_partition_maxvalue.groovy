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

import org.apache.doris.regression.suite.ClusterOptions

// MAXVALUE is forbidden in LIST partition values at DDL time. This suite verifies the
// rejection and, via a debug point that bypasses the check (simulating tables created by
// older versions that allowed MAXVALUE in LIST partitions), verifies that such legacy
// tables still load and query correctly.
suite("test_list_partition_maxvalue", "docker") {
    def options = new ClusterOptions()
    options.enableDebugPoints()

    docker(options) {
        sleep 2000
        try {
            // 1. MAXVALUE in CREATE TABLE list partition is rejected.
            test {
                sql """
                    CREATE TABLE list_table_maxvalue_err (
                        id int null,
                        k largeint null
                    )
                    PARTITION BY LIST (`id`, `k`)
                    (
                        PARTITION p1 VALUES IN ((NULL, MAXVALUE))
                    )
                    DISTRIBUTED BY HASH(`k`) BUCKETS 16
                    PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1"
                    );
                    """
                exception "MAXVALUE is not allowed in LIST partition"
            }

            // 2. RANGE partition's VALUES LESS THAN (MAXVALUE) is still allowed.
            sql """
                CREATE TABLE range_table_maxvalue_ok (
                    id int null,
                    k largeint null
                )
                PARTITION BY RANGE (`k`)
                (
                    PARTITION p1 VALUES LESS THAN ("100"),
                    PARTITION p2 VALUES LESS THAN (MAXVALUE)
                )
                DISTRIBUTED BY HASH(`k`) BUCKETS 16
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
                """

            // 3. ALTER TABLE ADD PARTITION with MAXVALUE is rejected too.
            sql """
                CREATE TABLE list_table_alter_err (
                    id int null,
                    k largeint null
                )
                PARTITION BY LIST (`id`, `k`)
                (
                    PARTITION p1 VALUES IN ((NULL, "1"))
                )
                DISTRIBUTED BY HASH(`k`) BUCKETS 16
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
                """
            test {
                sql """ ALTER TABLE `list_table_alter_err` ADD PARTITION `p2` VALUES IN ((NULL, MAXVALUE)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                exception "MAXVALUE is not allowed in LIST partition"
            }

            // 4. Simulate a table created by an older version (which allowed MAXVALUE in
            // LIST partitions): bypass the DDL check with a debug point, then verify that
            // loading and pruning such legacy tables still works.
            GetDebugPoint().enableDebugPointForAllFEs('FE.skipCheckMaxValueInListPartition', null)
            try {
                sql """
                    CREATE TABLE list_table_null (
                        id int null,
                        k largeint null
                    )
                    AUTO PARTITION BY LIST (`id`, `k`)
                    (
                    )
                    DISTRIBUTED BY HASH(`k`) BUCKETS 16
                    PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1"
                    );
                    """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p1` VALUES IN ((NULL, "1")) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p2` VALUES IN (("1", NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p3` VALUES IN ((NULL, NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p4` VALUES IN ((NULL, MAXVALUE)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p5` VALUES IN ((MAXVALUE, NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p6` VALUES IN (("1", MAXVALUE)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
                sql """ ALTER TABLE `list_table_null` ADD PARTITION `p7` VALUES IN ((MAXVALUE, "1")) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs('FE.skipCheckMaxValueInListPartition')
            }

            def res = sql "show create table list_table_null"
            assertTrue(res[0][1].contains("PARTITION p4 VALUES IN ((NULL, MAXVALUE))"))
            assertTrue(res[0][1].contains("PARTITION p6 VALUES IN ((\"1\", MAXVALUE))"))
            assertTrue(res[0][1].contains("PARTITION p5 VALUES IN ((MAXVALUE, NULL))"))
            assertTrue(res[0][1].contains("PARTITION p7 VALUES IN ((MAXVALUE, \"1\"))"))

            // Insert into a table containing MAXVALUE list partitions should not fail.
            // (NULL, "1") -> p1, ("1", NULL) -> p2, (NULL, NULL) -> p3,
            // ("2", "2") matches no predefined partition and is auto-created since the table is AUTO.
            sql """ insert into list_table_null values (null, "1"), ("1", null), (null, null), ("2", "2") """

            order_qt_select_all """ select * from list_table_null order by id, k """

            // Predicate on the partition columns must not crash partition pruning:
            // partition keys containing MAXVALUE cannot be evaluated, they are kept conservatively.
            order_qt_select_with_predicate """ select * from list_table_null where id = 2 and k = 2 order by id, k """

            // 5. INSERT OVERWRITE on a legacy MAXVALUE table is rejected as well: the temp
            // partition swap clones the MAXVALUE partition key, which is forbidden at DDL time.
            test {
                sql "INSERT OVERWRITE TABLE `list_table_null` VALUES (null, \"1\");"
                exception "MAXVALUE is not allowed in LIST partition values"
            }
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
