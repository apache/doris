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

suite("test_olap_static_partition_rejected") {
    sql "DROP TABLE IF EXISTS test_olap_static_partition_rejected"
    sql """
        CREATE TABLE test_olap_static_partition_rejected (
            id INT,
            dt DATE NOT NULL
        ) DUPLICATE KEY(id)
        PARTITION BY RANGE(dt) (
            PARTITION p20261104 VALUES [('2026-11-04'), ('2026-11-05')),
            PARTITION p20261105 VALUES [('2026-11-05'), ('2026-11-06'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_olap_static_partition_rejected VALUES (4, '2026-11-04'), (5, '2026-11-05')"

    test {
        sql "SELECT * FROM test_olap_static_partition_rejected PARTITION(`dt`='2026-11-04')"
        exception "mismatched input '='"
    }
    test {
        sql """INSERT INTO test_olap_static_partition_rejected PARTITION(`dt`='2026-11-04')
               VALUES (40, '2026-11-04')"""
        exception "Static partition syntax PARTITION (column=value) is not supported for OLAP tables"
    }
    test {
        sql """INSERT INTO test_olap_static_partition_rejected PARTITION(`dt`='2026-11-04')
               SELECT 40, CAST('2026-11-05' AS DATE)"""
        exception "Static partition syntax PARTITION (column=value) is not supported for OLAP tables"
    }
    test {
        sql """INSERT OVERWRITE TABLE test_olap_static_partition_rejected PARTITION(`dt`='2026-11-04')
               SELECT 40, CAST('2026-11-04' AS DATE)"""
        exception "Static partition syntax PARTITION (column=value) is not supported for OLAP tables"
    }
    test {
        sql """INSERT OVERWRITE TABLE test_olap_static_partition_rejected PARTITION(`dt`='2026-11-04')
               SELECT id, dt FROM test_olap_static_partition_rejected WHERE 1 = 0"""
        exception "Static partition syntax PARTITION (column=value) is not supported for OLAP tables"
    }
    order_qt_after_rejected_statements "SELECT * FROM test_olap_static_partition_rejected"

    sql """INSERT INTO test_olap_static_partition_rejected PARTITION(p20261104)
           VALUES (41, '2026-11-04')"""
    order_qt_insert_by_name "SELECT * FROM test_olap_static_partition_rejected"

    sql """INSERT OVERWRITE TABLE test_olap_static_partition_rejected PARTITION(p20261104)
           SELECT 42, CAST('2026-11-04' AS DATE)"""
    order_qt_overwrite_by_name "SELECT * FROM test_olap_static_partition_rejected"

    sql """INSERT OVERWRITE TABLE test_olap_static_partition_rejected PARTITION(*)
           SELECT 43, CAST('2026-11-04' AS DATE)"""
    order_qt_overwrite_auto_detect "SELECT * FROM test_olap_static_partition_rejected"

    sql """INSERT OVERWRITE TABLE test_olap_static_partition_rejected
           SELECT 44, CAST('2026-11-04' AS DATE)"""
    order_qt_overwrite_whole_table "SELECT * FROM test_olap_static_partition_rejected"
}
