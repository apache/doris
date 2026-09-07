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

// Checklist: B07 B08 B09 C08 F06 F16.
suite("test_uuid_composite_partition", "p0") {
    // Composite UUID partition/short keys distinguish adjacent values with the same prefix.
    sql "DROP TABLE IF EXISTS uuid_storage_partition"
    sql """CREATE TABLE uuid_storage_partition (u UUID NOT NULL, id INT NOT NULL, v INT)
           DUPLICATE KEY(u,id) PARTITION BY RANGE(u,id) (
             PARTITION p0 VALUES LESS THAN ('80000000-0000-0000-0000-000000000000', '2'),
             PARTITION p1 VALUES LESS THAN (MAXVALUE, MAXVALUE)
           ) DISTRIBUTED BY HASH(u,id) BUCKETS 3 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO uuid_storage_partition VALUES
           ('7fffffff-ffff-ffff-ffff-ffffffffffff',1,1),
           ('80000000-0000-0000-0000-000000000000',1,2),
           ('80000000-0000-0000-0000-000000000000',2,3),
           ('80000000-0000-0000-0000-000000000001',1,4)"""
    sql "INSERT INTO uuid_storage_partition SELECT * FROM uuid_storage_partition"
    qt_partition_before "SELECT * FROM uuid_storage_partition ORDER BY u,id,v"
    qt_partition_p0 "SELECT * FROM uuid_storage_partition PARTITION(p0) ORDER BY u,id,v"
    if (!isCloudMode()) {
        trigger_and_wait_compaction("uuid_storage_partition", "full")
    }
    qt_partition_after "SELECT * FROM uuid_storage_partition ORDER BY u,id,v"
    qt_partition_range """SELECT * FROM uuid_storage_partition
                          WHERE u >= '80000000-0000-0000-0000-000000000000' AND id >= 2 ORDER BY u,id,v"""

    // The generic composite RANGE evaluator retains both partitions at the exact tuple
    // boundary (also reproducible with BIGINT). Record the conservative fallback separately
    // from the strict first-column range below, which must actually prune.
    qt_exact_boundary """SELECT * FROM uuid_storage_partition
                         WHERE u = '80000000-0000-0000-0000-000000000000' AND id = 2 ORDER BY u,id,v"""
    explain {
        sql "verbose SELECT * FROM uuid_storage_partition WHERE u = '80000000-0000-0000-0000-000000000000' AND id = 2"
        contains "partitions=2/2 (p0,p1)"
        contains "tablets=2/6"
    }
    explain {
        sql "verbose SELECT * FROM uuid_storage_partition WHERE u = '80000000-0000-0000-0000-000000000000'"
        contains "partitions=2/2"
        contains "tablets=6/6"
    }

    explain {
        sql "verbose SELECT * FROM uuid_storage_partition WHERE u > '80000000-0000-0000-0000-000000000000'"
        contains "partitions=1/2 (p1)"
        contains "tablets=3/3"
    }
    qt_strict_range "SELECT * FROM uuid_storage_partition WHERE u > '80000000-0000-0000-0000-000000000000' ORDER BY u,id,v"
}
