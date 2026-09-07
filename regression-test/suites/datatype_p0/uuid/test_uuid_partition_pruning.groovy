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

// Checklist: B08 C05 C08.
suite("test_uuid_partition_pruning", "p0") {
    // UUID range/list partition bounds and hash-bucket constants are folded by FE.
    sql "DROP TABLE IF EXISTS uuid_range_partition"
    sql """
        CREATE TABLE uuid_range_partition (
            u UUID NOT NULL,
            payload INT
        ) DUPLICATE KEY(u)
        PARTITION BY RANGE(u) (
            PARTITION p0 VALUES LESS THAN ('40000000-0000-0000-0000-000000000000'),
            PARTITION p1 VALUES LESS THAN ('c0000000-0000-0000-0000-000000000000'),
            PARTITION p2 VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(u) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO uuid_range_partition VALUES
            ('00000000-0000-0000-0000-000000000001', 1),
            ('80000000-0000-0000-0000-000000000000', 2),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff', 3)
    """
    order_qt_uuid_range_partition_fold """
        SELECT payload FROM uuid_range_partition
        WHERE u = CAST(CONCAT('80000000-0000-0000-', '0000-000000000000') AS UUID)
        ORDER BY payload
    """
    explain {
        sql """
            verbose SELECT payload FROM uuid_range_partition
            WHERE u = CAST(CONCAT('80000000-0000-0000-', '0000-000000000000') AS UUID)
        """
        contains "PREDICATES: (u"
        contains "partitions=1/3 (p1)"
        contains "tablets=1/4"
    }

    sql "DROP TABLE IF EXISTS uuid_list_partition"
    sql """
        CREATE TABLE uuid_list_partition (
            u UUID NOT NULL,
            payload INT
        ) DUPLICATE KEY(u)
        PARTITION BY LIST(u) (
            PARTITION p0 VALUES IN ('00000000-0000-0000-0000-000000000001'),
            PARTITION p1 VALUES IN ('ffffffff-ffff-ffff-ffff-ffffffffffff')
        )
        DISTRIBUTED BY HASH(u) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO uuid_list_partition VALUES
            ('00000000-0000-0000-0000-000000000001', 1),
            ('ffffffff-ffff-ffff-ffff-ffffffffffff', 2)
    """
    order_qt_uuid_list_partition_fold """
        SELECT payload FROM uuid_list_partition
        WHERE u = CAST(CONCAT('ffffffff-ffff-ffff-', 'ffff-ffffffffffff') AS UUID)
        ORDER BY payload
    """
    explain {
        sql """
            verbose SELECT payload FROM uuid_list_partition
            WHERE u = CAST(CONCAT('ffffffff-ffff-ffff-', 'ffff-ffffffffffff') AS UUID)
        """
        contains "partitions=1/2 (p1)"
        contains "tablets=1/2"
    }

    // Include exact partition bounds and a value adjacent in the low 64 bits.
    sql """INSERT INTO uuid_range_partition VALUES
           ('40000000-0000-0000-0000-000000000000',4),
           ('c0000000-0000-0000-0000-000000000000',5),
           ('80000000-0000-0000-0000-000000000001',6)"""
    for (def item : [
            ["u >= '40000000-0000-0000-0000-000000000000' AND u < 'c0000000-0000-0000-0000-000000000000'", "partitions=1/3 (p1)"],
            ["u >= 'c0000000-0000-0000-0000-000000000000'", "partitions=1/3 (p2)"],
            ["u <= '40000000-0000-0000-0000-000000000000'", "partitions=2/3 (p0,p1)"],
            ["u IN (CAST('00000000000000000000000000000001' AS UUID),CAST(REPEAT('f',32) AS UUID))", "partitions=2/3 (p0,p2)"]]) {
        explain {
            sql "verbose SELECT payload FROM uuid_range_partition WHERE ${item[0]}"
            contains item[1]
        }
        qt_boundary "SELECT u,payload FROM uuid_range_partition WHERE ${item[0]} ORDER BY u,payload"
    }
}
