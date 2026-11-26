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

suite("test_uuid_partition", "p0") {
    sql "DROP TABLE IF EXISTS uuid_partition_rejected"
    for (partitionClause in [
            "PARTITION BY RANGE(u) ()",
            "PARTITION BY RANGE(u) (PARTITION p0 VALUES LESS THAN (MAXVALUE))",
            "PARTITION BY RANGE(u) (PARTITION p0 VALUES LESS THAN ('80000000-0000-0000-0000-000000000000'))",
            "PARTITION BY RANGE(u) (PARTITION p0 VALUES [('00000000-0000-0000-0000-000000000000'), ('80000000-0000-0000-0000-000000000000')))",
            "PARTITION BY RANGE(u,id) (PARTITION p0 VALUES LESS THAN (MAXVALUE,MAXVALUE))",
            "PARTITION BY RANGE(id,u) (PARTITION p0 VALUES LESS THAN (MAXVALUE,MAXVALUE))"]) {
        test {
            sql """CREATE TABLE uuid_partition_rejected (u UUID NOT NULL, id INT NOT NULL)
                   DUPLICATE KEY(u,id) ${partitionClause}
                   DISTRIBUTED BY HASH(u) BUCKETS 1 PROPERTIES('replication_num'='1')"""
            exception "Column[u] type[UUID] cannot be a range partition key."
        }
    }
    for (partitionClause in [
            "PARTITION BY LIST(u) ()",
            "PARTITION BY LIST(u) (PARTITION p0 VALUES IN ('00000000-0000-0000-0000-000000000001'))",
            "PARTITION BY LIST(u,id) (PARTITION p0 VALUES IN (('00000000-0000-0000-0000-000000000001','1')))",
            "PARTITION BY LIST(id,u) (PARTITION p0 VALUES IN (('1','00000000-0000-0000-0000-000000000001')))",
            "AUTO PARTITION BY LIST(u) ()",
            "AUTO PARTITION BY LIST(id,u) ()"]) {
        test {
            sql """CREATE TABLE uuid_partition_rejected (u UUID NOT NULL, id INT NOT NULL)
                   DUPLICATE KEY(u,id) ${partitionClause}
                   DISTRIBUTED BY HASH(u) BUCKETS 1 PROPERTIES('replication_num'='1')"""
            exception "Column[u] type[UUID] cannot be a list partition key."
        }
    }

    for (enableProperty in ['"dynamic_partition.enable" = "true",',
                            '"dynamic_partition.enable" = "false",', '']) {
        test {
            sql """CREATE TABLE uuid_partition_rejected (u UUID NOT NULL, id INT NOT NULL)
                   DUPLICATE KEY(u,id) PARTITION BY RANGE(u) ()
                   DISTRIBUTED BY HASH(u) BUCKETS 1
                   PROPERTIES (
                       "replication_num" = "1",
                       ${enableProperty}
                       "dynamic_partition.time_unit" = "DAY",
                       "dynamic_partition.start" = "-1",
                       "dynamic_partition.end" = "1",
                       "dynamic_partition.prefix" = "p",
                       "dynamic_partition.buckets" = "1"
                   )"""
            exception "Column[u] type[UUID] cannot be a range partition key."
        }
    }

    // UUID columns and UUID hash distribution remain valid in a table partitioned by date.
    sql "DROP TABLE IF EXISTS uuid_partition_date_control"
    sql """CREATE TABLE uuid_partition_date_control (u UUID NOT NULL, d DATE NOT NULL)
           DUPLICATE KEY(u,d) PARTITION BY RANGE(d) (
               PARTITION p0 VALUES LESS THAN ('2026-01-02')
           ) DISTRIBUTED BY HASH(u) BUCKETS 2 PROPERTIES('replication_num'='1')"""
    sql """ALTER TABLE uuid_partition_date_control
           ADD PARTITION p1 VALUES LESS THAN ('2026-01-03')"""
    sql """INSERT INTO uuid_partition_date_control VALUES
           ('00000000-0000-0000-0000-000000000001','2026-01-01'),
           ('ffffffff-ffff-ffff-ffff-ffffffffffff','2026-01-02')"""
    order_qt_date_partitions "SELECT u,d FROM uuid_partition_date_control"
    order_qt_uuid_hash """SELECT u,d FROM uuid_partition_date_control
                         WHERE u = 'ffffffff-ffff-ffff-ffff-ffffffffffff'"""
}
