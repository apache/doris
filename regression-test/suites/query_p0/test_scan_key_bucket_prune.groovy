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

suite("test_scan_key_bucket_prune") {
    sql "DROP TABLE IF EXISTS scan_key_bucket_dup"
    sql """CREATE TABLE scan_key_bucket_dup (
        address VARCHAR(80) NOT NULL, chain INT NOT NULL, payload STRING
    ) DUPLICATE KEY(address) DISTRIBUTED BY HASH(address) BUCKETS 128
    PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO scan_key_bucket_dup
        SELECT concat('address', lpad(cast(number AS STRING), 6, '0')), number % 3,
               concat('payload', cast(number AS STRING)) FROM numbers("number" = "1000")"""
    sql "INSERT INTO scan_key_bucket_dup VALUES ('', 1, 'empty'), ('中文', 1, 'unicode'), ('address000001', 2, 'duplicate')"

    sql "DROP TABLE IF EXISTS scan_key_bucket_mow"
    sql """CREATE TABLE scan_key_bucket_mow (
        address VARCHAR(80) NOT NULL, chain INT NOT NULL, payload STRING
    ) UNIQUE KEY(address) DISTRIBUTED BY HASH(address) BUCKETS 128
    PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")"""
    sql "INSERT INTO scan_key_bucket_mow SELECT * FROM scan_key_bucket_dup WHERE payload != 'duplicate'"
    sql "INSERT INTO scan_key_bucket_mow VALUES ('address000001', 2, 'updated'), ('address000002', 1, 'updated2')"
    sql "DELETE FROM scan_key_bucket_mow WHERE address = 'address000004'"

    def keys = (0..<55).collect { "'address${String.format('%06d', it * 17)}'" }.join(',')
    keys += ", '', '中文', 'missing', NULL, 'address000001', 'address000002', 'address000004'"
    [false, true].each { prune ->
        sql "SET enable_scan_key_bucket_prune = ${prune}"
        [false, true].each { parallel ->
            sql "SET enable_parallel_scan = ${parallel}"
            [1, 1024].each { cap ->
                // cap=1 coalesces IN keys to a range: routing must fall back.
                sql "SET max_scan_key_num = ${cap}"
                def label = "${prune}_${parallel}_${cap}"
                "order_qt_dup_${label}"("SELECT * FROM scan_key_bucket_dup WHERE address IN (${keys}) AND chain IN (1, 2)")
                "order_qt_mow_${label}"("SELECT * FROM scan_key_bucket_mow WHERE address IN (${keys}) AND chain IN (1, 2)")
                "order_qt_range_${label}"("SELECT * FROM scan_key_bucket_dup WHERE address > 'address000002' AND address < 'address000010'")
                "qt_full_${label}"("SELECT count(*), sum(chain) FROM scan_key_bucket_dup")
            }
        }
    }
    sql "SET enable_scan_key_bucket_prune = true"
    sql "SET max_scan_key_num = 1024"
    sql "DROP TABLE IF EXISTS scan_key_bucket_nullable"
    sql """CREATE TABLE scan_key_bucket_nullable (address VARCHAR(80) NULL, chain INT)
        DUPLICATE KEY(address) DISTRIBUTED BY HASH(address) BUCKETS 8
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO scan_key_bucket_nullable VALUES (NULL, 0), ('a', 1), ('b', 2)"
    order_qt_nullable "SELECT * FROM scan_key_bucket_nullable WHERE address IN ('a', 'b', NULL) OR address IS NULL"

    sql "DROP TABLE IF EXISTS scan_key_bucket_composite"
    sql """CREATE TABLE scan_key_bucket_composite (address VARCHAR(80) NOT NULL, chain INT NOT NULL, v INT)
        DUPLICATE KEY(address, chain) DISTRIBUTED BY HASH(address, chain) BUCKETS 8
        PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO scan_key_bucket_composite VALUES ('a', 1, 1), ('a', 2, 2), ('b', 3, 3)"
    order_qt_composite "SELECT * FROM scan_key_bucket_composite WHERE address IN ('a', 'b') AND chain IN (1, 3)"

    sql "DROP TABLE IF EXISTS scan_key_bucket_partitioned"
    sql """CREATE TABLE scan_key_bucket_partitioned (address VARCHAR(80) NOT NULL, chain INT NOT NULL)
        DUPLICATE KEY(address) PARTITION BY RANGE(chain) (PARTITION p1 VALUES LESS THAN ('2'), PARTITION p2 VALUES LESS THAN (MAXVALUE))
        DISTRIBUTED BY HASH(address) BUCKETS 8 PROPERTIES("replication_num" = "1")"""
    sql "INSERT INTO scan_key_bucket_partitioned VALUES ('a', 1), ('b', 2), ('c', 3)"
    order_qt_partitioned "SELECT * FROM scan_key_bucket_partitioned WHERE address IN ('a', 'b', 'c') AND chain != 2"
}
