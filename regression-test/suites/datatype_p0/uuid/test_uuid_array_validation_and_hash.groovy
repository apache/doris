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

suite("test_uuid_array_validation_and_hash", "p0") {
    sql "DROP TABLE IF EXISTS uuid_array_mixed_types"
    sql """CREATE TABLE uuid_array_mixed_types (
               id INT, uuids ARRAY<UUID>, integers ARRAY<INT>, strings ARRAY<STRING>
           ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num' = '1')"""
    sql """INSERT INTO uuid_array_mixed_types VALUES
               (1, ARRAY(CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID)), ARRAY(1),
                   ARRAY('00112233-4455-6677-8899-aabbccddeeff')),
               (2, [], [], []),
               (3, [NULL], [NULL], [NULL]),
               (4, NULL, NULL, NULL)"""

    test {
        sql "SELECT arrays_overlap(uuids, integers) FROM uuid_array_mixed_types"
        exception "Cannot find a common type for indexed ANY arguments"
    }
    test {
        sql "SELECT arrays_overlap(integers, uuids) FROM uuid_array_mixed_types"
        exception "Cannot find a common type for indexed ANY arguments"
    }

    order_qt_compatible_arrays """SELECT id,
               arrays_overlap(uuids, uuids), arrays_overlap(uuids, ARRAY(NULL)),
               arrays_overlap(ARRAY(NULL), uuids), arrays_overlap(uuids, strings),
               arrays_overlap(strings, uuids)
           FROM uuid_array_mixed_types"""

    sql "DROP TABLE IF EXISTS uuid_array_same_low_bits"
    sql """CREATE TABLE uuid_array_same_low_bits (
               id INT, uuids ARRAY<UUID>, disjoint ARRAY<UUID>
           ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
           PROPERTIES('replication_num' = '1')"""
    // All 16,384 distinct UUIDs in each array share the same low 64 bits.
    sql """INSERT INTO uuid_array_same_low_bits
           SELECT 1,
               array_agg(CAST(CONCAT(LPAD(HEX(number + 1), 8, '0'),
                   '-1234-1234-9234-001122334455') AS UUID)),
               array_agg(CAST(CONCAT(LPAD(HEX(number + 16385), 8, '0'),
                   '-1234-1234-9234-001122334455') AS UUID))
           FROM numbers('number' = '16384')"""

    order_qt_same_low_bits """SELECT id, size(array_distinct(uuids)),
               size(array_distinct(array_concat(uuids, uuids,
                   ARRAY(CAST(NULL AS UUID), CAST(NULL AS UUID))))),
               arrays_overlap(uuids, disjoint), arrays_overlap(uuids, uuids)
           FROM uuid_array_same_low_bits"""

    order_qt_same_low_bits_set_functions """SELECT id,
               size(array_union(uuids, uuids)), size(array_union(uuids, disjoint)),
               size(array_intersect(uuids, uuids)), size(array_intersect(uuids, disjoint)),
               size(array_except(uuids, uuids)), size(array_except(uuids, disjoint)),
               size(array_except_all(uuids, uuids)), size(array_except_all(uuids, disjoint)),
               size(array_except_all(array_concat(uuids, uuids), uuids))
           FROM uuid_array_same_low_bits"""
}
