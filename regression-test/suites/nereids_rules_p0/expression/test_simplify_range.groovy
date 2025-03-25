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

suite('test_simplify_range') {
    def tbl_1 = 'test_simplify_range_tbl_1'
    sql "set disable_nereids_rules='PRUNE_EMPTY_PARTITION'"

    sql "DROP TABLE IF EXISTS  ${tbl_1} FORCE"
    sql "CREATE TABLE ${tbl_1}(a DECIMAL(16,8), b INT) PROPERTIES ('replication_num' = '1')"
    sql "INSERT INTO ${tbl_1} VALUES(null, 10)"
    test {
        sql "SELECT a BETWEEN 100.02 and 40.123 OR a IN (54.0402) AND b < 10 FROM ${tbl_1}"
        result([[null]])
    }
    sql "DROP TABLE IF EXISTS  ${tbl_1} FORCE"

    sql """
         create table ${tbl_1}
        (
            pk_id                   varchar(32) not null,
            collect_time            varchar(16),
            item_id                 bigint      null,
        )
        unique key(pk_id, collect_time)
        auto partition by list(collect_time)()
        distributed by hash(pk_id) buckets auto
        properties (
            "replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true"
        );
        """

    // may cause dead loop because simplify range will output new in-predicate for options in new order.
    // test the in predicate again.
    test {
        sql """SELECT *
            FROM ${tbl_1}
            WHERE  ITEM_ID IN
                (1595012731474280448, 1595013220588847104, 1595013220634984448, 1595013220672733184, 1595013220718870528,
                1595013220760813568, 1595013220798562304, 1595013220832116736, 1595013220869865472, 1595013220911808512,
                1595013220945362944, 1595013220983111680, 1595013221025054720, 1595013221066997760, 1595013221104746496,
                1595013221146689536, 1595013221188632576, 1595013221222187008, 1595013221255741440, 1595013221289295872)
            AND COLLECT_TIME IN ('2025-02-09','2025-02-10','2025-02-11') """
        result([])
    }

    sql "DROP TABLE IF EXISTS  ${tbl_1} FORCE"
}
