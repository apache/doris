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

// A predicate that references no column, such as random() < 2, is not pushed into the storage
// layer and stays in the scanner. COUNT_ON_INDEX must still count with it instead of failing.
suite("test_count_on_index_slotless_predicate", "p0") {
    def tbl = "test_count_on_index_slotless_predicate"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            k INT,
            v STRING,
            INDEX idx_v (v) USING INVERTED PROPERTIES ('parser' = 'english')
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql "INSERT INTO ${tbl} VALUES (1, 'apple pie'), (2, 'banana'), (3, 'apple juice')"

    sql "SET enable_count_on_index_pushdown = true"
    explain {
        sql "SELECT count(*) FROM ${tbl} WHERE random() < 2"
        contains "pushAggOp=COUNT_ON_INDEX"
    }
    explain {
        sql "SELECT count(*) FROM ${tbl} WHERE v MATCH_ANY 'apple' AND random() < 2"
        contains "pushAggOp=COUNT_ON_INDEX"
    }

    // random() returns a value in [0, 1), so "< 2" keeps every row and "< 0" keeps none.
    qt_all_rows "SELECT count(*) FROM ${tbl} WHERE random() < 2"
    qt_no_rows "SELECT count(*) FROM ${tbl} WHERE random() < 0"
    qt_match_all_rows "SELECT count(*) FROM ${tbl} WHERE v MATCH_ANY 'apple' AND random() < 2"
    qt_match_no_rows "SELECT count(*) FROM ${tbl} WHERE v MATCH_ANY 'apple' AND random() < 0"
}
