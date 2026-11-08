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

suite("test_short_circuit_rowset_hidden_columns", "p0") {
    // Queries reading hidden columns do not use the short circuit: it used to return 0 for
    // __DORIS_VERSION_COL__ and __DORIS_COMMIT_TSO_COL__ instead of version 2 and the commit TSO.
    sql "DROP TABLE IF EXISTS short_circuit_rowset_hidden_columns"
    sql """
        CREATE TABLE short_circuit_rowset_hidden_columns (
            `id` INT NOT NULL,
            `v` INT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true",
            "changelog.enable" = "true"
        )
    """
    sql "INSERT INTO short_circuit_rowset_hidden_columns VALUES (1, 7), (2, 8)"

    explain {
        sql "SELECT __DORIS_VERSION_COL__, v FROM short_circuit_rowset_hidden_columns WHERE id = 1"
        notContains "SHORT-CIRCUIT"
    }
    qt_version "SELECT __DORIS_VERSION_COL__, v FROM short_circuit_rowset_hidden_columns WHERE id = 1"

    explain {
        sql "SELECT __DORIS_COMMIT_TSO_COL__ > 0, v FROM short_circuit_rowset_hidden_columns WHERE id = 1"
        notContains "SHORT-CIRCUIT"
    }
    qt_commit_tso "SELECT __DORIS_COMMIT_TSO_COL__ > 0, v FROM short_circuit_rowset_hidden_columns WHERE id = 1"

    // show_hidden_columns makes `select *` return hidden columns as well.
    sql "SET show_hidden_columns = true"
    explain {
        sql "SELECT * FROM short_circuit_rowset_hidden_columns WHERE id = 1"
        notContains "SHORT-CIRCUIT"
    }
    sql "SET show_hidden_columns = false"

    // Queries that do not read hidden columns keep the short circuit.
    explain {
        sql "SELECT * FROM short_circuit_rowset_hidden_columns WHERE id = 1"
        contains "SHORT-CIRCUIT"
    }
    qt_select_star "SELECT * FROM short_circuit_rowset_hidden_columns WHERE id = 1"
}
