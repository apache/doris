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

// INVERTED index is not supported on AGG_KEYS tables or merge-on-read UNIQUE_KEYS tables,
// even on key columns. score() (which relies on an inverted index) is likewise rejected on
// such tables. This suite verifies that both CREATE TABLE and ALTER TABLE ADD INDEX are
// blocked, and that score() reports a clear error.
suite("test_inverted_index_agg_mor_forbidden", "p0") {
    def aggTbl = "test_inv_forbidden_agg"
    def morTbl = "test_inv_forbidden_mor"
    def mowTbl = "test_inv_forbidden_mow"

    sql "DROP TABLE IF EXISTS ${aggTbl}"
    sql "DROP TABLE IF EXISTS ${morTbl}"
    sql "DROP TABLE IF EXISTS ${mowTbl}"

    // ---- AGG_KEYS: inverted index forbidden on key column at CREATE ----
    test {
        sql """
            CREATE TABLE ${aggTbl} (
                k1 INT,
                k2 VARCHAR(30),
                v1 INT SUM,
                INDEX idx_k2 (k2) USING INVERTED
            )
            AGGREGATE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1");
        """
        exception "INVERTED index is not supported on AGG_KEYS table"
    }

    // ---- AGG_KEYS: inverted index forbidden via ALTER, on both key and value columns ----
    sql """
        CREATE TABLE ${aggTbl} (
            k1 INT,
            k2 VARCHAR(30),
            v1 INT SUM
        )
        AGGREGATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """
    test {
        sql "ALTER TABLE ${aggTbl} ADD INDEX idx_k2 (k2) USING INVERTED;"
        exception "INVERTED index is not supported on AGG_KEYS table"
    }
    test {
        sql "ALTER TABLE ${aggTbl} ADD INDEX idx_v1 (v1) USING INVERTED;"
        exception "INVERTED index is not supported on AGG_KEYS table"
    }

    // ---- merge-on-read UNIQUE_KEYS: inverted index forbidden on key column at CREATE ----
    test {
        sql """
            CREATE TABLE ${morTbl} (
                k1 INT,
                k2 VARCHAR(30),
                v1 INT,
                INDEX idx_k2 (k2) USING INVERTED
            )
            UNIQUE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false"
            );
        """
        exception "merge-on-read UNIQUE_KEYS table"
    }

    // ---- merge-on-read UNIQUE_KEYS: inverted index forbidden via ALTER on value column ----
    sql """
        CREATE TABLE ${morTbl} (
            k1 INT,
            k2 VARCHAR(30),
            v1 VARCHAR(30)
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        );
    """
    test {
        sql "ALTER TABLE ${morTbl} ADD INDEX idx_v1 (v1) USING INVERTED;"
        exception "merge-on-read UNIQUE_KEYS table"
    }

    // ---- merge-on-write UNIQUE_KEYS: inverted index still allowed (sanity check) ----
    sql """
        CREATE TABLE ${mowTbl} (
            k1 INT,
            k2 VARCHAR(30),
            v1 VARCHAR(30),
            INDEX idx_v1 (v1) USING INVERTED
        )
        UNIQUE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // ---- score() is rejected on AGG / merge-on-read tables ----
    // The AGG/MOR tables above carry no inverted index (it is forbidden). The score() push-down
    // rule rejects the table type as soon as it sees score(), before the MATCH requirement, so a
    // plain filter is enough to reach that check.
    test {
        sql "SELECT score() AS s FROM ${aggTbl} WHERE k1 = 1 ORDER BY s LIMIT 10;"
        exception "score() function is not supported on AGG_KEYS table or merge-on-read UNIQUE_KEYS table"
    }
    test {
        sql "SELECT score() AS s FROM ${morTbl} WHERE k1 = 1 ORDER BY s LIMIT 10;"
        exception "score() function is not supported on AGG_KEYS table or merge-on-read UNIQUE_KEYS table"
    }

    sql "DROP TABLE IF EXISTS ${aggTbl}"
    sql "DROP TABLE IF EXISTS ${morTbl}"
    sql "DROP TABLE IF EXISTS ${mowTbl}"
}
