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

// DDL of the GLOBAL_POINT index: creation, validation, light index change.
suite("test_global_point_index_ddl") {
    def indexTypesOf = { tableName ->
        def rows = sql "SHOW INDEX FROM ${tableName}"
        // Key_name is column 2, Index_type column 10.
        return rows.collectEntries { row -> [(row[2]): row[10]] }
    }

    def tbl = "test_global_point_index_ddl"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            id BIGINT NOT NULL,
            ev INT NULL,
            name VARCHAR(64) NULL,
            dt DATE NULL,
            INDEX idx_ev (ev) USING GLOBAL_POINT PROPERTIES ("fpp" = "0.001")
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """
    assertEquals("GLOBAL_POINT", indexTypesOf(tbl)["idx_ev"])

    // CREATE INDEX and ALTER TABLE ADD INDEX keep the type (light index change, no data rewrite).
    sql "SET enable_add_index_for_new_data = true"
    sql "CREATE INDEX idx_name ON ${tbl}(name) USING GLOBAL_POINT"
    sql "ALTER TABLE ${tbl} ADD INDEX idx_dt (dt) USING GLOBAL_POINT"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tbl}" ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }
    def types = indexTypesOf(tbl)
    assertEquals("GLOBAL_POINT", types["idx_name"])
    assertEquals("GLOBAL_POINT", types["idx_dt"])

    // One GLOBAL_POINT index per column.
    test {
        sql "CREATE INDEX idx_ev2 ON ${tbl}(ev) USING GLOBAL_POINT"
        exception "GLOBAL_POINT index for column (ev) already exists"
    }

    sql "DROP INDEX idx_dt ON ${tbl}"
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${tbl}" ORDER BY CreateTime DESC LIMIT 1"""
        time 120
    }
    assertFalse(indexTypesOf(tbl).containsKey("idx_dt"))

    // Only types whose equality is byte equality are accepted.
    def badTbl = "test_global_point_index_ddl_bad"
    for (def colType : ["DOUBLE", "DECIMAL(10, 2)", "ARRAY<INT>", "JSON"]) {
        sql "DROP TABLE IF EXISTS ${badTbl}"
        test {
            sql """
                CREATE TABLE ${badTbl} (
                    id BIGINT NOT NULL,
                    c ${colType} NULL,
                    INDEX idx_c (c) USING GLOBAL_POINT
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "is not supported in GLOBAL_POINT index"
        }
    }

    // fpp must be in [1e-6, 0.5], and it is the only property.
    for (def props : ['"fpp" = "0.9"', '"fpp" = "abc"', '"gram_size" = "3"']) {
        sql "DROP TABLE IF EXISTS ${badTbl}"
        test {
            sql """
                CREATE TABLE ${badTbl} (
                    id BIGINT NOT NULL,
                    c VARCHAR(32) NULL,
                    INDEX idx_c (c) USING GLOBAL_POINT PROPERTIES (${props})
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
            exception "GLOBAL_POINT"
        }
    }

    // A GLOBAL_POINT index covers one column.
    sql "DROP TABLE IF EXISTS ${badTbl}"
    test {
        sql """
            CREATE TABLE ${badTbl} (
                id BIGINT NOT NULL,
                a INT NULL,
                b INT NULL,
                INDEX idx_ab (a, b) USING GLOBAL_POINT
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        exception "can only apply to a single column"
    }

    if (!isCloudMode()) {
        test {
            sql "BUILD INDEX idx_ev ON ${tbl}"
            exception "only supported in cloud mode"
        }
    }

    sql "DROP TABLE IF EXISTS ${badTbl}"
    sql "DROP TABLE IF EXISTS ${tbl}"
}
