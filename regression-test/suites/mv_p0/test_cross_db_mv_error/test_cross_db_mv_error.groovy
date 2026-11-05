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

// Regression test for DORIS-19133:
// Creating a sync materialized view must fail when the MV's database or catalog
// does not match the base table's database/catalog.

suite("test_cross_db_mv_error") {
    String baseDb = context.config.getDbNameByFile(context.file)
    String otherDb = baseDb + "_other"

    sql "DROP DATABASE IF EXISTS ${otherDb}"
    sql "DROP DATABASE IF EXISTS ${baseDb}"
    sql "CREATE DATABASE IF NOT EXISTS ${baseDb}"
    sql "CREATE DATABASE IF NOT EXISTS ${otherDb}"
    sql "USE ${baseDb}"

    sql "DROP TABLE IF EXISTS cross_db_test_t"
    sql """
        CREATE TABLE cross_db_test_t (
            id int null,
            k  largeint null,
            d  date null,
            k1 int
        )
        PARTITION BY RANGE (id) (
            PARTITION p1 VALUES [('1'), ('2'))
        )
        DISTRIBUTED BY HASH(k, id) BUCKETS 16
        PROPERTIES ("replication_num" = "1")
    """

    // Explicitly specifying a different db in the MV name should fail.
    test {
        sql """CREATE MATERIALIZED VIEW ${otherDb}.mv_cross_explicit AS SELECT d FROM ${baseDb}.cross_db_test_t"""
        exception "must be the same as the database"
    }

    // Using a session db that differs from the base table's db should fail.
    sql "USE ${otherDb}"
    test {
        sql """CREATE MATERIALIZED VIEW mv_cross_implicit AS SELECT d FROM ${baseDb}.cross_db_test_t"""
        exception "must be the same as the database"
    }

    // Specifying an external catalog in the MV name should fail.
    sql "USE ${baseDb}"
    test {
        sql """CREATE MATERIALIZED VIEW hive_catalog.${baseDb}.mv_cross_ctl AS SELECT d FROM cross_db_test_t"""
        exception "internal catalog"
    }

    // Creating a sync MV in the correct db must succeed.
    sql "DROP MATERIALIZED VIEW IF EXISTS mv_cross_ok ON cross_db_test_t"
    sql """CREATE MATERIALIZED VIEW ${baseDb}.mv_cross_ok AS SELECT d FROM cross_db_test_t"""

    sql "DROP DATABASE IF EXISTS ${otherDb}"
}
