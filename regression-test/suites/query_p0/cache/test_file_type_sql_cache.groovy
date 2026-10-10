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

// Exercise the real FE/BE SQL result cache, including a verified cache plan.
// Internal six-child payload retention is covered separately by QueryCache BE tests.
suite("test_file_type_sql_cache") {
    def publicStruct = "STRUCT<uri:VARCHAR(65533),offset:BIGINT,size:BIGINT," +
            "content_type:VARCHAR(1024),checksum:VARCHAR(1024),inline:VARBINARY>"
    def hasSqlCache = { String statement ->
        sql("EXPLAIN PHYSICAL PLAN " + statement).collect { it[0].toString() }
                .join("\n").contains("PhysicalSqlCache")
    }
    def primeSqlCache = { String statement ->
        for (int attempt = 0; attempt < 60; ++attempt) {
            sql statement
            if (hasSqlCache(statement)) {
                return
            }
            sleep(1000)
        }
        throw new IllegalStateException("FILE query did not populate the SQL result cache: " + statement)
    }

    withGlobalLock("cache_last_version_interval_second") {
        def originalInterval = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'cache_last_version_interval_second'")[0][1]
        try {
            sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"
            sql "SET enable_sql_cache = true"
            sql "DROP TABLE IF EXISTS test_file_type_sql_cache_values"
            sql """
                CREATE TABLE test_file_type_sql_cache_values (id INT NOT NULL, f FILE)
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1")
            """
            sql """
                INSERT INTO test_file_type_sql_cache_values VALUES
                (1, CAST(JSON_PARSE('{"uri":"urn:cache:one","offset":2,"size":5,
                    "content_type":"text/plain","checksum":"CRC32:00000000","inline":null}') AS FILE)),
                (2, NULL)
            """
            def statement = """
                SELECT id, f,
                       CAST(ARRAY(CAST(f AS ${publicStruct}), NULL) AS ARRAY<FILE>) AS files,
                       CAST(NAMED_STRUCT('f', CAST(f AS ${publicStruct})) AS STRUCT<f:FILE>) AS holder,
                       CAST(MAP('source', CAST(f AS ${publicStruct})) AS MAP<STRING,FILE>) AS lookup
                FROM test_file_type_sql_cache_values ORDER BY id
            """
            qt_uncached statement
            primeSqlCache(statement)
            qt_cached statement

            sql """
                INSERT INTO test_file_type_sql_cache_values VALUES
                (3, CAST(JSON_PARSE('{"uri":"urn:cache:three","size":0,"offset":null,"content_type":null,"checksum":null,"inline":null}') AS FILE))
            """
            assertFalse(hasSqlCache(statement), "A new table version must invalidate the cached FILE rows")
            qt_after_insert statement
            primeSqlCache(statement)
            qt_recached statement
        } finally {
            sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '${originalInterval}')"
        }
    }
}
