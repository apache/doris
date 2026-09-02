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

import org.apache.doris.regression.util.JdbcUtils

// return_object_data_as_binary decides whether the BE's MySQL result writer serializes HLL /
// BITMAP / QUANTILE_STATE as their raw bytes or as NULL, so it changes the very rows the sql cache
// stores. It must therefore take part in the cache key comparison
// (NereidsSqlCacheManager.usedVariablesChanged over SessionVariable.affectQueryResultFields),
// otherwise a session that turns it on replays the NULLs cached by a session that had it off.
suite("sql_cache_object_type") {
    def conn = context.getConn()
    def run = { String stmt ->
        def (result, meta) = JdbcUtils.executeToList(conn, stmt)
        return result
    }
    def hasSqlCache = { String stmt ->
        def (rows, meta) = JdbcUtils.executeToList(conn, "explain physical plan " + stmt)
        return rows.collect { row -> row.get(0).toString() }.join("\n").contains("PhysicalSqlCache")
    }
    def primeSqlCache = { String stmt ->
        for (int i = 0; i < 60; ++i) {
            run(stmt)
            if (hasSqlCache(stmt)) {
                return
            }
            sleep(1000)
        }
        throw new IllegalStateException("failed to create sql cache for: " + stmt)
    }
    def isNonEmpty = { value ->
        if (value == null) {
            return false
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).length > 0
        }
        return !value.toString().isEmpty()
    }

    withGlobalLock("cache_last_version_interval_second") {
        run "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"
        run "set enable_sql_cache=true"

        def tblName = "sql_cache_object_type_tbl"
        run "DROP TABLE IF EXISTS ${tblName}"
        run """
            CREATE TABLE ${tblName} (
                k INT,
                h HLL HLL_UNION,
                b BITMAP BITMAP_UNION
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        run "INSERT INTO ${tblName} SELECT 1, HLL_HASH('x'), TO_BITMAP(1)"

        def objectSql = "select h, b from ${tblName}"

        // With the default (false) the object columns come back as NULL, and that is what lands in
        // the cache.
        run "set return_object_data_as_binary=false"
        primeSqlCache(objectSql)
        def asNull = run(objectSql)
        assertEquals(1, asNull.size())
        assertNull(asNull[0][0])
        assertNull(asNull[0][1])

        // Turning it on must not be served the cached NULLs: it is a different result, so it is a
        // different cache key and the query has to be executed again.
        run "set return_object_data_as_binary=true"
        assertFalse(hasSqlCache(objectSql),
                "return_object_data_as_binary=true must not reuse the entry cached with it off")
        def asBinary = run(objectSql)
        assertEquals(1, asBinary.size())
        assertTrue(isNonEmpty(asBinary[0][0]),
                "expect the raw HLL bytes, but got: " + asBinary[0][0])
        assertTrue(isNonEmpty(asBinary[0][1]),
                "expect the raw BITMAP bytes, but got: " + asBinary[0][1])

        // The two settings keep their own entries, and each still serves its own result.
        primeSqlCache(objectSql)
        def asBinaryCached = run(objectSql)
        assertTrue(isNonEmpty(asBinaryCached[0][0]))
        assertTrue(isNonEmpty(asBinaryCached[0][1]))

        run "set return_object_data_as_binary=false"
        assertTrue(hasSqlCache(objectSql))
        def asNullAgain = run(objectSql)
        assertNull(asNullAgain[0][0])
        assertNull(asNullAgain[0][1])
    }
}
