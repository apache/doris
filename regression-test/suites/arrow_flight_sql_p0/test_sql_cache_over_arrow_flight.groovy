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

// Regression for https://github.com/apache/doris/issues/67364
//
// The FE sql cache is shared by every protocol, but its rows are MySQL wire protocol packets that
// StmtExecutor.sendCachedValues replays through a MysqlChannel, and an Arrow Flight SQL connection
// has none. Replaying an entry created by an identical MySQL query used to fail
// Preconditions.checkState(connectType == MYSQL) in StmtExecutor.sendFields() and reach the client
// as "INTERNAL ... IllegalStateException, msg: null", for any result type. The issue was reported
// on raw HLL / QUANTILE_STATE columns only because that sql text happened to be the one primed
// through the MySQL control session.
//
// Two setup details decide whether this test can reproduce the bug at all -- get either wrong and
// it stays green on a broken FE:
//
//  1. The flight statements are sent on the raw flight connection. Suite.arrow_flight_sql()
//     prepends "USE <db>;" to the statement, which changes the sql text and therefore the cache
//     key (NereidsSqlCacheManager.generateCacheKey is "<catalog>.<db>:<user>:<sql text>").
//  2. Both sessions must agree on every session variable the cache compares
//     (NereidsSqlCacheManager.usedVariablesChanged compares the whole affectQueryResult* set).
//     The MySQL JDBC driver adds STRICT_TRANS_TABLES to sql_mode at connect time while the Arrow
//     Flight JDBC driver does not, and sql_mode is affectQueryResultInPlan, so an unaligned
//     sql_mode alone makes every flight lookup miss.
suite("test_sql_cache_over_arrow_flight") {
    def mysqlConn = context.getConn()
    def flightConn = context.getArrowFlightSqlConnection()

    def runOnMysql = { String stmt ->
        def (result, meta) = JdbcUtils.executeToList(mysqlConn, stmt)
        return result
    }
    def runOnFlight = { String stmt ->
        def (result, meta) = JdbcUtils.executeToList(flightConn, stmt)
        return result
    }

    def hasSqlCache = { String stmt ->
        def (explainRows, meta) = JdbcUtils.executeToList(mysqlConn, "explain physical plan " + stmt)
        return explainRows.collect { row -> row.get(0).toString() }.join("\n").contains("PhysicalSqlCache")
    }

    // Create the cache entry on the MySQL connection, and wait until an identical statement is
    // actually served from it, so the flight query below really runs against a populated cache.
    def primeSqlCacheOnMysql = { String stmt ->
        for (int i = 0; i < 60; ++i) {
            runOnMysql(stmt)
            if (hasSqlCache(stmt)) {
                return
            }
            sleep(1000)
        }
        throw new IllegalStateException("failed to create sql cache for: " + stmt)
    }

    // JdbcUtils renders a binary column as an "0x.." hex string, but falls back to the raw object
    // when the driver does not implement getBytes().
    def isNonEmptyBinary = { value ->
        if (value == null) {
            return false
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).length > 0
        }
        return value.toString().length() > "0x".length()
    }

    withGlobalLock("cache_last_version_interval_second") {
        runOnMysql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"

        def dbName = context.dbName
        runOnMysql "USE `${dbName}`"
        runOnFlight "USE `${dbName}`"
        runOnMysql "set enable_sql_cache=true"
        runOnFlight "set enable_sql_cache=true"
        // See note 2 above: without this the flight lookup always misses and the test is toothless.
        runOnMysql "set sql_mode='ONLY_FULL_GROUP_BY'"
        runOnFlight "set sql_mode='ONLY_FULL_GROUP_BY'"

        // The cache key is the catalog, the database, the user and the sql text, and the lookup
        // additionally compares the session variables that affect the result. The statements below
        // are byte identical on both connections, so assert the rest of the inputs match too.
        assertEquals(runOnMysql("select database()")[0][0], runOnFlight("select database()")[0][0])
        assertEquals(runOnMysql("select current_user()")[0][0], runOnFlight("select current_user()")[0][0])
        assertEquals(runOnMysql("select @@sql_mode")[0][0], runOnFlight("select @@sql_mode")[0][0])

        // 1. A constant result, cached in the FE itself (PhysicalOneRowRelation.computeResultInFe
        // -> tryAddFeSqlCache). This replays through the resultSet branch of sendCachedValues,
        // needs no table and no quiet window, and is the cheapest way to hit the bug.
        def constantSql = "select 1 as c, 'x' as s"
        primeSqlCacheOnMysql(constantSql)
        def constantOnFlight = runOnFlight(constantSql)
        assertEquals(1, constantOnFlight.size())
        assertEquals(1, constantOnFlight[0][0] as int)
        assertEquals("x", constantOnFlight[0][1].toString())

        def tblName = "test_sql_cache_over_arrow_flight_tbl"
        runOnMysql "DROP TABLE IF EXISTS ${tblName}"
        runOnMysql """
            CREATE TABLE ${tblName} (
                k INT,
                h HLL HLL_UNION,
                q QUANTILE_STATE QUANTILE_UNION
            ) AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        runOnMysql "INSERT INTO ${tblName} SELECT 1, HLL_HASH('x'), TO_QUANTILE_STATE(1, 2048)"

        // 2. A plain scalar result read from a table, cached on the BE. The failure was protocol
        // specific, not type specific.
        def scalarSql = "select k from ${tblName} order by k"
        primeSqlCacheOnMysql(scalarSql)
        def scalarOnFlight = runOnFlight(scalarSql)
        assertEquals(1, scalarOnFlight.size())
        assertEquals(1, scalarOnFlight[0][0] as int)

        // 3. The raw aggregate state columns from the issue. HLL and QUANTILE_STATE are carried as
        // arrow binary (be/src/format/arrow/arrow_row_batch.cpp), so flight returns the serialized
        // state, while the MySQL protocol keeps showing NULL under
        // return_object_data_as_binary=false. Asserting both at once also proves the flight result
        // is produced by the BE rather than replayed from the MySQL rows sitting in the cache.
        def rawStateSql = "select h, q from ${tblName}"
        primeSqlCacheOnMysql(rawStateSql)
        def rawStateOnMysql = runOnMysql(rawStateSql)
        assertEquals(1, rawStateOnMysql.size())
        assertNull(rawStateOnMysql[0][0])
        assertNull(rawStateOnMysql[0][1])
        def rawStateOnFlight = runOnFlight(rawStateSql)
        assertEquals(1, rawStateOnFlight.size())
        assertTrue(isNonEmptyBinary(rawStateOnFlight[0][0]),
                "expect a non empty HLL state over arrow flight, but got: " + rawStateOnFlight[0][0])
        assertTrue(isNonEmptyBinary(rawStateOnFlight[0][1]),
                "expect a non empty QUANTILE_STATE over arrow flight, but got: " + rawStateOnFlight[0][1])

        // 4. The server side conversions the issue used as a workaround.
        def convertedSql = "select hll_cardinality(h) as c, quantile_percent(q, 0.5) as p from ${tblName}"
        primeSqlCacheOnMysql(convertedSql)
        def convertedOnFlight = runOnFlight(convertedSql)
        assertEquals(1, convertedOnFlight.size())
        assertEquals(1L, convertedOnFlight[0][0] as long)
        assertEquals(1.0d, convertedOnFlight[0][1] as double, 1e-9)

        // The flight queries must not have consumed the cache: a cached plan reaching a non MySQL
        // connection is exactly the crash this test guards against, and the entries must still be
        // there for the MySQL session afterwards.
        assertTrue(hasSqlCache(constantSql))
        assertTrue(hasSqlCache(scalarSql))
        assertTrue(hasSqlCache(rawStateSql))
        assertTrue(hasSqlCache(convertedSql))
    }
}
