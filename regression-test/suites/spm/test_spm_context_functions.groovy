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

suite("test_spm_context_functions", "spm") {
    sql "SET enable_spm_rewrite = true"
    sql "SET enable_spm_fallback = false"
    sql "SET enable_sql_cache = false"
    sql("SHOW BASELINE PLANS").findAll { it[1].toString().contains("spm_context_functions") }
            .each { row -> sql "DROP BASELINE PLAN ${row[0]}" }
    sql "DROP TABLE IF EXISTS spm_context_functions"
    sql """
        CREATE TABLE spm_context_functions (k INT, event_time DATETIME(6))
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO spm_context_functions VALUES (1, '2020-01-01'), (2, '2020-01-02')"

    def createFrozen = { String query ->
        long id = sql('CREATE GLOBAL BASELINE PLAN "' + query + '" WITH "' + query + '"')[0][0] as Long
        def stored = sql("SELECT plan_frozen, plan_sql FROM __internal_schema.spm_baselines WHERE id = ${id}")[0]
        assertTrue(stored[0].toString() in ["1", "true"], query + ": " + stored)
        return [id, stored[1].toString()]
    }
    def hit = { String query, long id ->
        assertTrue(sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id}"), query)
    }

    sql "SET time_zone = '+00:00'"
    String zoneQuery = "SELECT k, TIMESTAMPDIFF(HOUR, UTC_TIMESTAMP(), NOW()) AS zone_offset " +
            "FROM spm_context_functions WHERE k = 1"
    def (zoneId, zoneSql) = createFrozen(zoneQuery)
    assertTrue(zoneSql.toLowerCase().contains("utc_timestamp()") && zoneSql.toLowerCase().contains("now()"))
    hit(zoneQuery, zoneId)
    qt_zone_utc zoneQuery
    sql "SET time_zone = '+08:00'"
    hit(zoneQuery, zoneId)
    qt_zone_local zoneQuery
    String zoneVariant = zoneQuery.replace("k = 1", "k = 2")
    hit(zoneVariant, zoneId)
    qt_zone_variant zoneVariant

    for (boolean foldByBe : [false, true]) {
        sql "SET enable_fold_constant_by_be = ${foldByBe}"
        for (String call : ["now(3)", "now(6)", "now(9)", "now(1 + 2)",
                "utc_timestamp(3)", "utc_timestamp(9)", "utc_time(3)",
                "current_time(3)", "curtime(3)", "localtime(3)", "localtimestamp(3)"]) {
            String query = "SELECT ${call} AS c FROM spm_context_functions WHERE k = 1"
            def (id, frozen) = createFrozen(query)
            assertTrue(frozen.toLowerCase().contains("("), frozen)
            hit(query, id)
            hit(query.replace("k = 1", "k = 2"), id)
            sql(query)
        }
    }
    sql "SET enable_fold_constant_by_be = false"
    String precision = "SELECT now(3) AS c, k FROM spm_context_functions WHERE k = 1"
    def (precisionId, precisionSql) = createFrozen(precision)
    assertTrue(precisionSql.toLowerCase().contains("now(3)"), precisionSql)
    assertFalse(sql("EXPLAIN " + precision.replace("now(3)", "now(6)"))
            .toString().contains("SPM baseline hit: id=${precisionId}"))
    test {
        sql "CREATE GLOBAL BASELINE PLAN '${precision}' WITH '${precision.replace("now(3)", "now(6)")}'"
        exception "align"
    }

    // A moving predicate cutoff must evaluate with each execution's statement start.
    String cutoff = "SELECT k FROM spm_context_functions WHERE event_time < now() ORDER BY k"
    def (cutoffId, cutoffSql) = createFrozen(cutoff)
    assertTrue(cutoffSql.toLowerCase().contains("now()"), cutoffSql)
    sql "INSERT INTO spm_context_functions SELECT 3, date_add(now(), INTERVAL 8 SECOND)"
    hit(cutoff, cutoffId)
    qt_before_cutoff cutoff
    Thread.sleep(9000)
    hit(cutoff, cutoffId)
    qt_after_cutoff cutoff

    // The global baseline retains all FE-only calls and executes under the caller's identity.
    String db = sql("SELECT database()")[0][0].toString()
    String caller = "spm_context_functions_caller"
    String pwd = "SpmContextPwd123"
    sql "DROP USER IF EXISTS '${caller}'"
    sql "CREATE USER '${caller}' IDENTIFIED BY '${pwd}'"
    sql "GRANT SELECT_PRIV ON ${db}.spm_context_functions TO '${caller}'"
    String identity = "SELECT k, current_user() AS auth, session_user() AS sess, user() AS login, " +
            "connection_id() AS connection, database() AS db, current_catalog() AS cat, version() AS ver " +
            "FROM ${db}.spm_context_functions WHERE k = 1"
    def (identityId, identitySql) = createFrozen(identity)
    def creator = sql(identity)
    String identityFilter = "SELECT k FROM ${db}.spm_context_functions WHERE k = 1 AND " +
            "current_user() LIKE '%spm_context_functions_caller%'"
    def (filterId, filterSql) = createFrozen(identityFilter)
    assertTrue(sql(identityFilter).isEmpty())
    String url = org.apache.doris.regression.Config.buildUrlWithDb(context.config.jdbcUrl, db)
    try {
        connect(caller, pwd, url) {
            sql "SET enable_spm_rewrite = true"
            sql "SET enable_spm_fallback = false"
            sql "SET enable_sql_cache = false"
            hit(identity, identityId)
            def rewritten = sql(identity)
            sql "SET enable_spm_rewrite = false"
            assertEquals(sql(identity), rewritten)
            assertTrue(rewritten[0][1].toString().contains(caller))
            assertNotEquals(creator[0][1], rewritten[0][1])
            assertNotEquals(creator[0][4], rewritten[0][4])
            sql "SET enable_spm_rewrite = true"
            hit(identityFilter, filterId)
            qt_caller_filter identityFilter
        }
    } finally {
        sql "DROP USER IF EXISTS '${caller}'"
    }

    String lastQuery = "SELECT last_query_id() AS previous_id FROM spm_context_functions WHERE k = 1"
    def (lastId, lastSql) = createFrozen(lastQuery)
    assertTrue(lastSql.toLowerCase().contains("last_query_id()"), lastSql)
    hit(lastQuery, lastId)
    assertNotEquals(sql(lastQuery)[0][0], sql(lastQuery)[0][0])

    // A canonical rendered name cannot bind to a colliding UDF under caller preference.
    sql "DROP FUNCTION IF EXISTS now()"
    try {
        sql "CREATE ALIAS FUNCTION now() WITH PARAMETER() AS abs(-42)"
        sql "SET prefer_udf_over_builtin = false"
        String bare = "SELECT CURRENT_TIMESTAMP AS c FROM spm_context_functions WHERE k = 1"
        def (bareId, bareSql) = createFrozen(bare)
        hit(bare, bareId)
        sql "SET prefer_udf_over_builtin = true"
        def rows = sql(bare)
        assertFalse(rows[0][0].toString() == "42", rows.toString())
    } finally {
        sql "SET prefer_udf_over_builtin = false"
        sql "DROP FUNCTION IF EXISTS now()"
    }

    // Reload must retain the calls, not re-parameterize precision or persist a creator value.
    Thread.sleep(65000)
    hit(zoneQuery, zoneId)
    qt_after_reload zoneQuery

    test {
        sql "CREATE GLOBAL BASELINE PLAN 'SELECT @v FROM spm_context_functions' WITH 'SELECT @v FROM spm_context_functions'"
        exception "replay-time context"
    }
}
