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

import org.apache.doris.regression.action.ProfileAction

suite("dotted_key_escape_condition_cache", "p0") {
    sql "DROP TABLE IF EXISTS dotted_key_escape_condition_cache"
    // One tablet keeps the literal and nested predicates on the same segment/cache entries.
    sql """CREATE TABLE dotted_key_escape_condition_cache (
        id BIGINT, v VARIANT<PROPERTIES("variant_enable_nested_group"="false")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("variant_escape_dots_in_keys"="true", "replication_num"="1", "disable_auto_compaction"="true")"""
    sql """INSERT INTO dotted_key_escape_condition_cache
        SELECT number, CAST(CAST('{"a.b":0,"a":{"b":1},"a%2Eb":2,"":3}' AS JSON) AS VARIANT)
        FROM numbers("number"="65537")"""
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set enable_parallel_scan=false"
    sql "set enable_condition_cache=true"
    sql "set enable_profile=true"
    sql "set profile_level=2"

    def profiles = new ProfileAction(context)
    def checkCacheHit = { String query ->
        sql query
        String token = UUID.randomUUID().toString()
        sql "/* ${token} */ ${query}"
        String profile = profiles.getProfileBySql(token, ["ConditionCacheLookup", "ConditionCacheHit"])
        for (String counter : ["ConditionCacheLookup", "ConditionCacheHit"]) {
            def values = (profile =~ /${counter}: (\d+)/).findAll()
            assertTrue(values.any { (it[1] as long) > 0 }, "Missing ${counter}: ${profile}")
            logger.info("${counter}: ${values.collect { it[1] }} for ${query}")
        }
    }
    // Modulo stays as a scan conjunct: storage range pruning cannot replace cache evaluation.
    String literal = """SELECT count(*) FROM dotted_key_escape_condition_cache
        WHERE CAST(v['a.b'] AS BIGINT) % 3 = 1"""
    String nested = """SELECT count(*) FROM dotted_key_escape_condition_cache
        WHERE CAST(v['a']['b'] AS BIGINT) % 3 = 1"""
    checkCacheHit(literal)
    qt_literal literal
    qt_nested_after_literal nested
    check_sqls_result_equal(nested, nested.replace("SELECT", "SELECT /*+ SET_VAR(enable_condition_cache=false) */"))
    checkCacheHit(nested)
    qt_nested_warm nested
    qt_percent_key """SELECT count(*) FROM dotted_key_escape_condition_cache
        WHERE CAST(v['a%2Eb'] AS BIGINT) % 3 = 2"""
}
