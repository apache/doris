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

suite("test_trino_mongodb_queries", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableMongoDBTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    String user = context.config.otherConfigs.get("mongodb_user")
    String password = context.config.otherConfigs.get("mongodb_password")
    String encodedUser = java.net.URLEncoder.encode(user, "UTF-8").replace("+", "%20")
    String encodedPassword = java.net.URLEncoder.encode(password, "UTF-8").replace("+", "%20")
    sql "DROP CATALOG IF EXISTS mongodb_queries_test"
    sql """CREATE CATALOG mongodb_queries_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.cursor-batch-size" = "127"
    )"""
    sql "SWITCH mongodb_queries_test"
    sql "USE doris_mongodb_test"

    order_qt_count """SELECT count(*), count(owner), count(score), count(DISTINCT grp) FROM records"""

    order_qt_numeric_filter """SELECT seq FROM records WHERE seq >= 2 AND seq < 6"""

    order_qt_boolean_filter """SELECT seq FROM records WHERE active"""

    order_qt_string_filter """SELECT seq, name FROM records WHERE name IN ('alpha', 'beta', 'missing')"""

    order_qt_in_filter """SELECT seq FROM records WHERE seq IN (1, 3, 6)"""

    order_qt_not_in_filter """SELECT seq FROM records WHERE seq NOT IN (1, 3, 6)"""

    order_qt_between_filter """SELECT seq, score FROM records WHERE score BETWEEN -5 AND 10"""

    order_qt_or_filter """SELECT seq FROM records WHERE seq = 1 OR seq = 4 OR grp = 'north'"""

    order_qt_compound_filter """SELECT seq FROM records WHERE (grp = 'east' AND score > 0) OR (NOT active AND seq > 2)"""

    order_qt_null_logic """SELECT seq FROM records WHERE NOT (score > 0) OR score IS NULL"""

    order_qt_null_safe_equal """SELECT seq FROM records WHERE score <=> NULL"""

    order_qt_like_filter """SELECT seq FROM records WHERE name LIKE '%a%' AND name NOT LIKE 'g%'"""

    qt_residual_with_limit """SELECT seq FROM records WHERE length(name) > 4 ORDER BY seq LIMIT 2"""

    order_qt_cast_objectid_filter """SELECT seq, from_binary(_id) FROM records WHERE from_binary(_id) = '000000000000000000000001'"""

    order_qt_cast_objectid_range """SELECT seq, from_binary(_id) FROM records WHERE from_binary(_id) >= '000000000000000000000003'"""

    order_qt_cast_objectid_in """SELECT seq FROM records WHERE from_binary(owner) IN ('000000000000000000000065', '00FF807F0100020304050607')"""

    order_qt_case_expression """SELECT seq, CASE WHEN owner IS NULL THEN 'missing' ELSE from_binary(owner) END FROM records"""

    order_qt_coalesce """SELECT seq, coalesce(score, -99), coalesce(optional_note, 'none') FROM records"""

    order_qt_arithmetic_projection """SELECT seq, score + seq, amount / 2 FROM records"""

    order_qt_distinct """SELECT DISTINCT grp FROM records"""

    order_qt_group_by """SELECT grp, count(*), count(score), sum(score), min(seq), max(seq), sum(amount) FROM records GROUP BY grp"""

    order_qt_having """SELECT grp, count(*) FROM records GROUP BY grp HAVING count(*) > 1"""

    order_qt_objectid_group """SELECT from_binary(owner), count(*) FROM records GROUP BY from_binary(owner)"""

    qt_topn """SELECT seq, score FROM records ORDER BY score DESC, seq LIMIT 3"""

    qt_offset """SELECT seq FROM records ORDER BY seq LIMIT 2 OFFSET 2"""

    order_qt_limit_zero """SELECT _id, owner FROM records LIMIT 0"""

    order_qt_no_matches """SELECT _id, owner FROM records WHERE seq < 0"""

    order_qt_join_numeric """SELECT a.seq, b.seq FROM records a JOIN records b ON a.seq = b.seq WHERE a.seq <= 3"""

    order_qt_join_objectid """SELECT r.seq, o.label FROM records r JOIN owners o ON from_binary(r.owner) = from_binary(o._id)"""

    order_qt_left_join_objectid """SELECT r.seq, o.label FROM records r LEFT JOIN owners o ON from_binary(r.owner) = from_binary(o._id)"""

    order_qt_full_join """SELECT r.seq, o.label FROM records r FULL OUTER JOIN owners o ON r.seq = o.weight"""

    order_qt_semi_join """SELECT r.seq FROM records r LEFT SEMI JOIN owners o ON from_binary(r.owner) = from_binary(o._id)"""

    order_qt_anti_join """SELECT r.seq FROM records r LEFT ANTI JOIN owners o ON from_binary(r.owner) = from_binary(o._id)"""

    order_qt_cross_join """SELECT r.seq, o.label FROM records r CROSS JOIN owners o WHERE r.seq = 1"""

    order_qt_exists """SELECT r.seq FROM records r WHERE EXISTS (SELECT 1 FROM owners o WHERE from_binary(o._id) = from_binary(r.owner))"""

    order_qt_not_exists """SELECT r.seq FROM records r WHERE NOT EXISTS (SELECT 1 FROM owners o WHERE from_binary(o._id) = from_binary(r.owner))"""

    order_qt_in_subquery """SELECT seq FROM records WHERE from_binary(owner) IN (SELECT from_binary(_id) FROM owners WHERE weight <= 20)"""

    order_qt_scalar_subquery """SELECT seq FROM records WHERE seq > (SELECT avg(seq) FROM records)"""

    order_qt_cte """WITH t AS (SELECT grp, count(*) AS n FROM records GROUP BY grp) SELECT grp, n FROM t WHERE n > 1"""

    order_qt_union_all """SELECT seq, from_binary(_id) FROM records WHERE seq <= 2 UNION ALL SELECT seq, from_binary(_id) FROM records WHERE seq >= 5"""

    order_qt_union_distinct """SELECT grp FROM records WHERE seq < 4 UNION SELECT grp FROM records WHERE seq > 2"""

    order_qt_window """SELECT seq, grp, row_number() OVER (PARTITION BY grp ORDER BY seq), sum(seq) OVER (PARTITION BY grp) FROM records"""

    order_qt_array_unnest """SELECT seq, tag FROM records LATERAL VIEW explode(tags) t AS tag"""

    order_qt_objectid_array_unnest """SELECT seq, from_binary(ref) FROM records LATERAL VIEW explode(refs) t AS ref"""

    qt_null_filtered_limit """SELECT seq FROM records WHERE owner IS NULL ORDER BY seq LIMIT 1"""

    sql "CREATE DATABASE IF NOT EXISTS internal.mongodb_objectid_regression"
    sql "DROP VIEW IF EXISTS internal.mongodb_objectid_regression.objectid_view"
    sql """CREATE VIEW internal.mongodb_objectid_regression.objectid_view AS
        SELECT _id, seq, owner FROM mongodb_queries_test.doris_mongodb_test.records"""
    order_qt_view_schema "DESC internal.mongodb_objectid_regression.objectid_view"
    order_qt_view_data """SELECT seq, from_binary(_id), from_binary(owner)
        FROM internal.mongodb_objectid_regression.objectid_view"""
    sql "DROP TABLE IF EXISTS internal.mongodb_objectid_regression.materialized_records"
    sql """CREATE TABLE internal.mongodb_objectid_regression.materialized_records
        DISTRIBUTED BY HASH(seq) BUCKETS 1 PROPERTIES ("replication_num" = "1") AS
        SELECT seq, from_binary(_id) AS id_hex, from_binary(owner) AS owner_hex, grp
        FROM records"""
    order_qt_materialized_data "SELECT * FROM internal.mongodb_objectid_regression.materialized_records"
    order_qt_internal_join """SELECT r.seq, t.id_hex FROM records r
        JOIN internal.mongodb_objectid_regression.materialized_records t ON from_binary(r._id) = t.id_hex"""
    order_qt_internal_subquery """SELECT seq FROM records WHERE from_binary(_id) IN
        (SELECT id_hex FROM internal.mongodb_objectid_regression.materialized_records WHERE seq <= 3)"""
    sql "DROP TABLE IF EXISTS internal.mongodb_objectid_regression.raw_objectid_ctas"
    test {
        sql """CREATE TABLE internal.mongodb_objectid_regression.raw_objectid_ctas
            DISTRIBUTED BY HASH(seq) BUCKETS 1 PROPERTIES ("replication_num" = "1") AS
            SELECT seq, _id FROM records"""
        exception "varbinary"
    }

    // Native binary operators retain their existing analysis restrictions.
    test {
        sql "SELECT seq FROM records WHERE _id = X'000000000000000000000001'"
        exception "varbinary"
    }
    test {
        sql "SELECT owner, count(*) FROM records GROUP BY owner"
        exception "don't support filter, group by or order by"
    }
    test {
        sql "SELECT r.seq FROM records r JOIN owners o ON r.owner = o._id"
        exception "varbinary"
    }
}
