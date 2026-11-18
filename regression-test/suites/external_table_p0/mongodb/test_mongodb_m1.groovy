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

import groovy.json.JsonOutput

suite("test_mongodb_m1", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoDBTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    def quote = { value -> "'${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'" }
    def col = { String name, String type, List tags, List path = null ->
        [name: name, path: path ?: [name], doris_type: type, bson_types: tags]
    }
    List fields = [col("seq", "BIGINT", ["long"]), col("n", "BIGINT", ["int", "long"]),
        col("text", "STRING", ["string"]), col("active", "BOOLEAN", ["bool"]),
        col("amount", "DECIMAL(20,2)", ["decimal"]), col("observed", "DATETIME(3)", ["date"]),
        col("epoch_ms", "BIGINT", ["date"], ["observed"]),
        col("bytes", "VARBINARY", ["binData"]),
        col("nested", "ARRAY<STRUCT<amount:DECIMAL(10,2),tags:ARRAY<STRING>>>", ["array"])]
    fields.find { it.name == "bytes" }.subtype = 0
    List precisionFields = [fields[0]]
    for (int scale = 0; scale <= 6; scale++) {
        precisionFields.add(col("time_p${scale}", "DATETIME(${scale})", ["date"], ["observed"]))
    }
    for (List spec : [[1, 0], [9, 0], [9, 2], [9, 9], [18, 0], [18, 6], [18, 18], [38, 0], [38, 4], [38, 38]]) {
        precisionFields.add(col("decimal_p${spec[0]}s${spec[1]}", "DECIMAL(${spec[0]},${spec[1]})",
            ["decimal"], ["amount"]))
    }
    Map manifest = ["doris_mongodb_m1.records": [columns: fields],
        "doris_mongodb_m1.precision_records": [columns: precisionFields],
        "doris_mongodb_m1.dirty": [columns: fields.take(2)],
        "doris_mongodb_m1.collated": [columns: [fields[0], fields[2]]],
        "doris_mongodb_m1.paths": [columns: [fields[0], col("child", "BIGINT", ["long"], ["a", "b"]),
            col("literal", "STRING", ["string"], ["literal.dot"])]],
        "doris_mongodb_native.typed_records": [columns: [fields[0], col("tags", "ARRAY<STRING>", ["array"]),
            col("json_tags", "JSON", ["array"], ["tags"])]],
        "doris_mongodb_native.record_view": [columns: [fields[0], col("label", "STRING", ["string"])]],
        "doris_mongodb_native.measurements": [columns: [fields[5], col("device", "STRING", ["string"]),
            col("value", "INT", ["int"])]]]
    for (String policy : ["error", "null"]) {
        sql "DROP CATALOG IF EXISTS mongodb_m1_${policy}"
        Map properties = ["type": "mongodb", "mongodb.uri": "mongodb://${host}:${port}",
            "mongodb.username": context.config.otherConfigs.get("mongodb_user"),
            "mongodb.password": context.config.otherConfigs.get("mongodb_password"),
            "mongodb.databases": "doris_mongodb_m1,doris_mongodb_native", "mongodb.type_mismatch": policy,
            "mongodb.schema": JsonOutput.toJson(manifest)]
        sql "CREATE CATALOG mongodb_m1_${policy} PROPERTIES (${properties.collect { k, v -> "${quote(k)}=${quote(v)}" }.join(',')})"
    }
    List predicates = ["n = 9007199254740993", "n < -9007199254740992", "n >= 7 AND n <= 20",
        "n IN (7, 20, NULL)", "n NOT IN (7, NULL)", "n IS NULL", "n IS NOT NULL", "n <=> 7", "n <=> NULL",
        "n != 7", "n = 7 OR text LIKE '%alpha%'", "n = 7 OR cast(text AS BIGINT) = 20",
        "cast(text AS BIGINT) = 20", "active = true", "text = 'alpha'", "text > 'alpha'",
        "text LIKE '%a.b%'", "text LIKE '%中文%'", "text LIKE '%alpha%'", "text LIKE 'a_c%'",
        "text LIKE 'a%'", "text LIKE '%'", "text LIKE ''", "text REGEXP 'pha'",
        "amount = 123.45", "amount >= -2.50 AND amount <= 1.23",
        "observed >= '1969-12-31 23:59:59.999' AND observed < '1970-01-01 00:00:00.001'",
        "epoch_ms = -1", "n BETWEEN 7 AND 10"]
    for (boolean enabled : [false, true]) {
        sql "SET enable_mongodb_predicate_pushdown=${enabled}"
        sql "SET enable_mongodb_projection_pushdown=${enabled}"
        sql "SET enable_mongodb_like_pushdown=${enabled}"
        sql "SWITCH mongodb_m1_null"
        sql "USE doris_mongodb_m1"
        for (int i = 0; i < predicates.size(); i++) {
            "order_qt_filter_${enabled}_${i}" "SELECT seq, n, text FROM records WHERE ${predicates[i]}"
        }
        "order_qt_types_${enabled}" """SELECT seq, n, amount, observed, epoch_ms,
            from_binary(bytes), cast(nested AS STRING) FROM records"""
        "order_qt_count_${enabled}" "SELECT count(*) FROM records"
        "order_qt_aggregate_${enabled}" "SELECT active, count(*), sum(amount) FROM records WHERE n >= 7 GROUP BY active"
        "order_qt_join_${enabled}" "SELECT a.seq, b.seq FROM records a JOIN records b ON a.n = b.n WHERE a.seq < b.seq"
        "order_qt_residual_limit_${enabled}" "SELECT seq FROM records WHERE n >= 7 AND length(text) > 2 ORDER BY seq LIMIT 2"
        "order_qt_collation_${enabled}" "SELECT seq, text FROM collated WHERE text = 'alpha'"
        "order_qt_view_${enabled}" """SELECT seq, label FROM mongodb_m1_null.doris_mongodb_native.record_view
            WHERE label = 'VIEW-ALPHA' OR label LIKE '%中文%'"""
        "order_qt_timeseries_${enabled}" """SELECT device, sum(value), count(*)
            FROM mongodb_m1_null.doris_mongodb_native.measurements
            WHERE observed >= '2024-01-01 00:00:00.001' AND observed < '2024-01-02 00:00:00' GROUP BY device"""
        "order_qt_array_not_null_${enabled}" """SELECT seq, tags
            FROM mongodb_m1_null.doris_mongodb_native.typed_records WHERE tags IS NOT NULL"""
        "order_qt_json_not_null_${enabled}" """SELECT seq, cast(json_tags AS STRING)
            FROM mongodb_m1_null.doris_mongodb_native.typed_records WHERE json_tags IS NOT NULL"""
        "order_qt_paths_${enabled}" "SELECT seq, child, literal FROM paths"
        String originalTimeZone = sql("SELECT @@time_zone")[0][0].toString()
        try {
            List timeZones = ["UTC", "+08:00", "America/Los_Angeles", "Asia/Kathmandu"]
            for (int zone = 0; zone < timeZones.size(); zone++) {
                sql "SET time_zone='${timeZones[zone]}'"
                // BSON Date and its typed candidate use UTC even when the session crosses midnight.
                "order_qt_utc_date_${enabled}_${zone}" """SELECT seq, observed, epoch_ms FROM records
                    WHERE observed >= '1969-12-31 23:59:59.999'
                      AND observed < '1970-01-01 00:00:00.001'"""
                "order_qt_utc_timeseries_${enabled}_${zone}" """SELECT observed, device, value
                    FROM mongodb_m1_null.doris_mongodb_native.measurements
                    WHERE observed >= '2024-01-01 00:00:00.001'
                      AND observed < '2024-01-02 00:00:00'"""
                "order_qt_precision_types_${enabled}_${zone}" """SELECT ${precisionFields.collect { it.name }.join(',')}
                    FROM precision_records"""
                "order_qt_precision_filter_${enabled}_${zone}" """SELECT seq, time_p0, time_p3, time_p6,
                    decimal_p9s2, decimal_p38s4, decimal_p38s38 FROM precision_records
                    WHERE time_p6 >= '1969-12-31 23:59:59.999' AND decimal_p38s4 >= 123.45"""
                "order_qt_submillisecond_${enabled}_${zone}" """SELECT seq FROM precision_records
                    WHERE time_p6 = '1969-12-31 23:59:59.999001'"""
                "order_qt_dst_boundary_${enabled}_${zone}" """SELECT seq, time_p3, time_p6 FROM precision_records
                    WHERE time_p6 >= '2024-03-10 09:59:59.999' AND time_p6 <= '2024-03-10 10:00:00'"""
                "order_qt_high_scale_decimal_${enabled}_${zone}" """SELECT seq, decimal_p38s38 FROM precision_records
                    WHERE decimal_p38s38 = cast('0.1234567890123456789012345678901234' AS DECIMAL(38,38))"""
            }
        } finally {
            sql "SET time_zone='${originalTimeZone}'"
        }
        test {
            sql "SELECT seq, n FROM mongodb_m1_error.doris_mongodb_m1.dirty WHERE seq = 1"
            exception "BSON"
        }
        test {
            sql "SELECT seq, child FROM mongodb_m1_error.doris_mongodb_m1.paths"
            exception "BSON"
        }
        for (String field : ["time_p0", "time_p1", "time_p2", "decimal_p1s0", "decimal_p9s2", "decimal_p38s38"]) {
            test {
                sql "SELECT ${field} FROM mongodb_m1_error.doris_mongodb_m1.precision_records"
                exception "BSON"
            }
        }
    }
    sql "SET enable_mongodb_predicate_pushdown=true"
    sql "SET enable_mongodb_projection_pushdown=true"
    sql "SET enable_mongodb_like_pushdown=true"
    explain {
        sql "SELECT seq FROM records WHERE n >= 7"
        contains "MongoDB predicate: SUPERSET"
        contains "residual: retained"
    }
    explain {
        sql "SELECT seq FROM mongodb_m1_error.doris_mongodb_m1.dirty WHERE seq = 1"
        contains "MongoDB predicate: UNSUPPORTED"
    }
    explain {
        sql "SELECT /*+ SET_VAR(enable_mongodb_predicate_pushdown=false) */ seq FROM records WHERE n >= 7"
        contains "MongoDB predicate: UNSUPPORTED"
    }
    explain {
        sql "SELECT /*+ SET_VAR(enable_mongodb_like_pushdown=false) */ seq FROM records WHERE text LIKE '%alpha%'"
        contains "MongoDB predicate: UNSUPPORTED"
    }
    explain {
        sql "SELECT /*+ SET_VAR(enable_strict_cast=true) */ seq FROM records WHERE n >= 7"
        contains "MongoDB predicate: UNSUPPORTED"
    }
    explain {
        sql """SELECT value FROM mongodb_m1_null.doris_mongodb_native.measurements
            WHERE observed >= '2024-01-01 00:00:00.001' AND observed < '2024-01-02 00:00:00'"""
        contains "MongoDB predicate: SUPERSET"
    }
    explain {
        sql "SELECT seq FROM records WHERE amount = 123.45"
        contains "MongoDB predicate: SUPERSET"
    }
    def profileAction = new ProfileAction(context)
    def sourceRows = { boolean pushdown ->
        String token = "mongodb_m1_" + UUID.randomUUID().toString()
        sql """SELECT /*+ SET_VAR(enable_profile=true,profile_level=2,
            enable_mongodb_predicate_pushdown=${pushdown}) */ '${token}', seq
            FROM records WHERE n = 9007199254740993"""
        String profile = profileAction.getProfileBySql(token, ["MongoRowsRead"])
        def counter = profile =~ /MongoRowsRead:\s*([0-9]+)/
        if (!counter.find()) {
            throw new IllegalStateException("MongoDB source row counter is absent from the completed profile")
        }
        return Long.parseLong(counter.group(1))
    }
    long withoutCandidate = sourceRows(false)
    long withCandidate = sourceRows(true)
    qt_remote_read_counts "SELECT ${withoutCandidate}, ${withCandidate}"
    order_qt_statement_switch_restored "SELECT @@enable_mongodb_predicate_pushdown, @@enable_mongodb_like_pushdown"
}
