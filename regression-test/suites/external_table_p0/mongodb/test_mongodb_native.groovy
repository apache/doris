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

import groovy.json.JsonOutput

suite("test_mongodb_native", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoDBTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    String user = context.config.otherConfigs.get("mongodb_user")
    String password = context.config.otherConfigs.get("mongodb_password")
    def create = { String name, Map extra ->
        sql "DROP CATALOG IF EXISTS ${name}"
        Map properties = ["type": "mongodb", "mongodb.uri": "mongodb://${host}:${port}",
            "mongodb.username": user, "mongodb.password": password, "mongodb.databases": "doris_mongodb_native"]
        properties.putAll(extra)
        String values = properties.collect { key, value ->
            "\"${key}\" = '${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'"
        }.join(",\n")
        sql "CREATE CATALOG ${name} PROPERTIES (${values})"
    }
    create("mongodb_native_auto", [:])
    sql "SWITCH mongodb_native_auto"
    sql "USE doris_mongodb_native"

    order_qt_discovery "SHOW TABLES"
    order_qt_validator_schema "DESC typed_records"
    order_qt_typed """SELECT seq, name, grp, score, active, ratio, from_binary(_id), created,
        tags, profile.city, profile.rank, from_binary(profile.manager), clock.seconds, clock.increment
        FROM typed_records"""
    order_qt_objectid_equal "SELECT seq FROM typed_records WHERE FROM_BINARY(_id) = '000000000000000000000001'"
    order_qt_objectid_join "SELECT count(*) FROM typed_records a JOIN typed_records b ON FROM_BINARY(a._id) = FROM_BINARY(b._id)"
    order_qt_objectid_groups "SELECT from_binary(_id), count(*) FROM typed_records GROUP BY FROM_BINARY(_id)"
    test {
        sql "SELECT seq FROM typed_records WHERE _id = X'000000000000000000000001'"
        exception "varbinary"
    }
    test {
        sql "SELECT _id, count(*) FROM typed_records GROUP BY _id"
        exception "don't support filter, group by or order by"
    }
    order_qt_null "SELECT seq, score IS NULL, profile IS NULL, tags IS NULL FROM typed_records"
    order_qt_decimal_json "SELECT seq, cast(amount AS STRING), cast(blob AS STRING) FROM typed_records"

    def originalTimeZone = sql("SELECT @@time_zone")[0][0]
    sql "SET time_zone = '+08:00'"
    order_qt_utc_east "SELECT seq, created FROM typed_records"
    sql "SET time_zone = '-05:00'"
    order_qt_utc_west "SELECT seq, created FROM typed_records"
    sql "SET time_zone = '${originalTimeZone}'"
    order_qt_where "SELECT seq, name FROM typed_records WHERE score >= 10 AND active = true"
    order_qt_or_in "SELECT seq FROM typed_records WHERE grp IN ('east', 'west') AND (score IS NULL OR score < 20)"
    order_qt_like "SELECT seq, name FROM typed_records WHERE name LIKE '%a%'"
    order_qt_regexp "SELECT seq, name FROM typed_records WHERE name REGEXP '^(alpha|beta)\$'"
    order_qt_unicode "SELECT seq FROM typed_records WHERE name LIKE '%中文%'"
    order_qt_group "SELECT grp, count(*), sum(score), min(seq), max(seq) FROM typed_records GROUP BY grp"
    order_qt_join """SELECT a.seq, b.seq FROM typed_records a JOIN typed_records b
        ON a.grp = b.grp WHERE a.seq < b.seq"""
    order_qt_array "SELECT seq, array_size(tags), array_contains(tags, 'red') FROM typed_records"
    order_qt_window "SELECT seq, row_number() OVER (PARTITION BY grp ORDER BY seq) FROM typed_records"
    qt_order_limit "SELECT seq, name FROM typed_records ORDER BY seq DESC LIMIT 2 OFFSET 1"
    order_qt_count "SELECT count(*) FROM typed_records"
    order_qt_empty "SELECT count(*), count(seq) FROM empty_records"
    order_qt_zero_match "SELECT seq FROM typed_records WHERE seq > 100"
    order_qt_multi_batch "SELECT count(*), min(seq), max(seq), sum(seq) FROM large_records"
    order_qt_batch_edges "SELECT seq FROM large_records WHERE seq IN (0,126,127,128,1023,1024,1025,2047,2048,2080)"
    qt_batch_offset "SELECT seq FROM large_records ORDER BY seq LIMIT 5 OFFSET 1022"
    for (int i = 0; i < 3; ++i) {
        "qt_repeated_limit_${i}" "SELECT seq FROM large_records ORDER BY seq LIMIT 3"
        "qt_early_cursor_close_${i}" "SELECT count(*) FROM (SELECT seq FROM large_records LIMIT 1) limited_rows"
    }
    order_qt_after_limits "SELECT count(*) FROM large_records"
    order_qt_fallback_schema "DESC unsupported_validator"
    order_qt_fallback "SELECT cast(_mongo_ejson AS STRING), length(_mongo_bson) > 0 FROM unsupported_validator"
    test {
        sql "SELECT seq FROM historical_records"
        exception "BSON"
    }
    order_qt_view_document "SELECT cast(_mongo_ejson AS STRING) FROM record_view"
    order_qt_timeseries_document "SELECT count(*) FROM measurements"

    create("mongodb_native_documents", ["mongodb.schema.mode": "document"])
    sql "SWITCH mongodb_native_documents"
    sql "USE doris_mongodb_native"
    order_qt_document_override "DESC typed_records"
    order_qt_document_bytes "SELECT count(*), min(length(_mongo_bson)), max(length(_mongo_bson)) FROM typed_records"
    order_qt_extended_bson "SELECT cast(_mongo_ejson AS STRING) FROM special_records"
    create("mongodb_native_validators", ["mongodb.schema.mode": "validator"])
    sql "SWITCH mongodb_native_validators"
    sql "USE doris_mongodb_native"
    order_qt_forced_validator "SELECT count(*), sum(seq) FROM typed_records"
    for (String object : ["record_view", "measurements", "unsupported_validator"]) {
        test {
            sql "SELECT * FROM ${object}"
            exception "validator"
        }
    }

    def column = { String name, String type, List path = null, List tags = [] ->
        [name: name, path: path == null ? [name] : path, doris_type: type, bson_types: tags]
    }
    Map manifest = [
        "doris_mongodb_native.record_view": [columns: [column("seq", "BIGINT", null, ["long"]),
            column("label", "STRING", null, ["string"])]],
        "doris_mongodb_native.measurements": [columns: [column("observed", "DATETIME(3)", null, ["date"]),
            column("device", "STRING", null, ["string"]), column("value", "INT", null, ["int"])]],
        "doris_mongodb_native.special_records": [columns: [
            column("identifier", "VARBINARY(12)", ["_id"], ["objectId"]),
            column("integral", "BIGINT", null, ["long"]), column("exact_amount", "DECIMAL(10,4)", ["decimal"], ["decimal"]),
            column("literal", "STRING", ["literal.dot"], ["string"]),
            column("nested", "STRING", ["nested", "dot"], ["string"]),
            column("dollar", "STRING", ['dollar$field'], ["string"]),
            column("pattern", "JSON", null, ["regex"]), column("low", "JSON", null, ["minKey"]),
            column("high", "JSON", null, ["maxKey"]), column("code", "JSON", null, ["javascriptWithScope"]),
            column("refs", "ARRAY<VARBINARY(12)>", null, ["array"])
        ]]
    ]
    create("mongodb_native_explicit", ["mongodb.schema.mode": "explicit", "mongodb.schema": JsonOutput.toJson(manifest)])
    sql "SWITCH mongodb_native_explicit"
    sql "USE doris_mongodb_native"
    order_qt_view "SELECT seq, label FROM record_view"
    order_qt_view_analysis "SELECT count(*), min(seq), max(seq) FROM record_view WHERE label LIKE 'view-%'"
    order_qt_timeseries "SELECT observed, device, value FROM measurements"
    order_qt_timeseries_where """SELECT device, count(*), sum(value) FROM measurements
        WHERE observed >= '2024-01-01 00:00:00' AND observed < '2024-01-02 00:00:00' GROUP BY device"""
    order_qt_explicit_paths "SELECT from_binary(identifier), integral, exact_amount, literal, nested, dollar FROM special_records"
    order_qt_special_types "SELECT cast(pattern AS STRING), cast(low AS STRING), cast(high AS STRING), cast(code AS STRING) FROM special_records"
    order_qt_refs "SELECT array_size(refs), from_binary(refs[1]), refs[2] IS NULL FROM special_records"

    manifest["doris_mongodb_native.special_records"].columns.find { it.name == "literal" }.path = ["nested", "dot"]
    String changedSchema = JsonOutput.toJson(manifest).replace("\\", "\\\\").replace("'", "\\'")
    sql """ALTER CATALOG mongodb_native_explicit SET PROPERTIES ('mongodb.schema'='${changedSchema}')"""
    sql "REFRESH CATALOG mongodb_native_explicit"
    order_qt_refresh "SELECT literal, nested FROM special_records"

    List mismatchColumns = [column("seq", "BIGINT"), column("n", "BIGINT"), column("arr", "ARRAY<INT>"),
        column("obj", "STRUCT<a:INT,b:STRING>"), column("amount", "DECIMAL(10,2)")]
    Map mismatchManifest = ["doris_mongodb_native.mismatched": [columns: mismatchColumns]]
    create("mongodb_native_nulls", ["mongodb.schema.mode": "explicit", "mongodb.type_mismatch": "null",
        "mongodb.schema": JsonOutput.toJson(mismatchManifest)])
    sql "SWITCH mongodb_native_nulls"
    sql "USE doris_mongodb_native"
    order_qt_mismatch_nulls "SELECT seq, n, arr, obj, amount FROM mismatched"
    order_qt_mismatch_filter "SELECT seq FROM mismatched WHERE n IS NULL OR arr IS NULL OR obj IS NULL OR amount IS NULL"
    create("mongodb_native_errors", ["mongodb.schema.mode": "explicit",
        "mongodb.schema": JsonOutput.toJson(mismatchManifest)])
    sql "SWITCH mongodb_native_errors"
    sql "USE doris_mongodb_native"
    for (String field : ["n", "arr", "obj", "amount"]) {
        test {
            sql "SELECT ${field} FROM mismatched"
            exception "BSON"
        }
    }
    create("mongodb_native_view_only", ["mongodb.username": "native_view_reader",
        "mongodb.password": "native_view_reader", "mongodb.auth_database": "doris_mongodb_native",
        "mongodb.schema": JsonOutput.toJson(manifest)])
    sql "SWITCH mongodb_native_view_only"
    sql "USE doris_mongodb_native"
    order_qt_view_only_discovery "SHOW TABLES"
    order_qt_view_only_read "SELECT seq, label FROM record_view"
    sql "SWITCH internal"
}
