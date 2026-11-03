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

suite("test_trino_mongodb_types", "p0,external") {
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
    sql "DROP CATALOG IF EXISTS mongodb_types_test"
    sql """CREATE CATALOG mongodb_types_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.cursor-batch-size" = "127"
    )"""
    sql "SWITCH mongodb_types_test"
    sql "USE doris_mongodb_test"

    order_qt_schema """DESC records"""

    qt_all_columns """SELECT * FROM records ORDER BY seq"""

    order_qt_objectids """SELECT seq, from_binary(_id), from_binary(owner), length(cast(_id AS STRING)) FROM records"""

    order_qt_ordinary_binary """SELECT seq, from_binary(blob), length(cast(blob AS STRING)) FROM records"""

    order_qt_primitive_values """SELECT seq, name, grp, active, score, amount, ratio, created, optional_note FROM records"""

    order_qt_null_missing """SELECT seq, owner IS NULL, score IS NULL, optional_note IS NULL FROM records"""

    order_qt_objectid_null """SELECT seq, from_binary(owner) FROM records WHERE owner IS NULL"""

    order_qt_objectid_not_null """SELECT seq, from_binary(owner) FROM records WHERE owner IS NOT NULL"""

    order_qt_arrays """SELECT seq, tags, numbers, refs FROM records"""

    order_qt_objectid_array_elements """SELECT seq, from_binary(refs[1]), from_binary(refs[2]) FROM records"""

    order_qt_array_operations """SELECT seq, array_size(refs), array_size(tags), array_contains(tags, 'red') FROM records"""

    order_qt_nested_row """SELECT seq, profile.city, profile.rank, from_binary(profile.manager) FROM records"""

    order_qt_nested_projection_filter """SELECT seq, profile.city FROM records WHERE profile.city = 'A'"""

    order_qt_nullable_row """SELECT seq, profile FROM records WHERE seq >= 3"""

    order_qt_nested_objectid_null """SELECT seq FROM records WHERE profile.manager IS NULL"""

    order_qt_unicode """SELECT seq, name, tags FROM records WHERE name LIKE '%é' OR array_contains(tags, '中文')"""

    order_qt_decimal_arithmetic """SELECT seq, amount, amount * 2, round(ratio, 2) FROM records"""

    order_qt_dates """SELECT seq, created, year(created), month(created) FROM records WHERE created >= '2024-01-01'"""

    order_qt_mixed_numeric_types """SELECT seq, value, value IS NULL FROM mixed_values"""

    order_qt_empty_schema """DESC empty_records"""

    order_qt_empty_projection """SELECT _id, seq, owner FROM empty_records"""

    order_qt_empty_aggregate """SELECT count(*), count(owner), min(seq), max(seq) FROM empty_records"""

    order_qt_numeric_limits """SELECT seq, from_binary(_id), i32, i64, amount, ratio, text, created
        FROM numeric_limits"""
    order_qt_numeric_limit_predicates """SELECT seq FROM numeric_limits
        WHERE i64 = 9223372036854775807 OR created < '1969-12-31'"""
}
