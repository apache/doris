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

suite("test_trino_mongodb_json", "p0,external") {
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
    sql "DROP CATALOG IF EXISTS mongodb_json_test"
    sql """CREATE CATALOG mongodb_json_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.cursor-batch-size" = "127"
    )"""
    sql "SWITCH mongodb_json_test"
    sql "USE doris_mongodb_test"

    order_qt_schema "DESC json_records"
    qt_all_columns "SELECT * FROM json_records ORDER BY seq"
    order_qt_shapes '''SELECT seq, json_type(payload, '$'), payload IS NULL FROM json_records'''
    order_qt_object '''SELECT json_extract_string(payload, '$.city'), json_extract_int(payload, '$.n'),
        json_extract_bool(payload, '$.active'), json_extract_string(payload, '$.nested.key'),
        json_extract_string(payload, '$.owner."$oid"') FROM json_records WHERE seq = 1'''
    order_qt_array '''SELECT seq, json_length(payload), json_extract_string(payload, '$[1]'),
        json_extract_bool(payload, '$[3].ok') FROM json_records WHERE seq = 2'''
    order_qt_unicode '''SELECT seq, hex(json_extract_string(payload, '$'))
        FROM json_records WHERE seq = 3'''
    order_qt_scalars '''SELECT seq, cast(payload AS STRING), json_type(payload, '$')
        FROM json_records WHERE seq BETWEEN 4 AND 7'''
    order_qt_sql_null "SELECT seq FROM json_records WHERE payload IS NULL"
    order_qt_json_null '''SELECT seq FROM json_records
        WHERE json_type(payload, '$') = 'null'
    '''
    order_qt_not_null "SELECT seq FROM json_records WHERE payload IS NOT NULL"
    order_qt_empty_values "SELECT seq, json_length(payload) FROM json_records WHERE seq IN (10, 11, 12)"
    order_qt_filter '''SELECT seq FROM json_records WHERE json_extract_int(payload, '$.n') = 7'''
    qt_residual_limit '''SELECT seq FROM json_records WHERE json_type(payload, '$') = 'array'
        ORDER BY seq LIMIT 2'''
    order_qt_nested_array '''SELECT seq, json_array, json_type(json_array[1], '$'),
        json_extract_int(json_array[1], '$.x'), json_type(json_array[3], '$'), json_array[4] IS NULL
        FROM json_records WHERE seq <= 4'''
    order_qt_nested_row '''SELECT seq, json_row, json_extract_int(json_row.payload, '$.x'),
        json_row IS NULL, json_row.payload IS NULL FROM json_records WHERE seq <= 4'''
    order_qt_nested_map '''SELECT seq, json_map, json_extract_int(json_map['a'], '$.x'),
        json_type(json_map['b'], '$'), json_map['c'] IS NULL FROM json_records WHERE seq <= 4'''
    order_qt_empty_nested_projection "SELECT seq, json_array, json_map FROM json_records WHERE seq = 2"
    order_qt_null_nested_projection '''SELECT seq, json_array, json_row, json_map
        FROM json_records WHERE seq IN (3, 4)'''
    order_qt_serialization_round_trip '''SELECT seq,
        json_type(json_parse(cast(json_array AS STRING)), '$[0]'),
        json_extract_string(json_parse(cast(json_array AS STRING)), '$[0]'),
        json_extract_string(json_parse(cast(json_row AS STRING)), '$.payload'),
        json_extract_string(json_parse(cast(json_map AS STRING)), '$.bool_string')
        FROM json_records WHERE seq = 5'''
    // Typed SQL text uses NULL for SQL nulls; JSON null retains its lowercase spelling.
    for (String strict : ["false", "true"]) {
        quickTest("typed_array_round_trip_${strict}", """SELECT /*+ SET_VAR(enable_strict_cast=${strict}) */
            seq, a, json_type(a[2], '\$'), json_type(a[3], '\$'), a[3] IS NULL, a[4] IS NULL
            FROM (SELECT seq, cast(cast(json_array AS STRING) AS ARRAY<JSON>) a
                FROM json_records WHERE seq <= 5) t ORDER BY seq""")
        quickTest("typed_map_round_trip_${strict}", """SELECT /*+ SET_VAR(enable_strict_cast=${strict}) */
            seq, m, json_type(m['b'], '\$'), m['b'] IS NULL, m['c'] IS NULL,
            json_type(m['number_string'], '\$'), json_type(m['null_string'], '\$'),
            json_type(m['bool_string'], '\$'), hex(json_extract_string(m['escaped'], '\$'))
            FROM (SELECT seq, cast(cast(json_map AS STRING) AS MAP<STRING, JSON>) m
                FROM json_records WHERE seq <= 5) t ORDER BY seq""")
        quickTest("typed_struct_round_trip_${strict}", """SELECT /*+ SET_VAR(enable_strict_cast=${strict}) */
            seq, r, json_type(r.payload, '\$'), r IS NULL, r.payload IS NULL
            FROM (SELECT seq, cast(cast(json_row AS STRING) AS STRUCT<payload:JSON>) r
                FROM json_records WHERE seq <= 5) t ORDER BY seq""")
    }
    order_qt_explode '''SELECT seq, json_type(item, '$'), cast(item AS STRING)
        FROM json_records LATERAL VIEW explode(json_array) t AS item'''
    order_qt_aggregate '''SELECT json_type(payload, '$'), count(*) FROM json_records GROUP BY json_type(payload, '$')'''
    order_qt_join '''SELECT a.seq, b.seq FROM json_records a JOIN json_records b
        ON json_extract_int(a.payload, '$.n') = json_extract_int(b.payload, '$.n')'''
    order_qt_empty_schema "DESC empty_json_records"
    order_qt_empty_projection "SELECT payload, json_array FROM empty_json_records"
    order_qt_empty_aggregate "SELECT count(*), count(payload) FROM empty_json_records"
    order_qt_multi_page '''SELECT count(*), count(json_payload),
        sum(json_extract_bigint(json_payload, '$.seq')) FROM large_records'''
    order_qt_batch_boundaries '''SELECT seq, json_payload, json_payload IS NULL FROM large_records
        WHERE seq IN (0,126,127,128,1023,1024,1025,2047,2048,2080)'''
    qt_multi_page_offset '''SELECT seq, json_extract_string(json_payload, '$.text')
        FROM large_records ORDER BY seq LIMIT 5 OFFSET 1022'''

    sql "CREATE DATABASE IF NOT EXISTS internal.mongodb_json_regression"
    sql "DROP VIEW IF EXISTS internal.mongodb_json_regression.json_view"
    sql '''CREATE VIEW internal.mongodb_json_regression.json_view AS
        SELECT seq, payload FROM mongodb_json_test.doris_mongodb_test.json_records'''
    order_qt_view_schema "DESC internal.mongodb_json_regression.json_view"
    order_qt_view_data "SELECT * FROM internal.mongodb_json_regression.json_view"
    sql "DROP TABLE IF EXISTS internal.mongodb_json_regression.json_materialized"
    sql '''CREATE TABLE internal.mongodb_json_regression.json_materialized
        DISTRIBUTED BY HASH(seq) BUCKETS 1 PROPERTIES ("replication_num" = "1") AS
        SELECT seq, payload FROM json_records'''
    order_qt_materialized_schema "DESC internal.mongodb_json_regression.json_materialized"
    order_qt_materialized_data "SELECT * FROM internal.mongodb_json_regression.json_materialized"
    order_qt_materialized_functions '''SELECT seq, json_type(payload, '$'),
        json_extract_string(payload, '$.city')
        FROM internal.mongodb_json_regression.json_materialized'''
    sql "DROP TABLE IF EXISTS internal.mongodb_json_regression.nested_json_materialized"
    test {
        sql '''CREATE TABLE internal.mongodb_json_regression.nested_json_materialized
            DISTRIBUTED BY HASH(seq) BUCKETS 1 PROPERTIES ("replication_num" = "1") AS
            SELECT seq, json_array FROM json_records'''
        exception "ARRAY unsupported sub-type: json"
    }
    test {
        sql "SELECT payload FROM invalid_json_records"
        exception "JSON"
    }
}
