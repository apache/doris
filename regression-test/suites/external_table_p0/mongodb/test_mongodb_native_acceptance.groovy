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

suite("test_mongodb_native_acceptance", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoDBTest"))) {
        return
    }
    sql "SWITCH internal"
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    String user = context.config.otherConfigs.get("mongodb_user")
    String password = context.config.otherConfigs.get("mongodb_password")
    def quote = { value -> "'${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'" }
    def create = { String name, Map extra ->
        sql "DROP CATALOG IF EXISTS ${name}"
        Map properties = ["type": "mongodb", "mongodb.uri": "mongodb://${host}:${port}",
            "mongodb.username": user, "mongodb.password": password, "mongodb.databases": "doris_mongodb_native"]
        properties.putAll(extra)
        sql "CREATE CATALOG ${name} PROPERTIES (${properties.collect { k, v -> "${quote(k)}=${quote(v)}" }.join(',')})"
    }

    create("mongodb_native_auth_recovery", ["mongodb.password": password + "_invalid"])
    test {
        sql "SELECT count(*) FROM mongodb_native_auth_recovery.doris_mongodb_native.large_records"
        exception "MongoDB metadata request failed"
    }
    sql "ALTER CATALOG mongodb_native_auth_recovery SET PROPERTIES ('mongodb.password'=${quote(password)})"
    sql "REFRESH CATALOG mongodb_native_auth_recovery"
    qt_auth_recovery """SELECT count(*), sum(seq)
        FROM mongodb_native_auth_recovery.doris_mongodb_native.large_records"""

    for (Map unsupported : [["mongodb.read_concern": "snapshot"], ["mongodb.split_strategy": "range"]]) {
        test {
            sql """CREATE CATALOG mongodb_native_unsupported PROPERTIES (
                'type'='mongodb', 'mongodb.uri'='mongodb://${host}:${port}',
                ${unsupported.collect { k, v -> "${quote(k)}=${quote(v)}" }.join(',')})"""
            exception(unsupported.containsKey("mongodb.split_strategy")
                    ? "Unknown MongoDB catalog property" : "Invalid or unsupported mongodb.read_concern")
        }
    }

    Map schema = ["doris_mongodb_native.record_view": [columns: [
        [name: "seq", path: ["seq"], doris_type: "BIGINT", bson_types: ["long"]],
        [name: "label", path: ["label"], doris_type: "STRING", bson_types: ["string"]]
    ]]]
    create("mongodb_native_acceptance", ["mongodb.schema": JsonOutput.toJson(schema)])
    order_qt_view_local_case """SELECT seq, label FROM mongodb_native_acceptance.doris_mongodb_native.record_view
        WHERE label = 'VIEW-ALPHA'"""
    order_qt_view_local_null_logic """SELECT seq, label FROM mongodb_native_acceptance.doris_mongodb_native.record_view
        WHERE label NOT IN ('view-alpha', NULL)"""
    order_qt_view_projection "SELECT label, seq FROM mongodb_native_acceptance.doris_mongodb_native.record_view"
    qt_aggregate_without_source_fields "SELECT count(*) FROM mongodb_native_acceptance.doris_mongodb_native.measurements"

    sql "CREATE DATABASE IF NOT EXISTS mongodb_native_acceptance_db"
    sql "DROP TABLE IF EXISTS mongodb_native_acceptance_db.copied_records"
    sql """CREATE TABLE mongodb_native_acceptance_db.copied_records (
        seq BIGINT, payload STRING
    ) DUPLICATE KEY(seq) DISTRIBUTED BY HASH(seq) BUCKETS 1 PROPERTIES ('replication_num'='1')"""
    sql """INSERT INTO mongodb_native_acceptance_db.copied_records
        SELECT seq, payload FROM mongodb_native_acceptance.doris_mongodb_native.large_records"""
    qt_materialized_multiset """SELECT count(*), count(distinct seq), min(seq), max(seq), sum(seq)
        FROM mongodb_native_acceptance_db.copied_records"""
    qt_materialized_differential """SELECT count(*) FROM (
        (SELECT seq, payload FROM mongodb_native_acceptance_db.copied_records
            EXCEPT SELECT seq, payload FROM mongodb_native_acceptance.doris_mongodb_native.large_records)
        UNION ALL
        (SELECT seq, payload FROM mongodb_native_acceptance.doris_mongodb_native.large_records
            EXCEPT SELECT seq, payload FROM mongodb_native_acceptance_db.copied_records)
    ) differences"""
}
