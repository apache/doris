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

suite("test_mongodb_read_policy", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableMongoDBTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("mongodb_port")
    String user = context.config.otherConfigs.get("mongodb_user")
    String password = context.config.otherConfigs.get("mongodb_password")
    def original = sql("SELECT @@mongodb_read_preference, @@mongodb_read_concern")[0]
    def create = { String name, String uri ->
        sql "DROP CATALOG IF EXISTS ${name}"
        def properties = ["type": "mongodb", "mongodb.uri": uri, "mongodb.username": user,
            "mongodb.password": password, "mongodb.databases": "doris_mongodb_native",
            "mongodb.read_preference": "secondaryPreferred", "mongodb.read_concern": "local"]
        String values = properties.collect { key, value ->
            "\"${key}\" = '${value.toString().replace("\\", "\\\\").replace("'", "\\'")}'"
        }.join(",\n")
        sql "CREATE CATALOG ${name} PROPERTIES (${values})"
    }
    try {
        sql "SET mongodb_read_preference = 'inherit'"
        sql "SET mongodb_read_concern = 'inherit'"
        qt_defaults "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
        order_qt_show_variables "SHOW VARIABLES LIKE 'mongodb_read_%'"
        create("mongodb_read_policy", "mongodb://${host}:${port}")
        qt_catalog_defaults "SELECT count(*), sum(seq) FROM mongodb_read_policy.doris_mongodb_native.typed_records"
        for (String mode : ["primary", "primaryPreferred", "secondary", "secondaryPreferred", "nearest"]) {
            sql "SET mongodb_read_preference = '${mode}'"
            "qt_mode_${mode}" "SELECT count(*), sum(seq) FROM mongodb_read_policy.doris_mongodb_native.typed_records"
        }
        for (String level : ["local", "majority", "available"]) {
            sql "SET mongodb_read_concern = '${level}'"
            "qt_concern_${level}" "SELECT count(*), sum(seq) FROM mongodb_read_policy.doris_mongodb_native.typed_records"
        }
        sql "SET mongodb_read_preference = 'primary'"
        sql "SET mongodb_read_concern = 'majority'"
        qt_hint """SELECT /*+ SET_VAR(mongodb_read_preference='secondaryPreferred', mongodb_read_concern='local') */
            @@mongodb_read_preference, @@mongodb_read_concern, count(*), sum(seq)
            FROM mongodb_read_policy.doris_mongodb_native.typed_records"""
        qt_after_hint "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
        qt_hint_inherit """SELECT /*+ SET_VAR(mongodb_read_preference='inherit', mongodb_read_concern='inherit') */
            count(*), sum(seq) FROM mongodb_read_policy.doris_mongodb_native.typed_records"""
        qt_after_inherit_hint "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
        for (String mode : ["invalid", "secondary_preferred", ""]) {
            test {
                sql "SET mongodb_read_preference = '${mode}'"
                exception "mongodb_read_preference"
            }
        }
        for (String level : ["snapshot", "linearizable", "invalid", ""]) {
            test {
                sql "SET mongodb_read_concern = '${level}'"
                exception "mongodb_read_concern"
            }
        }
        test {
            sql """SELECT /*+ SET_VAR(mongodb_read_concern='snapshot') */
                count(*) FROM mongodb_read_policy.doris_mongodb_native.typed_records"""
            exception "mongodb_read_concern"
        }
        qt_after_invalid "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
        create("mongodb_read_policy_uri", "mongodb://${host}:${port}/?readPreference=secondaryPreferred"
            + "&readPreferenceTags=zone:east&maxStalenessSeconds=120&readConcernLevel=available")
        sql "SET mongodb_read_preference = 'inherit'"
        sql "SET mongodb_read_concern = 'inherit'"
        qt_uri_inherit "SELECT count(*) FROM mongodb_read_policy_uri.doris_mongodb_native.typed_records"
        test {
            sql """SELECT /*+ SET_VAR(mongodb_read_preference='primary') */
                count(*) FROM mongodb_read_policy_uri.doris_mongodb_native.typed_records"""
            exception "incompatible with URI"
        }
        qt_after_failed_hint "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
        sql "SET mongodb_read_preference = 'primary'"
        test {
            sql "SELECT count(*) FROM mongodb_read_policy_uri.doris_mongodb_native.typed_records"
            exception "incompatible with URI"
        }
        qt_hint_recover """SELECT /*+ SET_VAR(mongodb_read_preference='secondaryPreferred', mongodb_read_concern='local') */
            count(*) FROM mongodb_read_policy_uri.doris_mongodb_native.typed_records"""
        qt_after_recover "SELECT @@mongodb_read_preference, @@mongodb_read_concern"
    } finally {
        sql "SET mongodb_read_preference = '${original[0]}'"
        sql "SET mongodb_read_concern = '${original[1]}'"
    }
}
