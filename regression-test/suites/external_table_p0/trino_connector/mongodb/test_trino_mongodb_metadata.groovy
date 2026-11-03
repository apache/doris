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

suite("test_trino_mongodb_metadata", "p0,external") {
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
    sql "DROP CATALOG IF EXISTS mongodb_metadata_test"
    sql """CREATE CATALOG mongodb_metadata_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.cursor-batch-size" = "127"
    )"""
    sql "SWITCH mongodb_metadata_test"
    sql "USE doris_mongodb_test"

    order_qt_databases """SHOW DATABASES"""

    order_qt_tables """SHOW TABLES"""

    order_qt_inferred_schema """DESC inferred_records"""

    order_qt_inferred_data """SELECT from_binary(_id), seq, label FROM inferred_records"""

    order_qt_explicit_schema """DESC owners"""

    order_qt_explicit_data """SELECT from_binary(_id), label, weight FROM owners"""

    order_qt_refresh_before """SELECT seq, from_binary(_id) FROM records"""

    sql "REFRESH TABLE records"
    order_qt_refresh_table "SELECT seq, from_binary(_id) FROM records"
    sql "REFRESH DATABASE mongodb_metadata_test.doris_mongodb_test"
    order_qt_refresh_database "SELECT seq, from_binary(_id) FROM records"
    sql "REFRESH CATALOG mongodb_metadata_test"
    order_qt_refresh_catalog "SELECT seq, from_binary(_id) FROM records"
    test {
        sql "SELECT payload FROM invalid_schema"
        exception "Cannot transform unknown Trino type"
    }
    test {
        sql "SELECT * FROM missing_collection"
        exception "does not exist"
    }
    // A separate metadata collection changes the declared SQL schema without changing BSON data.
    sql "DROP CATALOG IF EXISTS mongodb_alternate_schema_test"
    sql """CREATE CATALOG mongodb_alternate_schema_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://${encodedUser}:${encodedPassword}@${host}:${port}/?authSource=admin",
        "trino.mongodb.schema-collection" = "_schema_alternate"
    )"""
    order_qt_alternate_schema "DESC mongodb_alternate_schema_test.doris_mongodb_test.records"
    order_qt_alternate_data """SELECT from_binary(_id), seq, name
        FROM mongodb_alternate_schema_test.doris_mongodb_test.records"""

    sql "DROP CATALOG IF EXISTS mongodb_readonly_test"
    sql """CREATE CATALOG mongodb_readonly_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://mongodb_readonly:mongodb_readonly@${host}:${port}/?authSource=admin"
    )"""
    order_qt_readonly_explicit_schema """SELECT seq, from_binary(_id)
        FROM mongodb_readonly_test.doris_mongodb_test.records"""
    test {
        sql "SELECT * FROM mongodb_readonly_test.doris_mongodb_test.readonly_inferred_records"
        exception "not authorized"
    }
    sql "DROP CATALOG IF EXISTS mongodb_bad_auth_test"
    sql """CREATE CATALOG mongodb_bad_auth_test PROPERTIES (
        "type" = "trino-connector",
        "trino.connector.name" = "mongodb",
        "trino.mongodb.connection-url" =
            "mongodb://mongodb_test:incorrect@${host}:${port}/?authSource=admin&serverSelectionTimeoutMS=2000"
    )"""
    test {
        sql "SHOW TABLES FROM mongodb_bad_auth_test.doris_mongodb_test"
        exception "authenticating MongoCredential"
    }
    sql "DROP CATALOG mongodb_bad_auth_test"
}
