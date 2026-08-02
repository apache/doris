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

// Catalog-level checks for the fluss connector: what a user sees before any
// data is read. Fixtures come from
// docker/thirdparties/docker-compose/fluss/sql/init.sql.
suite("test_fluss_catalog", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableFlussTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String coordinatorPort = context.config.otherConfigs.get("fluss_coordinator_port")
    String bootstrapServers = "${externalEnvIp}:${coordinatorPort}"
    String catalogName = "test_fluss_catalog"

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog ${catalogName} properties (
            "type" = "fluss",
            "fluss.bootstrap.servers" = "${bootstrapServers}"
        );
    """

    // --- the fixture database and its tables are visible -------------------
    // Every result is bound to a variable before it is chained on: in a Groovy
    // command expression `sql """..."""​.collect {}` would collect over the
    // string, not over the rows.
    def databaseRows = sql """show databases from ${catalogName}"""
    def databases = databaseRows.collect { it[0] }
    assertTrue(databases.contains("fluss_test"),
            "fluss_test missing from ${catalogName}: ${databases}")

    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    def tableRows = sql """show tables"""
    def tables = tableRows.collect { it[0] }
    for (String expected : ["log_basic", "log_types", "log_part", "pk_basic", "pk_types"]) {
        assertTrue(tables.contains(expected), "table ${expected} missing: ${tables}")
    }

    // --- schema mapping ----------------------------------------------------
    // desc rows are [Field, Type, Null, Key, Default, Extra].
    def descLogBasic = sql """desc log_basic"""
    assertEquals(["id", "name", "price"], descLogBasic.collect { it[0] })

    // One column per fluss type the connector maps, plus the id column. A
    // dropped or duplicated column shows up here before any query runs.
    def descLogTypes = sql """desc log_types"""
    assertEquals(["id", "f_boolean", "f_tinyint", "f_smallint", "f_int", "f_bigint",
                  "f_float", "f_double", "f_decimal", "f_char", "f_string", "f_binary",
                  "f_bytes", "f_date", "f_timestamp", "f_timestamp_ltz", "f_array",
                  "f_map", "f_row"],
            descLogTypes.collect { it[0] })

    // The partition key is an ordinary column of the table, not a hidden one.
    def descLogPart = sql """desc log_part"""
    assertEquals(["id", "name", "dt"], descLogPart.collect { it[0] })

    // Primary-key columns keep their position; the connector reports every
    // column as a key column, which is how Doris models external tables.
    def descPkBasic = sql """desc pk_basic"""
    assertEquals(["id", "name", "score"], descPkBasic.collect { it[0] })

    // --- comments survive the metadata mapping -----------------------------
    // Column comments live on the fluss schema, not on the row type: reading
    // the row type instead would silently drop every one of them.
    def createTableRows = sql """show create table log_basic"""
    def createTable = createTableRows[0][1].toString()
    assertTrue(createTable.contains("row id"), "column comment lost: ${createTable}")
    assertTrue(createTable.contains("fluss log table for regression"),
            "table comment lost: ${createTable}")

    // --- refresh keeps the catalog usable ----------------------------------
    sql """refresh catalog ${catalogName}"""
    def refreshedRows = sql """show tables"""
    def tablesAfterRefresh = refreshedRows.collect { it[0] }
    assertEquals(tables.sort(), tablesAfterRefresh.sort())

    sql """switch internal"""
    sql """drop catalog ${catalogName}"""

    // --- property validation happens at catalog creation -------------------
    test {
        sql """
            create catalog test_fluss_no_bootstrap properties (
                "type" = "fluss"
            );
        """
        exception "Required property 'fluss.bootstrap.servers' is missing"
    }

    test {
        sql """
            create catalog test_fluss_bad_port properties (
                "type" = "fluss",
                "fluss.bootstrap.servers" = "${externalEnvIp}:not-a-port"
            );
        """
        exception "expected a number between 1 and 65535"
    }

    test {
        sql """
            create catalog test_fluss_bad_union_mode properties (
                "type" = "fluss",
                "fluss.bootstrap.servers" = "${bootstrapServers}",
                "fluss.union_read.mode" = "sometimes"
            );
        """
        exception "expected one of auto, required, disabled"
    }
}
