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
    // The listings are recorded rather than spot-checked: a table that appears
    // out of nowhere is as much a bug as one that goes missing, and only the
    // whole list catches the first kind. In particular a lake table must be
    // listed ONCE, under its own name -- the $lake reader is a way of reading it,
    // not a second table, and a listing that showed both would double every lake
    // table for anything walking the schema.
    order_qt_databases """show databases from ${catalogName}"""

    sql """switch ${catalogName}"""
    sql """use fluss_test"""

    order_qt_tables """show tables"""

    // --- schema mapping ----------------------------------------------------
    // desc is recorded UNSORTED: column order is part of what is being checked,
    // and sorting the rows would throw it away. Types are pinned here too, so a
    // type-mapping change shows up before any query runs.
    qt_desc_log_basic """desc log_basic"""

    // One column per fluss type the connector maps, plus the id column.
    qt_desc_log_types """desc log_types"""

    // The partition key is an ordinary column of the table, not a hidden one.
    qt_desc_log_part """desc log_part"""

    // Primary-key columns keep their position; the connector reports every
    // column as a key column, which is how Doris models external tables.
    qt_desc_pk_basic """desc pk_basic"""

    // --- comments survive the metadata mapping -----------------------------
    // Column comments live on the fluss schema, not on the row type: reading
    // the row type instead would silently drop every one of them.
    //
    // Not recorded into a .out on purpose: the statement carries the catalog's
    // properties, including this environment's bootstrap address, so a recorded
    // baseline would be tied to the machine that generated it.
    def createTableRows = sql """show create table log_basic"""
    def createTable = createTableRows[0][1].toString()
    assertTrue(createTable.contains("row id"), "column comment lost: ${createTable}")
    assertTrue(createTable.contains("fluss log table for regression"),
            "table comment lost: ${createTable}")

    // --- refresh keeps the catalog usable ----------------------------------
    // Recorded again rather than compared with the listing above: the two blocks
    // have to stay identical in the baseline, which is the same statement made
    // in a way that also survives someone adding a fixture table.
    sql """refresh catalog ${catalogName}"""
    order_qt_tables_after_refresh """show tables"""

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
