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

suite("test_catalog_credential_masking", "p0") {
    String reader = "catalog_masking_reader"
    String denied = "catalog_masking_denied"
    String userPassword = "catalog_masking_login_fixture"
    String warehouse = "file:///tmp/catalog_masking_warehouse"
    def literal = { value -> "'${value.toString().replace("'", "''")}'" }
    def fixtures = [
        [name: "catalog_masking_iceberg_jdbc", properties: [
            "type": "iceberg", "iceberg.catalog.type": "jdbc", "warehouse": warehouse,
            "iceberg.jdbc.catalog_name": "catalog_masking",
            "uri": "jdbc:postgresql://127.0.0.1:1/metadata?user=reader&password=cm_iceberg_uri",
            "iceberg.jdbc.uri": "jdbc:postgresql://127.0.0.1:1/metadata?password=cm_iceberg_alias",
            "iceberg.jdbc.password": "cm_iceberg_password", "test_connection": "false"
        ], sensitive: ["uri", "iceberg.jdbc.uri", "iceberg.jdbc.password"]],
        [name: "catalog_masking_paimon_jdbc", properties: [
            "type": "paimon", "paimon.catalog.type": "jdbc", "warehouse": warehouse,
            "paimon.jdbc.uri": "jdbc:mysql://127.0.0.1:1/metadata?password=cm_paimon_uri",
            "paimon.jdbc.password": "cm_paimon_password", "jdbc.password": "cm_paimon_alias",
            "test_connection": "false"
        ], sensitive: ["paimon.jdbc.uri", "paimon.jdbc.password", "jdbc.password"]],
        [name: "catalog_masking_paimon_rest", properties: [
            "type": "paimon", "paimon.catalog.type": "rest", "warehouse": warehouse,
            "paimon.rest.uri": "http://reader:cm_paimon_rest@127.0.0.1:1",
            "paimon.rest.token.provider": "dlf", "paimon.rest.dlf.access-key-id": "cm_dlf_key",
            "paimon.rest.dlf.access-key-secret": "cm_dlf_secret", "test_connection": "false"
        ], sensitive: ["paimon.rest.uri", "paimon.rest.dlf.access-key-id", "paimon.rest.dlf.access-key-secret"]],
        [name: "catalog_masking_iceberg_rest", properties: [
            "type": "iceberg", "iceberg.catalog.type": "rest", "warehouse": warehouse,
            "iceberg.rest.uri": "http://reader:cm_iceberg_rest@127.0.0.1:1", "test_connection": "false"
        ], sensitive: ["iceberg.rest.uri"]]
    ]

    try {
        sql "DROP USER IF EXISTS '${reader}'@'%'"
        sql "DROP USER IF EXISTS '${denied}'@'%'"
        sql "CREATE USER '${reader}'@'%' IDENTIFIED BY '${userPassword}'"
        sql "CREATE USER '${denied}'@'%' IDENTIFIED BY '${userPassword}'"
        fixtures.each { fixture ->
            sql "DROP CATALOG IF EXISTS ${fixture.name}"
            String properties = fixture.properties.collect { key, value -> "${literal(key)} = ${literal(value)}" }.join(", ")
            sql "CREATE CATALOG ${fixture.name} PROPERTIES (${properties})"
            sql "GRANT SELECT_PRIV ON ${fixture.name}.*.* TO '${reader}'@'%'"
        }

        def grantRows = sql_return_maparray "SHOW GRANTS FOR '${reader}'@'%'"
        String grants = grantRows.collectMany { row ->
            row.findAll { key, value -> key.toUpperCase().endsWith("PRIVS") &&
                !key.equalsIgnoreCase("WorkloadGroupPrivs") }.values()
        }.join("\n").toUpperCase()
        Set privileges = grants.findAll(/[A-Z][A-Z_]*_PRIV/).toSet()
        boolean selectOnly = privileges == (["SELECT_PRIV"] as Set)
        quickTest("select_only_grants", "SELECT ${selectOnly ? 1 : 0}", "SELECT 1")
        // Every new user inherits USAGE on the default workload group; it grants no catalog access.
        String workloadGrants = grantRows.collectMany { row ->
            row.findAll { key, value -> key.equalsIgnoreCase("WorkloadGroupPrivs") }.values()
        }.join("\n").toUpperCase()
        boolean defaultUsageOnly = workloadGrants.findAll(/[A-Z][A-Z_]*_PRIV/).toSet() ==
            (["USAGE_PRIV"] as Set) && workloadGrants.contains("NORMAL:")
        quickTest("default_workload_usage", "SELECT ${defaultUsageOnly ? 1 : 0}", "SELECT 1")

        // Preserve the connection's TLS/options, but do not select a database the reader was not granted.
        String readerJdbcUrl = context.config.jdbcUrl.replaceFirst(/\/[^\/?]+(\?|$)/, '/$1')
        int checkedEntrances = 0
        connectToDoris(reader, userPassword, readerJdbcUrl) {
            quickTest("reader_identity", "SELECT CAST(LOCATE('${reader}', CURRENT_USER()) > 0 AS INT)",
                "SELECT CAST(1 AS INT)")
            fixtures.each { fixture ->
                Map visible = ["type": fixture.properties.type, "warehouse": warehouse]
                List secrets = fixture.sensitive.collect { fixture.properties[it] }
                def ddlRows = sql "SHOW CREATE CATALOG ${fixture.name}"
                String ddl = ddlRows.size() == 1 ? ddlRows[0][1].toString() : ""
                boolean ddlMasked = ddlRows.size() == 1 && fixture.sensitive.every { key ->
                    ddl.contains("\"${key}\" = \"*XXX\"")
                } && visible.every { key, value -> ddl.contains("\"${key}\" = \"${value}\"") } &&
                    secrets.every { value -> !ddl.contains(value) }
                quickTest("${fixture.name}_show_create", "SELECT ${ddlMasked ? 1 : 0}", "SELECT 1")

                def propertyRows = sql "SHOW CATALOG ${fixture.name}"
                Map shown = propertyRows.collectEntries { [it[0], it[1]] }
                boolean propertiesMasked = !propertyRows.isEmpty() &&
                    fixture.sensitive.every { key -> shown[key] == "*XXX" } &&
                    visible.every { key, value -> shown[key] == value } &&
                    secrets.every { value -> !propertyRows.toString().contains(value) }
                quickTest("${fixture.name}_show_catalog", "SELECT ${propertiesMasked ? 1 : 0}", "SELECT 1")

                def tvfRows = sql "SELECT Property, Value FROM catalogs() WHERE CatalogName = '${fixture.name}'"
                boolean tvfHasNoSecrets = !tvfRows.isEmpty() &&
                    secrets.every { value -> !tvfRows.toString().contains(value) }
                quickTest("${fixture.name}_catalogs_no_plaintext", "SELECT ${tvfHasNoSecrets ? 1 : 0}", "SELECT 1")
                Map expected = fixture.sensitive.collectEntries { [(it): "*XXX"] } + visible
                String keys = expected.keySet().collect { literal(it) }.join(", ")
                String reference = expected.collect { key, value ->
                    "SELECT ${literal(key)} AS Property, ${literal(value)} AS Value"
                }.join(" UNION ALL ") + " ORDER BY Property"
                // catalogs() crosses the actual FE/BE metadata scan path; the reference is independent SQL.
                quickTest("${fixture.name}_catalogs",
                    "SELECT Property, Value FROM catalogs() WHERE CatalogName = '${fixture.name}' " +
                        "AND Property IN (${keys}) ORDER BY Property", reference)
                checkedEntrances += 3
            }
        }

        quickTest("all_reader_entrances_executed", "SELECT ${checkedEntrances}", "SELECT ${fixtures.size() * 3}")

        int deniedEntrances = 0
        connectToDoris(denied, userPassword, readerJdbcUrl) {
            fixtures.each { fixture ->
                test {
                    sql "SHOW CREATE CATALOG ${fixture.name}"
                    exception "denied"
                }
                test {
                    sql "SHOW CATALOG ${fixture.name}"
                    exception "denied"
                }
                deniedEntrances += 2
            }
            String catalogs = fixtures.collect { literal(it.name) }.join(", ")
            quickTest("ungranted_catalogs", "SELECT COUNT(*) FROM catalogs() WHERE CatalogName IN (${catalogs})",
                "SELECT CAST(0 AS BIGINT)")
            deniedEntrances++
        }
        quickTest("all_denied_entrances_executed", "SELECT ${deniedEntrances}",
            "SELECT ${fixtures.size() * 2 + 1}")
    } finally {
        fixtures.each { fixture -> sql "DROP CATALOG IF EXISTS ${fixture.name}" }
        sql "DROP USER IF EXISTS '${reader}'@'%'"
        sql "DROP USER IF EXISTS '${denied}'@'%'"
    }
}
