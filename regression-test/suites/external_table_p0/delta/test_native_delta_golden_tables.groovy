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

suite("test_native_delta_golden_tables", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        throw new IllegalStateException("DORIS_HOME is required for the local Delta golden tables")
    }
    def data = new File(dorisHome, "regression-test/data/external_table_p0/delta")
    def manifest = new groovy.json.JsonSlurper().parse(new File(data, "golden-tables.json"))
    def archive = new File(data, "golden-tables.zip")
    def digest = java.security.MessageDigest.getInstance("SHA-256").digest(archive.bytes)
            .encodeHex().toString()
    if (digest != manifest.archive_sha256) {
        throw new IllegalStateException("Delta golden tables archive does not match the pinned manifest")
    }
    // Keep fixtures for debugging, including URI-escaped spaces, '+', '%' and '#' in the root.
    def root = java.nio.file.Files.createTempDirectory(new File(dorisHome, "output").toPath(),
            "delta-golden + % # ")
    new java.util.zip.ZipFile(archive).withCloseable { zip ->
        zip.entries().each { entry ->
            def target = root.resolve(entry.name)
            java.nio.file.Files.createDirectories(target.parent)
            zip.getInputStream(entry).withCloseable { input -> java.nio.file.Files.copy(input, target) }
        }
    }
    sql "SET enable_file_scanner_v2 = true"
    sql "SET time_zone = '+00:00'"

    def readQuery = { fixture, String table ->
        if (fixture.name.startsWith("parquet-decimal-dictionaries")) {
            // The upstream generator writes each id in [0, 1000000) exactly once with these
            // decimal values. Check every row and the complete id domain without a million-line out.
            return """SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), SUM(id),
                SUM(IF(col1 IS NULL OR col2 IS NULL OR col3 IS NULL OR
                       col1 <> id % 5 OR col2 <> id % 6 OR col3 <> id % 2, 1, 0)) FROM ${table}"""
        }
        if (fixture.name == "parquet-decimal-type") {
            // expand(n) = n * 100001; the upstream generator applies expand three times to col3.
            String n = "CAST(IF(id % 85 = 0, id, IF(id % 33 = 0, -id, id)) AS DECIMAL(6,0))"
            return """SELECT COUNT(*), COUNT(DISTINCT id), MIN(id), MAX(id), SUM(id),
                SUM(IF(col1 IS NULL OR col2 IS NULL OR col3 IS NULL OR
                       col1 <> ${n} * 0.1 OR
                       col2 <> IF(id % 85 = 0, id, ${n} * 1.00001) OR
                       col3 <> IF(id % 85 = 0, id, ${n} * 10000300003.00001), 1, 0)) FROM ${table}"""
        }
        String columns = fixture.columns.collect { column ->
            String quoted = "`${column.name.replace('`', '``')}`"
            column.binary ? "HEX(${quoted})" : quoted
        }.join(", ")
        return "SELECT ${columns} FROM ${table}"
    }

    manifest.cases.eachWithIndex { fixture, index ->
        String catalog = "test_native_delta_golden_${index}"
        String location = root.resolve(fixture.name).toUri().toASCIIString().replace("+", "%2B")
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            'type' = 'delta', 'delta.catalog.type' = 'path',
            'delta.database' = 'default', 'delta.table' = 'golden',
            'delta.table.path' = '${location}', 'test_connection' = 'false')"""
        String table = "${catalog}.`default`.golden"
        if (fixture.error) {
            test {
                sql "SELECT * FROM ${table}"
                exception fixture.error
            }
        } else {
            "order_qt_${fixture.tag}_latest"(readQuery(fixture, table))
            fixture.versions.each { version ->
                String historical = "${table} FOR VERSION AS OF ${version.version}"
                if (version.error) {
                    test {
                        sql "SELECT * FROM ${historical}"
                        exception version.error
                    }
                } else {
                    "order_qt_${fixture.tag}_v${version.version}"(readQuery(fixture, historical))
                }
            }
        }
        if (fixture.error == "[NOT_FOUND]") {
            // Both scanners share the committed-snapshot rule, even when the general BE
            // ignore_not_found_file_in_external_table option is enabled (its default).
            sql "SET enable_file_scanner_v2 = false"
            test {
                sql "SELECT * FROM ${table}"
                exception "[NOT_FOUND]"
            }
            sql "SET enable_file_scanner_v2 = true"
        }
    }
    logger.info("Validated ${manifest.cases.size()} upstream Delta fixtures from ${manifest.commit}")

    String createdLocation = root.resolve("created + literal%2B #").toUri().toASCIIString().replace("+", "%2B")
    sql "DROP CATALOG IF EXISTS test_native_delta_golden_uri_write"
    sql """CREATE CATALOG test_native_delta_golden_uri_write PROPERTIES (
        'type' = 'delta', 'delta.catalog.type' = 'path',
        'delta.database' = 'default', 'delta.table' = 'golden',
        'delta.table.path' = '${createdLocation}', 'delta.write.enabled' = 'true',
        'test_connection' = 'false')"""
    sql "CREATE TABLE test_native_delta_golden_uri_write.`default`.golden (id BIGINT NOT NULL, payload STRING)"
    sql "INSERT INTO test_native_delta_golden_uri_write.`default`.golden VALUES (1, 'first')"
    sql "INSERT INTO test_native_delta_golden_uri_write.`default`.golden VALUES (2, 'second')"
    order_qt_escaped_uri_append "SELECT * FROM test_native_delta_golden_uri_write.`default`.golden"
    order_qt_escaped_uri_history """SELECT * FROM test_native_delta_golden_uri_write.`default`.golden
        FOR VERSION AS OF 1"""
}
