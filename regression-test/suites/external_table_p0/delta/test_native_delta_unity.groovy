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

suite("test_native_delta_unity", "p0,external") {
    def dorisHome = System.getenv("DORIS_HOME") ?: System.getProperty("DORIS_HOME")
    if (dorisHome == null || dorisHome.trim().isEmpty()) {
        return
    }

    def unityReadOnlyDirectory = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-unity-read-")
    def tablePath = unityReadOnlyDirectory.toUri().toString()
    def catalogManagedDirectory = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-catalog-managed-")
    def unityExternalWriteDirectory = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-unity-write-")
    def sourceTable = new File(dorisHome,
            "samples/datalake/deltalake_and_kudu/data/customer").toPath()
    // Keep even rejected writes away from the shared sample and the writable table's history.
    [unityReadOnlyDirectory, unityExternalWriteDirectory].each { directory ->
        java.nio.file.Files.walk(sourceTable).withCloseable { paths ->
            paths.forEach { source ->
                def relative = sourceTable.relativize(source)
                def destination = directory.resolve(relative)
                if (java.nio.file.Files.isDirectory(source)) {
                    java.nio.file.Files.createDirectories(destination)
                } else {
                    java.nio.file.Files.copy(source, destination,
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING)
                }
            }
        }
    }
    def catalogManagedSource = new File(dorisHome,
            "fe/fe-connector/fe-connector-delta/src/test/resources/delta/catalog_managed_table")
            .toPath()
    java.nio.file.Files.walk(catalogManagedSource).withCloseable { paths ->
        paths.forEach { source ->
            def relative = catalogManagedSource.relativize(source)
            def destination = catalogManagedDirectory.resolve(relative)
            if (java.nio.file.Files.isDirectory(source)) {
                java.nio.file.Files.createDirectories(destination)
            } else {
                java.nio.file.Files.copy(source, destination,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }
    // Generate real id BIGINT files with non-null values matching the catalog-managed log schema.
    // Customer Parquet files have no id column and cannot serve as this table's data.
    def jsonSlurper = new groovy.json.JsonSlurper()
    def seedTablePath = java.nio.file.Files.createTempDirectory(
            "doris-native-delta-unity-seed-").resolve("ids")
    sql "DROP CATALOG IF EXISTS test_native_delta_unity_seed"
    sql """
        CREATE CATALOG test_native_delta_unity_seed PROPERTIES (
            'type' = 'delta',
            'delta.catalog.type' = 'path',
            'delta.database' = 'default',
            'delta.table' = 'ids',
            'delta.table.path' = '${seedTablePath.toUri()}',
            'delta.write.enabled' = 'true',
            'test_connection' = 'false'
        )
    """
    sql "CREATE TABLE test_native_delta_unity_seed.`default`.ids (id BIGINT NOT NULL)"
    def catalogManagedDataFiles = ["part-00001.parquet", "part-00002.parquet"]
    catalogManagedDataFiles.eachWithIndex { fileName, index ->
        sql "INSERT INTO test_native_delta_unity_seed.`default`.ids VALUES (${index + 1})"
        def commitPath = seedTablePath.resolve("_delta_log")
                .resolve(String.format("%020d.json", index + 1L))
        def adds = java.nio.file.Files.readAllLines(commitPath)
                .collect { jsonSlurper.parseText(it) }.findAll { it.add != null }.collect { it.add }
        if (adds.size() != 1) {
            throw new IllegalStateException("The one-row fixture insert must produce exactly one data file")
        }
        java.nio.file.Files.copy(seedTablePath.resolve(adds[0].path),
                catalogManagedDirectory.resolve(fileName))
    }
    def stagedCommitDirectory = catalogManagedDirectory.resolve("_delta_log/_staged_commits")
    java.nio.file.Files.list(stagedCommitDirectory).withCloseable { commits ->
        commits.forEach { commit ->
            def updatedLines = java.nio.file.Files.readAllLines(commit).collect { line ->
                def action = jsonSlurper.parseText(line)
                if (action.add != null) {
                    action.add.size = java.nio.file.Files.size(
                            catalogManagedDirectory.resolve(action.add.path))
                }
                groovy.json.JsonOutput.toJson(action)
            }
            java.nio.file.Files.write(commit, updatedLines)
        }
    }
    def catalogManagedPath = catalogManagedDirectory.toUri().toString()
    def catalogManagedTableId = "c79de738-d13c-44a5-8e75-8435123d60c7"
    // Only catalog-accepted commits are visible. A staged file alone is not a published commit.
    def acceptedCommits = new java.util.concurrent.ConcurrentSkipListMap<Long, Map>()
    java.nio.file.Files.list(stagedCommitDirectory).withCloseable { files ->
        files.filter { it.fileName.toString().endsWith(".json") }.forEach { commit ->
            def fileName = commit.fileName.toString()
            def version = Long.parseLong(fileName.substring(0, 20))
            acceptedCommits.put(version, [version: version, timestamp: 1700000000000L + version,
                    "file-name": fileName, "file-size": java.nio.file.Files.size(commit),
                    "file-modification-timestamp": 1700000000000L + version])
        }
    }
    def rejectCatalogManagedCommit = new java.util.concurrent.atomic.AtomicBoolean(false)
    def catalogManagedResponse = {
        def commits = new ArrayList(acceptedCommits.values())
        def latestVersion = commits.isEmpty() ? 0L : commits.last().version
        groovy.json.JsonOutput.toJson([
                metadata: [etag: "catalog-managed-etag-${latestVersion}",
                        "table-type": "MANAGED", "table-uuid": catalogManagedTableId,
                        location: catalogManagedPath, "partition-columns": [], properties: [
                                "delta.feature.catalogManaged": "supported",
                                "delta.enableInCommitTimestamps": "true"],
                        "last-commit-version": 0],
                commits: commits, "latest-table-version": latestVersion])
    }
    def token = "native-delta-unity-test-token"
    def server = com.sun.net.httpserver.HttpServer.create(
            new java.net.InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/") { exchange ->
        def sendJson = { int status, String body ->
            def bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(status, bytes.length)
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        if (exchange.requestHeaders.getFirst("Authorization") != "Bearer ${token}") {
            sendJson(401, '{"error_code":"UNAUTHENTICATED"}')
            return
        }

        def path = exchange.requestURI.path
        if (path == "/api/2.1/unity-catalog/delta/v1/config") {
            sendJson(200, '{"endpoints":['
                    + '"GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}",'
                    + '"GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials"],'
                    + '"protocol-version":"1.0"}')
        } else if (path == "/api/2.1/unity-catalog/schemas") {
            sendJson(200, '{"schemas":[{"name":"default","catalog_name":"main"}]}')
        } else if (path == "/api/2.1/unity-catalog/tables") {
            def tables = [customer: "EXTERNAL", catalog_managed: "MANAGED",
                    managed_customer: "MANAGED", unity_external_write: "EXTERNAL", blocked: "EXTERNAL"]
            sendJson(200, groovy.json.JsonOutput.toJson([tables: tables.collect { name, tableType ->
                def capabilities = name == "blocked" ? ["OTHER_CAPABILITY"]
                        : ["HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT"]
                if (name in ["catalog_managed", "unity_external_write"]) {
                    capabilities.add("HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT")
                }
                [name: name, catalog_name: "main", schema_name: "default", table_type: tableType,
                        data_source_format: "DELTA", securable_kind_manifest: [capabilities: capabilities]]
            }]))
        } else if (path.endsWith("/tables/customer")) {
            sendJson(200, '{"metadata":{"etag":"test-etag",'
                    + '"table-type":"EXTERNAL",'
                    + '"table-uuid":"421eb35b-e9ec-44ed-92fd-25e0fda91036",'
                    + '"location":"' + tablePath + '",'
                    + '"partition-columns":[],"last-commit-version":0},'
                    + '"commits":[],"latest-table-version":0}')
        } else if (path.endsWith("/tables/managed_customer")) {
            sendJson(200, '{"metadata":{"etag":"managed-etag",'
                    + '"table-type":"MANAGED",'
                    + '"table-uuid":"0e5e167d-b97c-4fe9-af31-4e06f57a82a4",'
                    + '"location":"' + tablePath + '",'
                    + '"partition-columns":[],"last-commit-version":0},'
                    + '"commits":[],"latest-table-version":0}')
        } else if (path.endsWith("/tables/unity_external_write")) {
            def latestVersion = java.nio.file.Files.list(
                    unityExternalWriteDirectory.resolve("_delta_log")).withCloseable { files ->
                def versions = files.filter { file ->
                    file.fileName.toString() ==~ /[0-9]{20}\.json/
                }.collect { file ->
                    Long.parseLong(file.fileName.toString().substring(0, 20))
                }
                versions.isEmpty() ? 0L : versions.max()
            }
            sendJson(200, groovy.json.JsonOutput.toJson([
                    metadata: [etag: "unity-write-etag-${latestVersion}",
                            "table-type": "EXTERNAL",
                            "table-uuid": "7fbe2c6d-ec90-43be-9a66-38d31c7de20d",
                            location: unityExternalWriteDirectory.toUri().toString(),
                            "partition-columns": [], "last-commit-version": latestVersion],
                    commits: [], "latest-table-version": latestVersion]))
        } else if (path.endsWith("/tables/catalog_managed")) {
            if (!exchange.requestMethod.equalsIgnoreCase("GET")) {
                def request = jsonSlurper.parseText(exchange.requestBody.getText("UTF-8"))
                def newCommits = request.updates.findAll { it.action == "add-commit" }.collect { it.commit }
                if (!newCommits.isEmpty() && rejectCatalogManagedCommit.get()) {
                    sendJson(403, '{"error_code":"PERMISSION_DENIED",'
                            + '"message":"fixture rejected catalog-managed commit"}')
                    return
                }
                for (def commit : newCommits) {
                    if (commit.version != acceptedCommits.lastKey() + 1) {
                        sendJson(409, '{"error_code":"CONFLICT","message":"Unexpected commit version"}')
                        return
                    }
                    acceptedCommits.put(commit.version as Long, commit)
                }
            }
            sendJson(200, catalogManagedResponse())
        } else {
            sendJson(404, '{"error_code":"NOT_FOUND"}')
        }
    }
    server.start()

    try {
        def catalogName = "test_native_delta_unity"
        sql "DROP CATALOG IF EXISTS ${catalogName}"
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'delta',
                'delta.catalog.type' = 'unity',
                'unity.uri' = 'http://127.0.0.1:${server.address.port}',
                'unity.auth.type' = 'pat',
                'unity.token' = '${token}',
                'unity.catalog' = 'main',
                'delta.write.enabled' = 'true',
                'test_connection' = 'false'
            )
        """

        order_qt_databases "SHOW DATABASES FROM ${catalogName}"
        order_qt_tables "SHOW TABLES FROM ${catalogName}.`default`"
        test {
            sql "SELECT COUNT(*) FROM ${catalogName}.`default`.blocked"
            exception "Table [blocked] does not exist in database"
        }
        order_qt_count "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"
        order_qt_filtered """
            SELECT c_custkey, c_name
            FROM ${catalogName}.`default`.customer
            WHERE c_custkey <= 3
            ORDER BY c_custkey
        """
        order_qt_managed_count "SELECT COUNT(*) FROM ${catalogName}.`default`.managed_customer"
        order_qt_catalog_managed_count """
            SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed
        """
        order_qt_catalog_managed_initial_rows """
            SELECT id FROM ${catalogName}.`default`.catalog_managed ORDER BY id
        """
        sql "INSERT INTO ${catalogName}.`default`.catalog_managed VALUES (300002)"
        order_qt_catalog_managed_after """
            SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed
        """
        order_qt_catalog_managed_inserted """
            SELECT id
            FROM ${catalogName}.`default`.catalog_managed
            WHERE id = 300002
        """
        order_qt_unity_external_before "SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write"
        sql """
            INSERT INTO ${catalogName}.`default`.unity_external_write VALUES
            (300001, 'Customer#000300001', 'Doris Unity external write', 1,
             '30-300-300-3000', 45.67, 'BUILDING', 'native Unity write')
        """
        order_qt_unity_external_after "SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write"
        order_qt_unity_external_inserted """
            SELECT c_custkey, c_name, c_acctbal
            FROM ${catalogName}.`default`.unity_external_write
            WHERE c_custkey = 300001
        """

        test {
            sql "TRUNCATE TABLE ${catalogName}.`default`.customer"
            exception "Unity Catalog does not advertise external-engine write support"
        }
        test {
            sql "INSERT INTO ${catalogName}.`default`.customer SELECT * FROM ${catalogName}.`default`.customer"
            exception "Unity Catalog does not advertise external-engine write support"
        }
        order_qt_read_only_after_rejected_write "SELECT COUNT(*) FROM ${catalogName}.`default`.customer"

        sql "TRUNCATE TABLE ${catalogName}.`default`.unity_external_write"
        order_qt_unity_external_after_truncate "SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write"
        order_qt_unity_external_history_after_truncate """
            SELECT c_custkey, c_name FROM ${catalogName}.`default`.unity_external_write
            FOR VERSION AS OF 1 WHERE c_custkey = 300001 ORDER BY c_custkey
        """
        sql "TRUNCATE TABLE ${catalogName}.`default`.unity_external_write"
        order_qt_unity_external_after_repeated_truncate """
            SELECT COUNT(*) FROM ${catalogName}.`default`.unity_external_write
        """
        sql """
            INSERT INTO ${catalogName}.`default`.unity_external_write VALUES
            (300003, 'Customer#000300003', 'Doris after truncate', 1,
             '30-300-300-3000', 78.90, 'BUILDING', 'after truncate')
        """
        order_qt_unity_external_append_after_truncate """
            SELECT c_custkey, c_name, c_acctbal FROM ${catalogName}.`default`.unity_external_write ORDER BY c_custkey
        """

        // A previous Kernel commit may enable inCommitTimestamp; the next write must still work.
        sql "INSERT INTO ${catalogName}.`default`.catalog_managed VALUES (300005)"
        order_qt_catalog_managed_consecutive_insert """
            SELECT id FROM ${catalogName}.`default`.catalog_managed WHERE id >= 300000 ORDER BY id
        """
        sql "TRUNCATE TABLE ${catalogName}.`default`.catalog_managed"
        order_qt_catalog_managed_after_truncate "SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed"
        order_qt_catalog_managed_history_after_truncate """
            SELECT id FROM ${catalogName}.`default`.catalog_managed
            FOR VERSION AS OF 3 ORDER BY id
        """
        sql "TRUNCATE TABLE ${catalogName}.`default`.catalog_managed"
        order_qt_catalog_managed_after_repeated_truncate """
            SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed
        """
        sql "INSERT INTO ${catalogName}.`default`.catalog_managed VALUES (300004)"
        order_qt_catalog_managed_append_after_truncate """
            SELECT id FROM ${catalogName}.`default`.catalog_managed ORDER BY id
        """
        rejectCatalogManagedCommit.set(true)
        test {
            sql "TRUNCATE TABLE ${catalogName}.`default`.catalog_managed"
            // Kernel wraps the rejected Unity HTTP commit in its retry-limit exception.
            exception "Commit attempt for version 7 failed"
        }
        order_qt_catalog_managed_after_rejected_commit """
            SELECT id FROM ${catalogName}.`default`.catalog_managed ORDER BY id
        """
        rejectCatalogManagedCommit.set(false)
        sql "TRUNCATE TABLE ${catalogName}.`default`.catalog_managed"
        order_qt_catalog_managed_truncate_after_rejection """
            SELECT COUNT(*) FROM ${catalogName}.`default`.catalog_managed
        """
    } finally {
        server.stop(0)
    }
}
