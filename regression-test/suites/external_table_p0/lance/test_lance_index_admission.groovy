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

suite("test_lance_index_admission", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index admission test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    // The authoritative admission preflight runs against real Lance metadata but the
    // synchronous execution path has not landed: a mutation that would be admitted ends
    // in the shared not-supported rejection, and nothing is created or dropped. Index
    // names still carry this per-run suffix so rerunning the suite on a shared pipeline
    // cluster can never collide with leftovers from other suites.
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_admission_${runSuffix}"
    String restCatalog = "test_lance_index_admission_rest"
    String tableName = "vs_ivf_pq_f32"
    // vs_ivf_pq_f32 ships with one preloaded IVF_PQ index on the embedding column; its
    // authoritative logical metadata (metric_type=L2, compression.num_sub_vectors=4,
    // compression.num_bits=4) is the anchor for the CREATE IF NOT EXISTS mismatch cases
    // below.
    String preloadedIndex = "embedding_ivf_pq_f32"
    String createIndexName = "idx_create_${runSuffix}"
    String absentIndexName = "idx_absent_${runSuffix}"

    sql """DROP CATALOG IF EXISTS `${restCatalog}`"""

    // The gate is masterOnly. Read it on the master even if the suite's ordinary JDBC
    // connection points at a follower. SHOW uses the experimental display name for the
    // gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    assertEquals(1, gateRows.size())
    String originalGate = gateRows[0][1].toString()
    Throwable suiteFailure = null

    try {
        // Open the mutation gate for this suite only. masterOnly configs set through
        // ADMIN SET land on the master node locally, which is where admission reads them;
        // the finally block below restores the gate no matter where the suite fails.
        master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "true")"""

        sql """
            CREATE CATALOG `${filesystemCatalog}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "filesystem",
                "warehouse" = "s3://warehouse/lance",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.region" = "us-east-1",
                "use_path_style" = "true"
            )
        """

        // CREATE INDEX passes static validation and the whole authoritative preflight
        // (snapshot read, case analysis, column resolution, schema contract), then ends
        // in the shared not-supported rejection: no index is created.
        test {
            sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "CREATE INDEX is not supported for Lance catalog tables"
        }

        // Nothing was created above, so a same-name CREATE runs the same preflight and
        // ends in the same rejection.
        test {
            sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "CREATE INDEX is not supported for Lance catalog tables"
        }

        // The preloaded index persists num_bits=4 (lance_build_preinstalled_catalog.py
        // PQ_BUILD_PARAMS), which is outside the SQL-expressible parameter space: static
        // validation pins num_bits to 8, and the authoritative preflight compares an omitted
        // num_bits as the always-persisted 8. No admitted request can therefore no-op against
        // this index; the matching-definition no-op path is covered by LanceIndexAdmissionTest
        // against mocked snapshots. What this fixture pins live is the fail-closed mismatch
        // at each layer.
        test {
            sql """CREATE INDEX IF NOT EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "num_partitions"="256", "num_sub_vectors"="4", "num_bits"="4")"""
            exception "num_bits must be 8"
        }

        // An omitted num_bits passes static validation, but the preflight compares the
        // always-persisted 8 against the on-disk 4: an authoritative mismatch, not a no-op.
        test {
            sql """CREATE INDEX IF NOT EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "num_partitions"="256", "num_sub_vectors"="4")"""
            exception "already exists with a different definition"
        }

        // The same name with a different metric is an authoritative mismatch as well.
        test {
            sql """CREATE INDEX IF NOT EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="cosine", "num_partitions"="256", "num_sub_vectors"="4")"""
            exception "already exists with a different definition"
        }

        // DROP IF EXISTS of an authoritatively absent name completes as a no-op. Without the
        // JobId result set there is nothing to return, so the statement finishes with the
        // default OK packet, which the framework surfaces as a single zero update-count row.
        def dropNoopRows = sql """DROP INDEX IF EXISTS `${absentIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
        assertEquals(1, dropNoopRows.size())
        assertEquals(0, dropNoopRows[0][0])

        // ...while plain DROP of an absent name fails.
        test {
            sql """DROP INDEX `${absentIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
            exception "not found"
        }

        // REST catalogs keep failing fast before admission even with the gate open.
        sql """
            CREATE CATALOG `${restCatalog}` PROPERTIES (
                "type" = "lance",
                "lance.catalog.type" = "rest",
                "lance.rest.uri" = "http://${externalEnvIp}:${lanceRestPort}",
                "lance.rest.security.type" = "bearer",
                "lance.rest.bearer-token" = "doris-lance-rest-test-token",
                "lance.namespace.root_database" = "default",
                "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                "s3.region" = "us-east-1",
                "use_path_style" = "true",
                "test_connection" = "true"
            )
        """

        test {
            sql """CREATE INDEX idx ON `${restCatalog}`.`default`.`all_types` (row_id) USING BTREE"""
            exception "CREATE INDEX is not supported for Lance REST catalogs"
        }

        test {
            sql """CREATE OR REPLACE INDEX idx ON `${restCatalog}`.`default`.`all_types` (row_id) USING BTREE"""
            exception "CREATE OR REPLACE INDEX is not supported for Lance REST catalogs"
        }

        test {
            sql """DROP INDEX idx ON `${restCatalog}`.`default`.`all_types`"""
            exception "DROP INDEX is not supported for Lance REST catalogs"
        }

        // DROP INDEX IF EXISTS of the preloaded index resolves to a name that is present in
        // the authoritative snapshot, so the preflight passes and the statement ends in the
        // shared not-supported rejection: nothing is dropped.
        test {
            sql """DROP INDEX IF EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
            exception "DROP INDEX is not supported for Lance catalog tables"
        }
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Attempt every cleanup, but never report success after a failed restore.
        // Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            { sql """DROP CATALOG IF EXISTS `${restCatalog}`""" },
            { sql """DROP CATALOG IF EXISTS `${filesystemCatalog}`""" }
        ].each { cleanup ->
            try {
                cleanup()
            } catch (Throwable failure) {
                if (cleanupFailure == null) {
                    cleanupFailure = failure
                } else {
                    cleanupFailure.addSuppressed(failure)
                }
            }
        }
        if (suiteFailure == null && cleanupFailure != null) {
            throw cleanupFailure
        }
    }
}
