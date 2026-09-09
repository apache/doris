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
    // Admitted jobs are durable and stay PENDING forever in this delivery slice: dispatch,
    // FORCE_RELEASE and job GC only land in later slices, so their fences and quota charges
    // can never be released here. Every index name (and the filesystem catalog itself,
    // because fence/quota keys include the persisted catalog id) carries this per-run suffix
    // so that rerunning the suite on a shared pipeline cluster can never collide with a
    // previous run's leftovers.
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_admission_${runSuffix}"
    String restCatalog = "test_lance_index_admission_rest"
    String user = "test_lance_index_admission_user"
    String password = "C123_567p"
    String tableName = "vs_ivf_pq_f32"
    String quotaTableName = "predicate_pushdown"
    // vs_ivf_pq_f32 ships with one preloaded IVF_PQ index on the embedding column; its
    // authoritative logical metadata (metric_type=L2, compression.num_sub_vectors=4) is the
    // anchor for the CREATE IF NOT EXISTS and admitted DROP cases below.
    String preloadedIndex = "embedding_ivf_pq_f32"
    String createIndexName = "idx_create_${runSuffix}"
    String quotaIndexNameA = "idx_quota_a_${runSuffix}"
    String quotaIndexNameB = "idx_quota_b_${runSuffix}"
    String absentIndexName = "idx_absent_${runSuffix}"

    // The filesystem catalog is fresh per run by construction, and once it holds unresolved
    // jobs DROP CATALOG is guarded, so there is deliberately no DROP for it here.
    sql """DROP CATALOG IF EXISTS `${restCatalog}`"""
    try_sql "DROP USER '${user}'@'%'"

    // Both settings are masterOnly. Read them on the master even if the suite's
    // ordinary JDBC connection points at a follower. SHOW uses the experimental
    // display name for the gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def quotaRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_max_unresolved_per_table'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, quotaRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalQuota = quotaRows[0][1].toString()
    // The main scenario admits two jobs on one table, independently of the
    // cluster's original quota. The dedicated quota case temporarily lowers it.
    String suiteQuota = Math.max(2L, originalQuota.toLong()).toString()
    Throwable suiteFailure = null

    try {
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_unresolved_per_table" = "${suiteQuota}")"""
        // Open the mutation gate for this suite only. masterOnly configs set through
        // ADMIN SET land on the master node locally, which is where admission reads them;
        // the finally block below restores the gate no matter where the suite fails (T4).
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

        // CREATE INDEX is admitted and returns a single-column JobId result set with one row.
        def createRows = sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
        assertEquals(1, createRows.size())
        assertEquals(1, createRows[0].size())
        String createJobId = createRows[0][0].toString()

        // SHOW LANCE INDEX JOBS exposes JobId, CatalogName, DbName, TableName, IndexName,
        // Operation, State and further inspection columns (design section 2.3); the job is
        // visible as PENDING right after admission.
        def jobsAfterCreate = sql_return_maparray """SHOW LANCE INDEX JOBS FROM `${filesystemCatalog}`.`doris`
                WHERE TableName = "${tableName}" """
        def createJobRow = jobsAfterCreate.find { it.IndexName == createIndexName }
        assertTrue(createJobRow != null)
        assertEquals(createJobId, createJobRow.JobId.toString())
        assertEquals("PENDING", createJobRow.State.toString())

        // A same-name CREATE passes the authoritative preflight (the admitted job has not
        // built anything yet) and is then stopped by the durable same-name fence.
        test {
            sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
            exception "is fenced by unresolved job"
        }

        // CREATE IF NOT EXISTS against the preloaded index with a matching definition is an
        // immediate no-op: no job is created and the same JobId-shaped result set comes back
        // with zero rows. num_partitions is never compared by the authoritative preflight,
        // and a property left out of the request (metric here) is not compared either.
        def noopRows = sql """CREATE INDEX IF NOT EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "num_partitions"="256", "num_sub_vectors"="4")"""
        assertTrue(noopRows.isEmpty())

        // The same name with a different metric is an authoritative mismatch, not a no-op.
        test {
            sql """CREATE INDEX IF NOT EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="cosine", "num_partitions"="256", "num_sub_vectors"="4")"""
            exception "already exists with a different definition"
        }

        // DROP IF EXISTS of an authoritatively absent name is an immediate zero-row no-op...
        def dropNoopRows = sql """DROP INDEX IF EXISTS `${absentIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
        assertTrue(dropNoopRows.isEmpty())

        // ...while plain DROP of an absent name fails.
        test {
            sql """DROP INDEX `${absentIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
            exception "not found"
        }

        // Per-table unresolved-job quota on a dedicated table (row_id is the only NOT NULL
        // scalar column of predicate_pushdown): with the limit at one, the first
        // differently-named job is admitted and the second is rejected. The quota key
        // includes the persisted catalog id, so the per-run catalog keeps this rerun-safe.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_unresolved_per_table" = "1")"""
        try {
            def admittedQuotaRows = sql """CREATE INDEX `${quotaIndexNameA}` ON `${filesystemCatalog}`.`doris`.`${quotaTableName}` (row_id) USING BTREE"""
            assertEquals(1, admittedQuotaRows.size())
            test {
                sql """CREATE INDEX `${quotaIndexNameB}` ON `${filesystemCatalog}`.`doris`.`${quotaTableName}` (row_id) USING BTREE"""
                exception "quota exceeded"
            }
        } finally {
            // Restore immediately so the remaining cases of this run are unaffected; the
            // outer finally restores the original cluster settings even if this fails.
            master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_unresolved_per_table" = "${suiteQuota}")"""
        }

        // Job inspection is authorized row by row: a user without SHOW privilege on the
        // target sees no jobs at all, and a direct lookup of an existing job id gets the
        // same fixed non-disclosing "not found" response as a missing job.
        sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'"""
        sql """GRANT SELECT_PRIV ON regression_test TO '${user}'@'%'"""
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${user}'@'%'"""
        }

        connect(user, password, context.config.jdbcUrl) {
            def invisibleJobs = sql """SHOW LANCE INDEX JOBS"""
            assertTrue(invisibleJobs.isEmpty())
            test {
                sql """SHOW LANCE INDEX JOB ${createJobId}"""
                exception "Lance index job not found"
            }
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
        // the authoritative snapshot, so it is admitted as a second durable job on the table.
        def dropRows = sql """DROP INDEX IF EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
        assertEquals(1, dropRows.size())
        assertEquals(1, dropRows[0].size())
        String dropJobId = dropRows[0][0].toString()
        assertTrue(dropJobId != createJobId)

        def jobsAfterDrop = sql_return_maparray """SHOW LANCE INDEX JOBS FROM `${filesystemCatalog}`.`doris`
                WHERE TableName = "${tableName}" """
        def createJobRowAfterDrop = jobsAfterDrop.find { it.IndexName == createIndexName }
        def dropJobRow = jobsAfterDrop.find { it.IndexName == preloadedIndex }
        assertTrue(createJobRowAfterDrop != null)
        assertTrue(dropJobRow != null)
        assertEquals(createJobId, createJobRowAfterDrop.JobId.toString())
        assertEquals(dropJobId, dropJobRow.JobId.toString())
        assertEquals("PENDING", createJobRowAfterDrop.State.toString())
        assertEquals("PENDING", dropJobRow.State.toString())
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Attempt every cleanup, but never report success after a failed restore.
        // Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_unresolved_per_table" = "${originalQuota}")""" },
            { sql "DROP USER IF EXISTS '${user}'@'%'" },
            { sql """DROP CATALOG IF EXISTS `${restCatalog}`""" }
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
        // The filesystem catalog stays behind: admitted jobs remain unresolved and guard
        // DROP CATALOG until FORCE_RELEASE lands in a later slice.
    }
}
