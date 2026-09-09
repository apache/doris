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

// PR3E regression scope: the 3D dispatcher is not delivered yet, so a cluster cannot
// produce a genuine UNKNOWN job and every case in this suite is negative or static.
// Deliberately NOT covered here (all wait for 3D / slice 4): FORCE_RELEASE happy
// path e2e, same-name re-admission after FORCE e2e, quota reclaim after FORCE e2e,
// and expiry GC of resolved jobs e2e.

suite("test_lance_index_resolve", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index resolve test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // Read for fixture parity with the admission suite; this suite creates no REST
    // catalog because a REST catalog can never hold an UNKNOWN job, and the REST
    // rejection family is already covered by test_lance_index_admission.
    String lanceRestPort = context.config.otherConfigs.get("lance_rest_port")
    // Admitted jobs are durable and stay PENDING forever in this delivery slice:
    // dispatch, the FORCE happy path and job GC only land in later slices, so their
    // fences and quota charges can never be released here. Every index name (and the
    // filesystem catalog itself, because fence/quota keys include the persisted
    // catalog id) carries this per-run suffix so that rerunning the suite on a
    // shared pipeline cluster can never collide with a previous run's leftovers.
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_resolve_${runSuffix}"
    String user = "test_lance_index_resolve_user"
    String password = "C123_567p"
    String tableName = "vs_ivf_pq_f32"
    String resolveIndexName = "idx_resolve_${runSuffix}"
    // Job ids come from the cluster-wide Env.getNextId() allocator, so this literal
    // can never name a real job.
    String absentJobId = "9223372036854775806"

    // The filesystem catalog is fresh per run by construction, and once it holds
    // unresolved jobs DROP CATALOG is guarded, so there is deliberately no DROP for
    // it here.
    try_sql "DROP USER '${user}'@'%'"

    // All three settings are masterOnly. Read them on the master even if the suite's
    // ordinary JDBC connection points at a follower. SHOW uses the experimental
    // display name for the gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def keepRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_keep_max_second'"""
    def cleanRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_clean_interval_second'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, keepRows.size())
    assertEquals(1, cleanRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalKeep = keepRows[0][1].toString()
    String originalClean = cleanRows[0][1].toString()
    // The retention configs ship with these documented defaults; asserting them here
    // fails loudly if a shared cluster has drifted instead of silently restoring a
    // non-default value afterwards.
    assertEquals("604800", originalKeep)
    assertEquals("3600", originalClean)
    Throwable suiteFailure = null

    // test { ... exception } always runs on the suite's default connection; the
    // masterOnly config rejections below must be asserted on the master connection.
    def expectMasterSqlException = { String stmt, String substring ->
        String caught = null
        try {
            master_sql(stmt)
        } catch (Throwable t) {
            caught = t.toString()
        }
        assertTrue(caught != null && caught.contains(substring))
    }

    try {
        // Rejections reachable with the mutation gate closed. RESOLVE is
        // deliberately NOT behind enable_lance_index_mutation: it is the operator
        // escape hatch and must stay usable while admission is gated, otherwise an
        // unresolved job would freeze catalog DDL forever. Pin the gate closed so
        // the cases in this block also prove the statement is not answered with the
        // gate rejection.
        master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "false")"""

        // Wrong AS literal: only FORCE_RELEASE is accepted after AS.
        test {
            sql """RESOLVE LANCE INDEX JOB ${absentJobId} AS FORCE COMMENT 'wrong literal'"""
            exception "mismatched input"
        }

        // The AS FORCE_RELEASE clause is mandatory.
        test {
            sql """RESOLVE LANCE INDEX JOB ${absentJobId} COMMENT 'missing as clause'"""
            exception "mismatched input"
        }

        // COMMENT is mandatory at grammar level.
        test {
            sql """RESOLVE LANCE INDEX JOB ${absentJobId} AS FORCE_RELEASE"""
            exception "mismatched input"
        }

        // An empty COMMENT parses but never changes the observable response: the
        // note check runs after the job lookup and the UNKNOWN state gate, so with a
        // missing job the fixed not-found wording answers first. The dedicated
        // empty-note rejection needs a genuine UNKNOWN job and stays UT-only
        // (ResolveLanceIndexJobCommandTest) until 3D can produce one.
        test {
            sql """RESOLVE LANCE INDEX JOB ${absentJobId} AS FORCE_RELEASE COMMENT ''"""
            exception "Lance index job not found"
        }

        // A well-formed RESOLVE of a missing job: the fixed non-disclosing 5103
        // wording, with no gate rejection even though the gate is closed.
        test {
            sql """RESOLVE LANCE INDEX JOB ${absentJobId} AS FORCE_RELEASE COMMENT 'no such job'"""
            exception "Lance index job not found"
        }

        // Rejections reachable with an admitted PENDING job. masterOnly configs set
        // through ADMIN SET land on the master node locally, which is where
        // admission reads them; the finally block below restores the gate no matter
        // where the suite fails.
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

        // CREATE INDEX is admitted and returns a single-column JobId result set with
        // one row; the job stays PENDING because no dispatcher exists in this slice.
        def createRows = sql """CREATE INDEX `${resolveIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
        assertEquals(1, createRows.size())
        assertEquals(1, createRows[0].size())
        String createJobId = createRows[0][0].toString()

        // RESOLVE authorization is checked against the job's target (table ALTER, or
        // global ADMIN for an orphan), and an unauthorized caller gets exactly the
        // same non-disclosing response as a missing job. A fresh user holding only
        // an unrelated SELECT privilege must therefore see "not found".
        sql """CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'"""
        sql """GRANT SELECT_PRIV ON regression_test TO '${user}'@'%'"""
        if (isCloudMode()) {
            def clusters = sql "SHOW CLUSTERS"
            assertTrue(!clusters.isEmpty())
            sql """GRANT USAGE_PRIV ON CLUSTER `${clusters[0][0]}` TO '${user}'@'%'"""
        }

        connect(user, password, context.config.jdbcUrl) {
            test {
                sql """RESOLVE LANCE INDEX JOB ${createJobId} AS FORCE_RELEASE COMMENT 'unauthorized caller'"""
                exception "Lance index job not found"
            }
        }

        // FORCE_RELEASE only accepts UNKNOWN jobs: the admitted job is PENDING, the
        // one durable state this slice can produce, so an authorized RESOLVE is
        // rejected with the not-in-UNKNOWN wording.
        test {
            sql """RESOLVE LANCE INDEX JOB ${createJobId} AS FORCE_RELEASE COMMENT 'still pending'"""
            exception "cannot be resolved: not in UNKNOWN state"
        }

        // Retention config smoke: both are masterOnly and guarded by the
        // positive-long callback, whose rejection message carries the field name and
        // the offending value.
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_keep_max_second" = "0")""",
                "must be a positive long")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_keep_max_second" = "-1")""",
                "must be a positive long")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_clean_interval_second" = "0")""",
                "must be a positive long")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_clean_interval_second" = "-1")""",
                "must be a positive long")

        // A positive value is accepted and visible immediately on the master.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_clean_interval_second" = "3601")"""
        def cleanRowsAfterSet = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_clean_interval_second'"""
        assertEquals("3601", cleanRowsAfterSet[0][1].toString())
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_clean_interval_second" = "${originalClean}")"""

        // Retention GC never touches an unresolved job: with the keep window pinned
        // to one second, any resolved record older than a second is eligible for
        // deletion at the next clean round, while the admitted PENDING job must
        // survive regardless of its age because the GC predicate fails closed on
        // unresolved records. Waiting out a clean interval is impractical here, so
        // this is a static existence assertion; expiry GC e2e waits for 3D / slice 4
        // (see the header comment).
        try {
            master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_keep_max_second" = "1")"""
            def keepRowsAfterSet = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_keep_max_second'"""
            assertEquals("1", keepRowsAfterSet[0][1].toString())
            def jobsRows = sql_return_maparray """SHOW LANCE INDEX JOBS FROM `${filesystemCatalog}`.`doris`
                    WHERE TableName = "${tableName}" """
            def pendingJobRow = jobsRows.find { it.IndexName == resolveIndexName }
            assertTrue(pendingJobRow != null)
            assertEquals(createJobId, pendingJobRow.JobId.toString())
            assertEquals("PENDING", pendingJobRow.State.toString())
        } finally {
            // Restore immediately so the tiny keep window cannot outlive this case;
            // the outer finally restores the original value again defensively.
            master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_keep_max_second" = "${originalKeep}")"""
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
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_keep_max_second" = "${originalKeep}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_clean_interval_second" = "${originalClean}")""" },
            { sql "DROP USER IF EXISTS '${user}'@'%'" }
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
        // The filesystem catalog stays behind: the admitted job remains unresolved
        // and guards DROP CATALOG until FORCE_RELEASE lands in a later slice.
    }
}
