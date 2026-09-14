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

// PR3D regression scope: no backend worker exists in this delivery slice to consume
// a dispatched Lance index job request (the isolated worker lands in a later slice),
// so a cluster cannot exercise positive dispatch, the result callback, or the
// terminal-job refresh driver end to end, and every case in this suite is negative
// or static: the six new dispatcher configs are smoked through SHOW/SET (validator
// rejections plus a boolean two-state round trip), and, with the dispatcher's
// polling interval pinned to one hour, an admitted job's user-visible row is proven
// frozen for the suite's window (the daemon exists but is disabled by configuration
// from dispatching). The mutation gate is opened only to admit that one job; both
// the gate and the interval are restored in the finally block. Deliberately NOT
// covered here (all wait for the worker slice / fake-worker UT): dispatch and
// callback e2e, genuine UNKNOWN creation, and the G2/G4 evidence.

suite("test_lance_index_dispatch", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index dispatch test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // Admitted jobs are durable and stay PENDING in this delivery slice: no worker
    // exists to execute a dispatched job and the dispatcher is pinned silent below,
    // while FORCE_RELEASE and job GC only land in later slices, so their fences and
    // quota charges can never be released here.
    // Every index name (and the filesystem catalog itself, because fence/quota keys
    // include the persisted catalog id) carries this per-run suffix so that rerunning
    // the suite on a shared pipeline cluster can never collide with a previous run's
    // leftovers.
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_dispatch_${runSuffix}"
    String tableName = "vs_ivf_pq_f32"
    String dispatchIndexName = "idx_dispatch_${runSuffix}"

    // All seven settings are masterOnly. Read them on the master even if the suite's
    // ordinary JDBC connection points at a follower. SHOW uses the experimental
    // display name for the gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def intervalRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_dispatch_interval_second'"""
    def deadlineRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_execute_deadline_second'"""
    def roundRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_max_dispatch_per_round'"""
    def inflightRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_max_inflight_per_backend'"""
    def retryRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_refresh_retry_second'"""
    def localFileRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_lance_index_local_file_mutation'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, intervalRows.size())
    assertEquals(1, deadlineRows.size())
    assertEquals(1, roundRows.size())
    assertEquals(1, inflightRows.size())
    assertEquals(1, retryRows.size())
    assertEquals(1, localFileRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalInterval = intervalRows[0][1].toString()
    String originalDeadline = deadlineRows[0][1].toString()
    String originalRound = roundRows[0][1].toString()
    String originalInflight = inflightRows[0][1].toString()
    String originalRetry = retryRows[0][1].toString()
    String originalLocalFile = localFileRows[0][1].toString()
    // The six dispatcher configs ship with these documented defaults; asserting them
    // here fails loudly if a shared cluster has drifted instead of silently
    // restoring a non-default value afterwards. The derived wait below also relies
    // on the interval really being the shipped 10 seconds when the pin is applied.
    assertEquals("10", originalInterval)
    assertEquals("3600", originalDeadline)
    assertEquals("16", originalRound)
    assertEquals("2", originalInflight)
    assertEquals("300", originalRetry)
    assertEquals("false", originalLocalFile)
    long dispatchIntervalSecond = originalInterval.toLong()
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
        // Dispatcher config smoke: the five numeric items are guarded by the
        // positive-int/positive-long callback, whose rejection message carries the
        // field name and the offending value. Zero and negative values are rejected
        // before assignment, so none of these sets can take effect.
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "0")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "-1")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_execute_deadline_second" = "0")""",
                "must be a positive long")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_execute_deadline_second" = "-1")""",
                "must be a positive long")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_max_dispatch_per_round" = "0")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_max_dispatch_per_round" = "-1")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "0")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "-1")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_refresh_retry_second" = "0")""",
                "must be a positive int")
        expectMasterSqlException("""ADMIN SET FRONTEND CONFIG ("lance_index_job_refresh_retry_second" = "-1")""",
                "must be a positive int")

        // A positive value is accepted and visible immediately on the master.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_dispatch_per_round" = "17")"""
        def roundRowsAfterSet = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_max_dispatch_per_round'"""
        assertEquals("17", roundRowsAfterSet[0][1].toString())
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_dispatch_per_round" = "${originalRound}")"""

        // The file:// mutation assertion is a plain boolean with no validator
        // callback: both of its states are settable and immediately visible.
        master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_local_file_mutation" = "true")"""
        def localFileRowsTrue = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_lance_index_local_file_mutation'"""
        assertEquals("true", localFileRowsTrue[0][1].toString())
        master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_local_file_mutation" = "false")"""
        def localFileRowsFalse = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_lance_index_local_file_mutation'"""
        assertEquals("false", localFileRowsFalse[0][1].toString())
        // The original value (asserted to be the shipped "false" above) is restored
        // again defensively by the finally block.

        // The dispatcher daemon polls every lance_index_job_dispatch_interval_second
        // regardless of the mutation gate (durable jobs must be driven even with
        // admission closed), and in this slice a round really reaches the backends:
        // their handler answers submit_lance_index_job with a clean not-implemented
        // error, which converges the job to NOT_COMMITTED
        // (PRE_INVOCATION_RESOURCE_REJECTED). An unpinned round would therefore
        // legitimately advance the admitted job, so the state-stability case below
        // pins the interval to one hour first; a cycle already sleeping on the
        // shipped interval can still wake once more within that interval, and
        // outliving it here guarantees zero rounds for the rest of the suite.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "3600")"""
        sleep((dispatchIntervalSecond + 1) * 1000L)

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
        // one row; the job is visible as PENDING right after admission.
        def createRows = sql """CREATE INDEX `${dispatchIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="256", "num_sub_vectors"="16")"""
        assertEquals(1, createRows.size())
        assertEquals(1, createRows[0].size())
        String createJobId = createRows[0][0].toString()

        def jobsBeforeWait = sql_return_maparray """SHOW LANCE INDEX JOBS FROM `${filesystemCatalog}`.`doris`
                WHERE TableName = "${tableName}" """
        def jobRowBeforeWait = jobsBeforeWait.find { it.IndexName == dispatchIndexName }
        assertTrue(jobRowBeforeWait != null)
        assertEquals(createJobId, jobRowBeforeWait.JobId.toString())
        assertEquals("PENDING", jobRowBeforeWait.State.toString())
        // A never-dispatched job holds no possible-live worker slot and has no
        // refresh obligation yet; both must stay that way across the wait below.
        assertEquals("NO", jobRowBeforeWait.PossibleLive.toString())

        // A short static wait inside the pinned window: with the daemon disabled by
        // configuration no round can dispatch the job, so the visible row must not
        // move.
        sleep(5000)

        def jobsAfterWait = sql_return_maparray """SHOW LANCE INDEX JOBS FROM `${filesystemCatalog}`.`doris`
                WHERE TableName = "${tableName}" """
        def jobRowAfterWait = jobsAfterWait.find { it.IndexName == dispatchIndexName }
        assertTrue(jobRowAfterWait != null)
        assertEquals("PENDING", jobRowAfterWait.State.toString())
        assertEquals("NO", jobRowAfterWait.PossibleLive.toString())
        // No lifecycle column moved between the two reads: with the dispatcher
        // pinned silent no round can dispatch the job, and without a worker nothing
        // else can advance it.
        ["JobId", "CatalogName", "DbName", "TableName", "IndexName", "Operation",
         "State", "RefreshState", "PossibleLive"].each { column ->
            assertEquals(jobRowBeforeWait[column].toString(), jobRowAfterWait[column].toString())
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
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "${originalInterval}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_execute_deadline_second" = "${originalDeadline}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_dispatch_per_round" = "${originalRound}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "${originalInflight}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_refresh_retry_second" = "${originalRetry}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_local_file_mutation" = "${originalLocalFile}")""" }
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
