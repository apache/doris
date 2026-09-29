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

// PR4A worker-slice regression negatives. The BE submit_lance_index_job handler now sits in
// front of the cgroup-isolated Lance index worker, so a dispatch can fail before launch — the
// startup isolation preflight failed on a runner without cgroup v2 delegation, the bounded
// submission queue is full, the payload violates the dispatch bounds — or inside the worker,
// e.g. object-store credentials that no longer authenticate. Every one of those paths must
// converge the durable job terminally (COMMITTED, NOT_COMMITTED or UNKNOWN), never leave it
// stuck in PENDING or RUNNING, release the possible-live worker slot for the states whose
// termination proof rides with the result (PossibleLive=NO), and keep all credential material
// out of every user-visible column and message.
//
// The CI runner may or may not delegate a writable cgroup v2 subtree to the BE (amendment
// G11). On a delegation-less runner every dispatch is rejected synchronously with a clean
// error and the job converges to NOT_COMMITTED with the internal NEVER_LAUNCHED proof; on a
// delegated runner with a working object-store path the same job can genuinely train and
// COMMIT. The suite therefore derives the runner profile from the first job's terminal
// result, logs it, and asserts only the invariants that hold on both profiles.
//
// PossibleLive gating note (design-tolerated path, not a product bug): a COMMITTED job's
// CHILD_REAPED proof is BE-side evidence, and on a runner where the supervisor can never form
// it (kernel without pidfd/waitid(P_PIDFD), or a process-wide SIGCHLD reaper winning every
// race without pidfd/cgroup corroboration) the slot is deliberately RETAINED until the BE
// epoch changes — D5/D16 require exactly that. Such a runner therefore shows COMMITTED with
// PossibleLive=YES despite fully correct behavior. Case 1 derives commitProofStrict from its
// own outcome (COMMITTED + PossibleLive=NO visible => strict), and isSettled/assertSlotReleased
// apply the strict PossibleLive=NO assertion to COMMITTED rows only under that profile; other
// profiles settle COMMITTED by state with a loud log line. NOT_COMMITTED is never gated: its
// proofs are FE-internal or envelope-carried, so the slot release always lands with the result.
//
// Timing note: this is the first Lance suite whose cases need dispatch rounds to fire. The
// dispatcher daemon sleeps in bounded slices (never longer than MAX_SLEEP_SLICE_MS) and
// re-reads lance_index_job_dispatch_interval_second at every wake, so the one-second
// interval this suite pins below takes effect within one slice even when the
// admission/dispatch suites left the interval pinned at one hour: no wake mechanism is
// involved, and a wake inside a long configured interval only skips rounds until that
// period elapses, while a shortened interval runs a round at the very next wake. Case 1
// therefore keeps a daemon-responsiveness barrier: a fast polling phase for the common
// case, then a long-strided phase that outlives one sleep slice with ample margin (bounded
// at 3700s) instead of failing a green-able run. Once the barrier passes the daemon cycles
// at the pinned one second for the rest of the suite.
//
// Covered here: (1) preflight-rejection smoke with terminal convergence; (2) file://
// topology refusal with enable_lance_index_local_file_mutation explicitly enabled;
// (3) queue pressure: more rapid-fire jobs than the per-backend inflight cap, all converging
// terminally; (4) credential non-disclosure: a job dispatched only after the catalog secret
// was rotated to a wrong value, plus the admission-time rejection of a fresh CREATE INDEX
// against the rotated catalog, with a sweep of every visible column and message for secrets
// and storage-option key names. Deliberately NOT covered (BE UT territory, commits 5-6):
// invocation-id dedup, oversize payload frames, kill/timeout escalation and malformed
// protocol frames — none of them is expressible through SQL.

suite("test_lance_index_worker_negative", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index worker negative test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // Every index name and every catalog carries this per-run suffix so that rerunning the
    // suite on a shared pipeline cluster can never collide with a previous run's leftovers
    // (fence/quota keys include the persisted catalog id).
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_worker_negative_${runSuffix}"
    String fileCatalog = "test_lance_index_neg_file_${runSuffix}"
    String credCatalog = "test_lance_index_neg_cred_${runSuffix}"
    String tableName = "vs_ivf_pq_f32"
    String preflightIndexName = "idx_neg_preflight_${runSuffix}"
    String fileIndexName = "idx_neg_file_${runSuffix}"
    String credIndexName = "idx_neg_cred_${runSuffix}"
    String credRetryIndexName = "idx_neg_cred_b_${runSuffix}"
    String decoyIndexName = "idx_neg_decoy_${runSuffix}"
    String wrongSecret = "wrongSecret${runSuffix}"
    List<String> terminalStates = ["COMMITTED", "NOT_COMMITTED", "UNKNOWN"]
    // Neither the wrong secret nor the real MinIO credentials nor any storage-option key
    // spelling may ever surface in a job's user-visible columns. The BE builds its messages
    // from static category strings plus numeric identity fields and drops a message outright
    // if a storage-option value ever appears in it; the FE reasons are static strings too.
    List<String> forbiddenInJobColumns = [
            wrongSecret, "password", "admin",
            "aws_access_key_id", "aws_secret_access_key", "aws_session_token",
            "aws_endpoint", "aws_region",
            "s3.access_key", "s3.secret_key", "s3.endpoint", "s3.region"]
    // The admission-time rejection text sweeps the same secret values and key names but not
    // the bare access key id: an S3 signature-mismatch body may legitimately echo the access
    // key id inside a Credential= scope (it is not a secret), and that provider text predates
    // the storage-option vocabulary the FE sweeps.
    List<String> forbiddenInAdmissionErrors = [
            wrongSecret, "password",
            "aws_access_key_id", "aws_secret_access_key", "aws_session_token",
            "s3.access_key", "s3.secret_key"]
    // Every job this suite admits on the shared fixture (any catalog — they all point at the
    // same warehouse) is registered here as a [jobId, indexName] pair. The finally block
    // waits each of them settled and drops the ones that COMMITTED, so the shared
    // preinstalled fixture keeps the index set that the entries and show_index suites pin.
    // Each DROP is its own dispatched job; on a delegation-less runner nothing ever commits,
    // so the drops are all zero-row no-ops there.
    List cleanupCandidates = []

    // All four settings are masterOnly. Read them on the master even if the suite's ordinary
    // JDBC connection points at a follower. SHOW uses the experimental display name for the
    // gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def intervalRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_dispatch_interval_second'"""
    def inflightRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_max_inflight_per_backend'"""
    def localFileRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'enable_lance_index_local_file_mutation'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, intervalRows.size())
    assertEquals(1, inflightRows.size())
    assertEquals(1, localFileRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalInterval = intervalRows[0][1].toString()
    String originalInflight = inflightRows[0][1].toString()
    String originalLocalFile = localFileRows[0][1].toString()
    // The shipped defaults are asserted (not just captured) so a drifted shared cluster fails
    // loudly instead of being silently restored to a non-default value afterwards; the daemon
    // barrier arithmetic in case 1 also relies on the interval really being the shipped 10
    // seconds when this suite starts.
    assertEquals("10", originalInterval)
    assertEquals("2", originalInflight)
    assertEquals("false", originalLocalFile)
    Throwable suiteFailure = null

    // One SHOW LANCE INDEX JOB lookup by id: the singular form authorizes by job, needs no
    // FROM resolution against a possibly broken catalog, and exposes the inspection columns
    // (ResultCode, CompletionReason, TerminationProof, ...) the plural listing lacks.
    def fetchJobRow = { String jobId ->
        def rows = sql_return_maparray """SHOW LANCE INDEX JOB ${jobId}"""
        return rows.isEmpty() ? null : rows[0]
    }

    // An observation is settled once the state is terminal and — for the two states whose
    // termination proof rides with the result (one durable transition for the clean
    // pre-invocation rejection, one report-handler call for a worker-reported result) — the
    // possible-live slot release has landed too. UNKNOWN may legitimately keep its slot until
    // a later proof or the epoch sweep, so it settles on the state alone. COMMITTED is
    // runner-profile gated (see the header): only a commitProofStrict runner (case 1 proved
    // the reap-proof evidence is visible there) requires PossibleLive=NO; on other profiles a
    // COMMITTED-with-slot row settles by state with a loud log line.
    boolean commitProofStrict = false
    Set lenientCommittedLogged = new HashSet()
    def isSettled = { row ->
        if (row == null) {
            return false
        }
        String state = row.State.toString()
        if (!terminalStates.contains(state)) {
            return false
        }
        if (state == "UNKNOWN" || row.PossibleLive.toString() == "NO") {
            return true
        }
        if (state == "COMMITTED" && !commitProofStrict) {
            String jobKey = row.JobId == null ? row.toString() : row.JobId.toString()
            if (lenientCommittedLogged.add(jobKey)) {
                logger.info("job ${jobKey} is COMMITTED with PossibleLive!=NO: the reap-proof " +
                        "evidence is unavailable on this runner profile, so the slot release rides " +
                        "the BE epoch sweep by design; settling by state alone")
            }
            return true
        }
        return false
    }

    // The PossibleLive=NO assertion honoring the runner-profile gate above: UNKNOWN is exempt,
    // NOT_COMMITTED is always strict, COMMITTED is strict only on a commitProofStrict runner.
    def assertSlotReleased = { String label, row ->
        String state = row.State.toString()
        if (state == "UNKNOWN") {
            return
        }
        String live = row.PossibleLive.toString()
        if (live == "NO") {
            return
        }
        if (state == "COMMITTED" && !commitProofStrict) {
            logger.info("${label}: COMMITTED with PossibleLive=${live} tolerated on this runner " +
                    "profile (reap-proof evidence unavailable by design): ${row}")
            return
        }
        assertEquals("NO", live, "${label}: PossibleLive must be NO for ${state}: ${row}")
    }

    // Bounded poll; returns the last observed row for the caller to assert on. The fast phase
    // covers a healthy dispatcher (at most one bounded sleep slice before the pinned
    // one-second rounds start, plus worker execution and the report). If the job never
    // leaves PENDING there, the interval SET has not landed yet or a straggler round is
    // still owed (see the header); the slow phase outlives that with a cheap stride instead
    // of failing a green-able run. A job observed RUNNING is dispatched already and
    // converges on worker terms within seconds on a healthy runner, so it keeps the fast
    // stride.
    def waitForSettledJob = { String jobId, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        long slowPhaseAfterMs = Math.min(deadlineMs, System.currentTimeMillis() + 240000L)
        boolean slowPhaseLogged = false
        int pollCount = 0
        def row = fetchJobRow(jobId)
        while (!isSettled(row) && System.currentTimeMillis() < deadlineMs) {
            boolean pendingStuck = row != null && row.State.toString() == "PENDING" &&
                    System.currentTimeMillis() >= slowPhaseAfterMs
            if (pendingStuck && !slowPhaseLogged) {
                slowPhaseLogged = true
                logger.info("job ${jobId} is still PENDING after the fast phase; the Lance index dispatcher " +
                        "daemon runs a round within one bounded sleep slice of the pinned one-second interval " +
                        "(the interval is re-read at every wake); polling with a long stride")
            }
            sleep(pendingStuck ? 15000 : 1000)
            pollCount++
            if (pendingStuck && pollCount % 4 == 0) {
                logger.info("job ${jobId} still PENDING while waiting for the dispatcher's next round; " +
                        "last row: ${row}")
            }
            row = fetchJobRow(jobId)
        }
        return row
    }

    // The full exception chain text, for the message sweeps.
    def collectMessages = { Throwable t ->
        StringBuilder sb = new StringBuilder()
        Throwable current = t
        while (current != null) {
            sb.append(current.toString()).append('\n')
            current = current.getCause()
        }
        return sb.toString()
    }

    // Sweep every visible column of a job row (a Map) or an error text (any other object) for
    // credential material. The row is flattened first so the assertion stays one closure
    // level deep, matching the proven expectMasterSqlException pattern of the dispatch suite.
    def sweepForCredentials = { String label, inspected, List<String> forbidden ->
        String blob
        if (inspected instanceof Map) {
            StringBuilder sb = new StringBuilder()
            for (entry in inspected.entrySet()) {
                if (entry.value != null) {
                    sb.append("column ").append(entry.key).append(": ").append(entry.value.toString()).append('\n')
                }
            }
            blob = sb.toString()
        } else {
            blob = inspected.toString()
        }
        String lower = blob.toLowerCase()
        for (fragment in forbidden) {
            assertTrue(!lower.contains(fragment.toLowerCase()),
                    "${label} must not disclose '${fragment}' but shows: ${blob}")
        }
    }

    try {
        // Fast dispatch rounds and the admission gate for this suite only; the finally block
        // restores every captured setting no matter where the suite fails. The inflight cap is
        // pinned to its shipped default of two so the queue-pressure case is explicit about
        // the capacity it pressures. If the dispatcher daemon is still inside a previously
        // pinned hour-long interval, this SET takes effect within one bounded sleep slice
        // (the interval is re-read at every wake), which is exactly what case 1's barrier
        // waits out.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "1")"""
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "2")"""
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

        // ---- Case 1: preflight-rejection smoke ------------------------------------------
        // A CREATE INDEX whose parameters can genuinely train on the 1024-row fixture
        // (num_partitions <= 4 per the Lance sample-rate default of 256 training points per
        // partition, num_sub_vectors divides the 16-dimensional embedding, num_bits is pinned
        // to 8 by static validation). On a delegation-less runner the BE rejects the dispatch
        // synchronously (the startup isolation preflight failed there) and the job converges
        // to NOT_COMMITTED with the internal NEVER_LAUNCHED proof in one durable transition;
        // on a delegated runner with a working object-store path it may genuinely build and
        // COMMIT. Both profiles must converge terminally, never hang in RUNNING, and release
        // the possible-live slot. This first job also serves as the daemon-responsiveness
        // barrier described in the header.
        def createRows = sql """CREATE INDEX `${preflightIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
        assertEquals(1, createRows.size())
        assertEquals(1, createRows[0].size())
        String preflightJobId = createRows[0][0].toString()
        cleanupCandidates.add([jobId: preflightJobId, indexName: preflightIndexName])

        def preflightRow = waitForSettledJob(preflightJobId, 3700000L)
        assertTrue(preflightRow != null, "job ${preflightJobId} never became visible")
        String preflightState = preflightRow.State.toString()
        assertTrue(terminalStates.contains(preflightState),
                "job ${preflightJobId} never converged; stuck in ${preflightState}: ${preflightRow}")
        String preflightResultCode = preflightRow.ResultCode == null ? "" : preflightRow.ResultCode.toString()
        // The runner profile drives what later cases may assert: a clean pre-invocation
        // rejection proves this runner has no cgroup delegation (amendment G11: such a runner
        // only ever verifies rejections), COMMITTED proves the worker executed end to end,
        // and anything else means the dispatch path is degraded (e.g. the BE cannot reach the
        // object store) and only the universal invariants are asserted.
        boolean delegationLess = preflightState == "NOT_COMMITTED" &&
                preflightResultCode == "PRE_INVOCATION_RESOURCE_REJECTED"
        logger.info("lance worker runner profile: state=${preflightState} resultCode=${preflightResultCode} -> " +
                (delegationLess ? "delegation-less (clean pre-invocation rejection)"
                        : (preflightState == "COMMITTED" ? "delegated (worker executed and committed)"
                        : "degraded dispatch path")))
        // A runner that committed case 1 AND showed the released slot proves the reap-proof
        // evidence is visible here; only then do later COMMITTED rows get the strict
        // PossibleLive=NO assertion (see the header gating note).
        commitProofStrict = preflightState == "COMMITTED" && preflightRow.PossibleLive.toString() == "NO"
        logger.info("commit-proof-strict profile: ${commitProofStrict}")
        assertSlotReleased("case 1 job row", preflightRow)
        // The failure path must not leak credentials into any visible column.
        sweepForCredentials("case 1 job row", preflightRow, forbiddenInJobColumns)

        // ---- Case 2: file:// topology refusal still holds with the assertion enabled -----
        // enable_lance_index_local_file_mutation=true only lifts the operator-assertion
        // check; the dispatcher still pins local-file mutations to a single-FE/single-alive-BE
        // topology, and this suite's warehouse path exists on no filesystem anywhere, so
        // admission is expected to reject the statement outright before any job exists. If a
        // job were admitted anyway (a future FE that can see a real local dataset), it must
        // still never COMMIT a mutation: the dispatcher keeps it PENDING on any multi-node
        // topology, and on the one-node topology the worker cannot open the missing dataset
        // on the BE's filesystem. COMMITTED is the only state that commits a mutation, so its
        // absence is the no-mutation proof (the fake dataset has no entries baseline to
        // compare).
        master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_local_file_mutation" = "true")"""
        try {
            sql """
                CREATE CATALOG `${fileCatalog}` PROPERTIES (
                    "type" = "lance",
                    "lance.catalog.type" = "filesystem",
                    "warehouse" = "file:///tmp/lance_worker_negative_${runSuffix}"
                )
            """

            String fileCaught = null
            String fileJobId = null
            try {
                def fileCreateRows = sql """CREATE INDEX `${fileIndexName}` ON `${fileCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                        PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
                fileJobId = fileCreateRows[0][0].toString()
            } catch (Throwable t) {
                fileCaught = collectMessages(t)
            }
            if (fileCaught != null) {
                // Expected: the admission-time refusal (the local warehouse cannot resolve
                // the fixture table). No job row may exist for the attempted name, anywhere.
                logger.info("file:// dataset CREATE INDEX rejected at admission as expected: ${fileCaught}")
                sweepForCredentials("case 2 admission rejection", fileCaught, forbiddenInAdmissionErrors)
                def allJobs = sql_return_maparray """SHOW LANCE INDEX JOBS"""
                assertTrue(allJobs.find {
                    it.CatalogName.toString() == fileCatalog && it.IndexName.toString() == fileIndexName
                } == null)
            } else {
                // Defensive branch: admitted despite the local locator. The topology pins
                // still apply; the job must never commit a mutation, and if it converges it
                // converges like any other job.
                logger.info("file:// dataset CREATE INDEX was unexpectedly admitted as job ${fileJobId}; " +
                        "asserting the no-mutation invariants")
                def fileRow = waitForSettledJob(fileJobId, 90000L)
                assertTrue(fileRow != null)
                String fileState = fileRow.State.toString()
                assertTrue(fileState != "COMMITTED",
                        "a file:// dataset mutation must never commit: ${fileRow}")
                if (fileState == "PENDING") {
                    // Topology refusal holding: never dispatched, so no worker slot either.
                    assertEquals("NO", fileRow.PossibleLive.toString())
                } else {
                    assertTrue(terminalStates.contains(fileState),
                            "file:// job ${fileJobId} stuck in ${fileState}: ${fileRow}")
                    assertSlotReleased("case 2 file:// job row", fileRow)
                }
            }
        } finally {
            // Restore immediately so the remaining cases run with the shipped topology
            // assertion; the outer finally restores it again defensively.
            master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_local_file_mutation" = "${originalLocalFile}")"""
        }

        // ---- Case 3: queue-full pressure --------------------------------------------------
        // Four jobs rapid-fired at one table against a per-backend inflight cap of two (the
        // dispatcher's, pinned above; the BE additionally bounds its own submission queue to
        // lance_index_worker_queue_size entries). Whether a job is accepted-then-executed
        // (delegated runner), rejected with a clean synchronous error (delegation-less
        // runner), or rejected at a full queue under pressure, every job must converge
        // terminally; nothing may be left stuck in PENDING or RUNNING. The exact try_put
        // boundary itself is pinned by the commit-6 UTs; here the asserted surface is
        // convergence under pressure end to end.
        List queueJobs = []
        for (int i = 0; i < 4; i++) {
            String indexName = "idx_neg_queue_${i}_${runSuffix}"
            def queueCreateRows = sql """CREATE INDEX `${indexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, queueCreateRows.size())
            queueJobs.add([jobId: queueCreateRows[0][0].toString(), indexName: indexName])
            cleanupCandidates.add([jobId: queueCreateRows[0][0].toString(), indexName: indexName])
        }

        long queueDeadlineMs = System.currentTimeMillis() + 300000L
        Map queueFinalRows = [:]
        boolean allSettled = false
        while (!allSettled && System.currentTimeMillis() < queueDeadlineMs) {
            allSettled = true
            for (job in queueJobs) {
                def row = fetchJobRow(job.jobId)
                queueFinalRows[job.jobId] = row
                if (!isSettled(row)) {
                    allSettled = false
                }
            }
            if (!allSettled) {
                sleep(1000)
            }
        }
        int notCommitted = 0
        for (job in queueJobs) {
            def row = queueFinalRows[job.jobId]
            assertTrue(row != null, "job ${job.jobId} never became visible")
            String state = row.State.toString()
            assertTrue(terminalStates.contains(state),
                    "job ${job.jobId} (${job.indexName}) never converged; stuck in ${state}: ${row}")
            assertSlotReleased("case 3 queue job ${job.jobId}", row)
            if (state == "NOT_COMMITTED") {
                notCommitted++
            }
        }
        logger.info("queue pressure outcome: ${notCommitted} of ${queueJobs.size()} jobs NOT_COMMITTED")
        if (delegationLess) {
            // On a runner without cgroup delegation every dispatch is a clean synchronous
            // rejection, so all four jobs must end NOT_COMMITTED with the slot released.
            assertEquals(queueJobs.size(), notCommitted)
        }

        // ---- Case 4: credential non-disclosure --------------------------------------------
        // The dedicated catalog starts with the real MinIO credentials so a job can be
        // admitted; only afterwards is its secret rotated to a wrong value. Credentials are
        // not target-identity properties, so the rotation is allowed even with an unresolved
        // job, and the dispatcher resolves storage options from the catalog's current
        // properties fresh at send time — so the next dispatch of the admitted job carries
        // the wrong secret to the BE. On a delegated runner the worker then fails to open
        // the dataset and reports a classified error; on a delegation-less runner the
        // dispatch is rejected even earlier, before any storage option is read. Either way
        // the job converges terminally, can never commit, and no user-visible column may
        // disclose the wrong secret, the real credentials, or storage-option key names.
        sql """
            CREATE CATALOG `${credCatalog}` PROPERTIES (
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

        if (!delegationLess) {
            // On a runner that can actually execute, a dispatch round landing between the
            // admission below and the secret rotation would build the index with the
            // still-good credentials. Pin the per-backend inflight cap to one and occupy that
            // slot with a decoy job first: while the decoy holds the only slot the dispatcher
            // skips every other job, so the admission-to-rotation window is with overwhelming
            // margin dispatch-free. The post-rotation PENDING check below arbitrates the
            // residual race instead of trusting the margin.
            master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "1")"""
            def decoyCreateRows = sql """CREATE INDEX `${decoyIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, decoyCreateRows.size())
            String decoyJobId = decoyCreateRows[0][0].toString()
            cleanupCandidates.add([jobId: decoyJobId, indexName: decoyIndexName])
            def decoyRow = null
            long decoyDeadlineMs = System.currentTimeMillis() + 120000L
            while (System.currentTimeMillis() < decoyDeadlineMs) {
                decoyRow = fetchJobRow(decoyJobId)
                if (decoyRow != null && decoyRow.State.toString() != "PENDING") {
                    break
                }
                sleep(1000)
            }
            assertTrue(decoyRow != null && decoyRow.State.toString() != "PENDING",
                    "decoy job ${decoyJobId} never left PENDING; the credential case cannot be " +
                    "protected from a dispatch race: ${decoyRow}")
        }

        def credCreateRows = sql """CREATE INDEX `${credIndexName}` ON `${credCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
        assertEquals(1, credCreateRows.size())
        String credJobId = credCreateRows[0][0].toString()
        cleanupCandidates.add([jobId: credJobId, indexName: credIndexName])
        sql """ALTER CATALOG `${credCatalog}` SET PROPERTIES ("s3.secret_key" = "${wrongSecret}")"""

        // If the job is still PENDING now that the rotation is durable, no round dispatched
        // it with the good credentials, and every future round sends the wrong secret. On a
        // delegation-less runner even a lost race is harmless (the BE rejects before reading
        // any storage option, so the assertions below hold identically), so only the other
        // profiles degrade — loudly — instead of failing.
        def credPostRotation = fetchJobRow(credJobId)
        boolean dispatchedBeforeRotation = credPostRotation == null ||
                credPostRotation.State.toString() != "PENDING"
        if (dispatchedBeforeRotation && !delegationLess) {
            logger.info("credential job ${credJobId} was dispatched before the secret rotation landed; " +
                    "the wrong-credential dispatch path is not exercised this run: ${credPostRotation}")
        }

        def credRow = waitForSettledJob(credJobId, 240000L)
        assertTrue(credRow != null)
        String credState = credRow.State.toString()
        assertTrue(terminalStates.contains(credState),
                "credential job ${credJobId} never converged; stuck in ${credState}: ${credRow}")
        if (!dispatchedBeforeRotation) {
            // A wrong secret can never commit: it is either a clean pre-invocation rejection
            // or a classified native failure.
            assertTrue(credState == "NOT_COMMITTED" || credState == "UNKNOWN",
                    "wrong-credential job must not commit; final state ${credState}: ${credRow}")
        }
        assertSlotReleased("case 4 job row", credRow)
        logger.info("wrong-credential job ${credJobId} converged to ${credState} " +
                "(resultCode=${credRow.ResultCode}, proof=${credRow.TerminationProof})")
        sweepForCredentials("case 4 job row", credRow, forbiddenInJobColumns)
        def globalJobs = sql_return_maparray """SHOW LANCE INDEX JOBS"""
        def credPluralRow = globalJobs.find { it.IndexName.toString() == credIndexName }
        assertTrue(credPluralRow != null)
        sweepForCredentials("case 4 plural row", credPluralRow, forbiddenInJobColumns)

        // With the catalog now holding the wrong secret, a fresh CREATE INDEX must be
        // rejected at admission (the authoritative snapshot read cannot authenticate), and
        // the rejection text must not disclose the secret either.
        String credRetryCaught = null
        try {
            sql """CREATE INDEX `${credRetryIndexName}` ON `${credCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
        } catch (Throwable t) {
            credRetryCaught = collectMessages(t)
        }
        assertTrue(credRetryCaught != null,
                "CREATE INDEX against a catalog with a wrong secret must be rejected at admission")
        sweepForCredentials("case 4 admission rejection", credRetryCaught, forbiddenInAdmissionErrors)
        def jobsAfterRetry = sql_return_maparray """SHOW LANCE INDEX JOBS"""
        assertTrue(jobsAfterRetry.find { it.IndexName.toString() == credRetryIndexName } == null)
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Best-effort hygiene first, while the gate is still open and dispatch is fast: for
        // every admitted job, wait it settled and drop the index if it COMMITTED, so the
        // shared preinstalled fixture keeps the index set the entries/show_index suites pin.
        // DROP IF EXISTS of a name that never committed is an immediate zero-row no-op, and
        // each real DROP is its own dispatched job. Everything here is tolerant: a failure of
        // the scenario being cleaned up after must never be masked by cleanup noise.
        for (candidate in cleanupCandidates) {
            try {
                def candidateRow = waitForSettledJob(candidate.jobId, 120000L)
                if (candidateRow == null || candidateRow.State.toString() != "COMMITTED") {
                    continue
                }
                def dropRows = sql """DROP INDEX IF EXISTS `${candidate.indexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                if (!dropRows.isEmpty()) {
                    def dropRow = waitForSettledJob(dropRows[0][0].toString(), 120000L)
                    if (dropRow == null || dropRow.State.toString() != "COMMITTED") {
                        logger.info("cleanup DROP INDEX ${candidate.indexName} did not commit: ${dropRow}")
                    }
                }
            } catch (Throwable t) {
                logger.info("cleanup of index ${candidate.indexName} failed: ${t}")
            }
        }
        // Attempt every config restore, but never report success after a failed restore.
        // Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "${originalInterval}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_max_inflight_per_backend" = "${originalInflight}")""" },
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
        // A catalog whose jobs are all resolved can be dropped; one holding an unresolved job
        // (a PENDING file:// job the dispatcher refuses by topology, or an UNKNOWN
        // wrong-credential job awaiting FORCE_RELEASE in a later slice) is guarded against
        // DROP CATALOG and stays behind, exactly like the admission suite's per-run catalog.
        // These drops are deliberately tolerant.
        try_sql """DROP CATALOG IF EXISTS `${fileCatalog}`"""
        try_sql """DROP CATALOG IF EXISTS `${credCatalog}`"""
        try_sql """DROP CATALOG IF EXISTS `${filesystemCatalog}`"""
    }
}
