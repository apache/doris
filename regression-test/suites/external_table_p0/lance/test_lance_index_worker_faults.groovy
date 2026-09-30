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

// PR4B worker-fault coverage: UNKNOWN convergence and backend survival when the
// cgroup-isolated Lance index worker dies, hangs, or goes silent mid-invocation,
// using the fault-injection debug points added in this PR:
//   * LanceIndexWorker.hang          - the worker blocks after dispatch
//                                      validation, before the first lance FFI
//                                      call, so no mutation can ever have
//                                      happened;
//   * LanceIndexWorker.skip_report   - the worker completes the native
//                                      invocation but exits 0 without writing its
//                                      result frame, so the dataset-side mutation
//                                      happened but the job never learns it;
//   * LanceIndexSupervisor.reject_after_enqueue - the supervisor rejects the
//                                      dispatch after dequeue with the full
//                                      NEVER_LAUNCHED envelope (D6 path 3).
// The two worker-side points ride the supervisor's controlled-environment
// handoff (DORIS_LANCE_WORKER_DEBUG_POINTS) into the exec'd worker, so enabling
// them on the BEs before the dispatch is all this suite has to do.
//
// Deadline budget math: the BE subtracts lance_index_worker_report_margin_seconds
// (default 400) and the termination grace (default 10) from the FE-stamped
// deadline and refuses to launch a worker whose remaining wall clock would not
// exceed the 5-second minimum executable budget, so the "short" deadline of this
// suite is 480 seconds: the worker gets a 70-second wall clock, and the FE
// deadline sweep converges the job RUNNING -> UNKNOWN (ResultCode
// NO_TRUSTED_RESULT, refresh NOT_REQUIRED) at deadline expiry, one dispatch round
// later. The supervisor-side termination proof (CHILD_REAPED) lands much earlier
// - seconds after a manual kill, ~80 seconds after the supervisor's own
// TERM->KILL escalation of a hung worker - and releases the possible-live slot
// without touching the UNKNOWN outcome, which is exactly the UNKNOWN semantics
// this suite pins.
//
// Runner-profile derivation (same probe pattern as the tracer and worker-negative
// suites): the opening probe CREATE INDEX runs with no fault armed. COMMITTED
// proves a delegation-capable runner, and cases 1-3 (which need a worker that
// actually launches) proceed; NOT_COMMITTED with PRE_INVOCATION_RESOURCE_REJECTED
// proves a delegation-less runner, and those cases are skipped with a loud log
// line. Any other probe outcome means a degraded dispatch path and is also
// skip-with-loud-log for cases 1-3 (a false worker-fault green would be worse
// than no evidence). Case 4 (reject_after_enqueue) produces the same terminal
// envelope on every profile - on a delegation-less runner the handler rejects the
// dispatch synchronously with the identical NOT_COMMITTED + NEVER_LAUNCHED
// observables - so it runs unconditionally. The strict PossibleLive=NO /
// CHILD_REAPED assertions are derived from the probe's observed slot release,
// exactly like the worker-negative suite: a kernel without pidfd retains the
// slot until the BE epoch sweep by design (D5/D16).
//
// Probe-outcome gate decision (the deliberate asymmetry with the tracer suite,
// documented so it is a conscious choice rather than an accident): the tracer
// fails loudly on a probe UNKNOWN because its positive proof is meaningless
// without a committed build, while this suite degrades to envelope-only evidence
// (case 4) on any non-COMMITTED probe outcome. A degraded dispatch path between
// the tracer and faults runs therefore still reports this suite green on the
// case-4 envelope alone - accepted, because cases 1-3 cannot distinguish
// "worker killed" from "worker never existed" without a COMMITTED probe, and a
// false worker-fault green is worse than no evidence.
//
// Per-run catalog leak convention: the UNKNOWN fault jobs (kill/hang/skip-drop)
// hold their same-name fence until FORCE_RELEASE lands in a later slice, so the
// run-suffixed catalog can never be DROP CATALOG-ed and each run leaves exactly
// one catalog behind on a shared cluster. The accumulation is bounded at one
// catalog per run and matches the worker-negative suite's documented convention;
// it is tracked until FORCE_RELEASE makes these catalogs droppable.
//
// Case ordering: case 4 runs before the delegation-gated cases because it needs
// no worker, converges in seconds, and leaves no worker-fault residue behind.
//
// Hygiene (recon section 5): the manual kill targets the invocation's exact PID
// only - the worker is located by pgrep -f 'doris_be --lance-worker' on the
// dispatch's own backend host and attributed to this job through the
// lance-worker-<jobId>-* invocation cgroup visible in /proc/<pid>/cgroup; pkill
// by pattern is never used. Every finally clears only the debug points this
// suite armed and drops only the indexes this suite committed.
//
// skip_report deliberately targets a DROP job rather than a CREATE job: the
// worker physically removes the freshly built index and then goes silent, which
// is the UNKNOWN semantics (the dataset-side mutation happened but the job never
// learns it) with the physical end state the suite wanted anyway. A CREATE under
// skip_report would instead leave a physically committed orphan index whose
// same-name fence is held by the UNKNOWN job until FORCE_RELEASE lands in a
// later slice, polluting the shared preinstalled fixture that the
// entries/show_index suites pin. No entries assertion is made for that job
// either way: UNKNOWN owns no refresh, so the catalog-side index view is not
// part of its contract.

suite("test_lance_index_worker_faults", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index worker faults test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // Every index name and the catalog carry this per-run suffix so that
    // rerunning the suite on a shared pipeline cluster can never collide with a
    // previous run's leftovers (fence/quota keys include the persisted catalog
    // id).
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_worker_faults_${runSuffix}"
    String cleanupCatalog = "test_lance_index_faults_clean_${runSuffix}"
    String tableName = "vs_ivf_pq_f32"
    String probeIndexName = "idx_fault_probe_${runSuffix}"
    String rejectIndexName = "idx_fault_reject_${runSuffix}"
    String killIndexName = "idx_fault_kill_${runSuffix}"
    String followupIndexName = "idx_fault_followup_${runSuffix}"
    String hangIndexName = "idx_fault_hang_${runSuffix}"
    String skipIndexName = "idx_fault_skip_${runSuffix}"
    String hangPoint = "LanceIndexWorker.hang"
    String skipReportPoint = "LanceIndexWorker.skip_report"
    String rejectPoint = "LanceIndexSupervisor.reject_after_enqueue"
    // The fault-phase deadline: 480s of FE deadline gives the worker a 70s wall
    // clock after the report margin and the termination grace (see the header),
    // and converges UNKNOWN cases at the sweep ~8 minutes after dispatch.
    String faultDeadlineSecond = "480"
    List<String> terminalStates = ["COMMITTED", "NOT_COMMITTED", "UNKNOWN"]

    // All three settings are masterOnly. Read them on the master even if the
    // suite's ordinary JDBC connection points at a follower. SHOW uses the
    // experimental display name for the gate, while ADMIN SET accepts its
    // unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def intervalRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_dispatch_interval_second'"""
    def deadlineRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_execute_deadline_second'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, intervalRows.size())
    assertEquals(1, deadlineRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalInterval = intervalRows[0][1].toString()
    String originalDeadline = deadlineRows[0][1].toString()
    // The shipped defaults are asserted (not just captured) so a drifted shared
    // cluster fails loudly instead of being silently restored to a non-default
    // value afterwards.
    assertEquals("10", originalInterval)
    assertEquals("3600", originalDeadline)
    Throwable suiteFailure = null
    String probeJobId = null
    String skipCreateJobId = null
    boolean commitProofStrict = false
    boolean delegated = false
    Set lenientCommittedLogged = new HashSet()
    // Indexes this suite committed on the main catalog, dropped in the finally
    // block ([jobId, indexName] pairs, mirroring the worker-negative suite).
    List cleanupCandidates = []
    // The skip_report target: its fault-DROP job converges UNKNOWN and holds the
    // same-name fence until FORCE_RELEASE (a later slice), so its cleanup drop
    // must go through a separate catalog id (see the header).
    boolean skipIndexCommitted = false

    // One SHOW LANCE INDEX JOB lookup by id: the singular form authorizes by job,
    // needs no FROM resolution, and exposes the inspection columns (ResultCode,
    // TerminationProof, BackendId, ...) the plural listing lacks.
    def fetchJobRow = { String jobId ->
        def rows = sql_return_maparray """SHOW LANCE INDEX JOB ${jobId}"""
        return rows.isEmpty() ? null : rows[0]
    }

    // Settle rules, identical to the worker-negative suite: UNKNOWN settles on the
    // state alone, NOT_COMMITTED requires the envelope-carried slot release,
    // COMMITTED is strict only when the probe proved the reap-proof evidence is
    // visible on this runner.
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

    // Bounded poll; returns the last observed row for the caller to assert on.
    def waitForSettledJob = { String jobId, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        def row = fetchJobRow(jobId)
        while (!isSettled(row) && System.currentTimeMillis() < deadlineMs) {
            sleep(1000)
            row = fetchJobRow(jobId)
        }
        return row
    }

    // Bounded poll for one mutation state; returns the last row.
    def waitForState = { String jobId, String target, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        def row = fetchJobRow(jobId)
        while (row != null && row.State.toString() != target && System.currentTimeMillis() < deadlineMs) {
            sleep(1000)
            row = fetchJobRow(jobId)
        }
        return row
    }

    // Bounded poll for the possible-live slot release; returns the last row.
    def waitForPossibleLiveRelease = { String jobId, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        def row = fetchJobRow(jobId)
        while (row != null && row.PossibleLive.toString() != "NO" && System.currentTimeMillis() < deadlineMs) {
            sleep(1000)
            row = fetchJobRow(jobId)
        }
        return row
    }

    // Bounded poll for one refresh state; returns the last row.
    def waitForRefreshState = { String jobId, String target, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        def row = fetchJobRow(jobId)
        while (row != null && row.RefreshState.toString() != target && System.currentTimeMillis() < deadlineMs) {
            sleep(1000)
            row = fetchJobRow(jobId)
        }
        return row
    }

    // The PossibleLive=NO / CHILD_REAPED assertion honoring the runner-profile
    // gate: on a runner where the supervisor cannot form the reap proof the slot
    // is retained by design, so a miss is a loud log line instead of a failure.
    def assertTerminationProof = { String label, String jobId, row ->
        if (commitProofStrict) {
            assertEquals("NO", row.PossibleLive.toString(),
                    "${label}: the termination proof must release the worker slot on this runner: ${row}")
            assertEquals("CHILD_REAPED", row.TerminationProof.toString(),
                    "${label}: a reaped worker must leave the CHILD_REAPED proof: ${row}")
        } else if (row.PossibleLive.toString() != "NO") {
            logger.info("${label}: job ${jobId} keeps PossibleLive=${row.PossibleLive} on this runner " +
                    "profile (reap-proof evidence unavailable by design); the slot release rides the " +
                    "BE epoch sweep: ${row}")
        }
    }

    // Run one shell command on a backend host and capture stdout, following the
    // pythonudf precedent: local shell for a loopback host, ssh otherwise. The
    // remote command must not contain single quotes (the ssh wrapper uses them).
    def execOnBackend = { String beIp, String remoteCmd ->
        if (beIp == "127.0.0.1" || beIp == "localhost") {
            return cmd(remoteCmd)
        }
        return cmd("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o BatchMode=yes root@${beIp} '${remoteCmd}'")
    }

    def isShellReachable = { String beIp ->
        try {
            String probeOut = execOnBackend(beIp, "echo ok")
            return probeOut.readLines().any { it.trim() == "ok" }
        } catch (Throwable t) {
            logger.info("BE host ${beIp} is not shell-reachable from the regression driver: ${t}")
            return false
        }
    }

    // Locate this job's exact worker PID on one backend host. The worker argv is
    // exactly 'doris_be --lance-worker'; the pattern is start-anchored so the
    // shell wrapper running pgrep itself can never self-match, and the invocation
    // cgroup name carries the job id, which attributes the PID to this invocation
    // precisely (pkill by pattern is never used). When cgroup attribution is
    // unreadable the poll does NOT fall back to a sole candidate: on a shared BE
    // host that PID could be another suite's worker, so an unattributed kill is
    // never attempted — returning null degrades the case to the supervisor's
    // wall-clock termination, which carries the convergence and survival
    // assertions anyway (case 2's mechanism). Returns null on timeout or
    // ambiguity.
    def findWorkerPid = { String beIp, String jobId, long timeoutMs ->
        long deadlineMs = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadlineMs) {
            try {
                String out = execOnBackend(beIp, "pgrep -f \"^doris_be --lance-worker\" || true")
                List<String> pids = out.readLines().collect { it.trim() }.findAll { it ==~ /\d+/ }
                if (!pids.isEmpty()) {
                    List<String> ours = pids.findAll { pid ->
                        String cgroup = execOnBackend(beIp, "cat /proc/${pid}/cgroup 2>/dev/null || true")
                        cgroup.contains("lance-worker-${jobId}-")
                    }
                    if (ours.size() == 1) {
                        return ours[0]
                    }
                    logger.info("worker pid attribution for job ${jobId} on ${beIp} is ambiguous: " +
                            "candidates=${pids} attributed=${ours}; retrying (no unattributed kill " +
                            "is ever attempted; unattributed degrades to the wall-clock path)")
                }
            } catch (Throwable t) {
                // A transient shell/ssh blip must not sink a forty-minute suite;
                // the poll retries until the window closes.
                logger.info("worker pid poll for job ${jobId} on ${beIp} failed; retrying: ${t}")
            }
            sleep(1000)
        }
        return null
    }

    def assertAllBackendsAlive = { String label ->
        def backends = sql_return_maparray """SHOW BACKENDS"""
        assertTrue(!backends.isEmpty(), "${label}: no backends visible")
        def dead = backends.findAll { !it.Alive.toString().equalsIgnoreCase("true") }
        assertTrue(dead.isEmpty(),
                "${label}: every backend must survive a worker fault: ${dead}")
    }

    try {
        // Fast dispatch rounds, the admission gate, and the bounded fault-phase
        // deadline for this suite only; the finally block restores every captured
        // setting no matter where the suite fails. The interval SET takes effect
        // within one bounded sleep slice (the daemon re-reads the config at every
        // wake), so dispatch rounds fire within seconds.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "1")"""
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_execute_deadline_second" = "${faultDeadlineSecond}")"""
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

        def backendIdToIp = [:]
        def backendIdToHttpPort = [:]
        getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)

        // ---- Runner-profile probe (no fault armed) -----------------------------
        // Same derivation as the tracer suite: COMMITTED proves delegation,
        // NOT_COMMITTED + PRE_INVOCATION_RESOURCE_REJECTED proves a
        // delegation-less runner, anything else is a degraded dispatch path.
        def probeCreateRows = sql """CREATE INDEX `${probeIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
        assertEquals(1, probeCreateRows.size())
        assertEquals(1, probeCreateRows[0].size())
        probeJobId = probeCreateRows[0][0].toString()
        cleanupCandidates.add([jobId: probeJobId, indexName: probeIndexName])

        def probeRow = waitForSettledJob(probeJobId, 600000L)
        assertTrue(probeRow != null, "probe job ${probeJobId} never became visible")
        String probeState = probeRow.State.toString()
        assertTrue(terminalStates.contains(probeState),
                "probe job ${probeJobId} never converged; stuck in ${probeState}: ${probeRow}")
        String probeResultCode = probeRow.ResultCode == null ? "" : probeRow.ResultCode.toString()
        delegated = probeState == "COMMITTED"
        logger.info("lance worker runner profile: state=${probeState} resultCode=${probeResultCode} -> " +
                (delegated ? "delegated (worker executed and committed)"
                        : (probeState == "NOT_COMMITTED" && probeResultCode == "PRE_INVOCATION_RESOURCE_REJECTED"
                        ? "delegation-less (clean pre-invocation rejection)"
                        : "degraded dispatch path")))
        if (delegated) {
            def probeLiveRow = waitForPossibleLiveRelease(probeJobId, 20000L)
            commitProofStrict = probeLiveRow != null && probeLiveRow.PossibleLive.toString() == "NO"
            logger.info("commit-proof-strict profile: ${commitProofStrict}")
        }

        // ---- Case 4: reject_after_enqueue (runs on every runner profile) -------
        // The supervisor rejects the dispatch right after dequeue with the full
        // async NEVER_LAUNCHED envelope; on a delegation-less runner the handler
        // rejects the same dispatch synchronously before the supervisor ever sees
        // it, with the identical durable observables. Either way the job converges
        // NOT_COMMITTED - never UNKNOWN - with the slot released.
        GetDebugPoint().enableDebugPointForAllBEs(rejectPoint)
        try {
            def rejectCreateRows = sql """CREATE INDEX `${rejectIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, rejectCreateRows.size())
            assertEquals(1, rejectCreateRows[0].size())
            String rejectJobId = rejectCreateRows[0][0].toString()

            def rejectRow = waitForSettledJob(rejectJobId, 180000L)
            assertTrue(rejectRow != null, "reject job ${rejectJobId} never became visible")
            assertEquals("NOT_COMMITTED", rejectRow.State.toString(),
                    "a never-launched invocation must converge NOT_COMMITTED, never UNKNOWN: ${rejectRow}")
            assertEquals("PRE_INVOCATION_RESOURCE_REJECTED", rejectRow.ResultCode.toString(),
                    "the async rejection envelope must carry the resource-rejected code: ${rejectRow}")
            assertEquals("NEVER_LAUNCHED", rejectRow.TerminationProof.toString(),
                    "the async rejection envelope must carry the NEVER_LAUNCHED proof: ${rejectRow}")
            assertEquals("NO", rejectRow.PossibleLive.toString(),
                    "a provably never-launched invocation releases its slot with the result: ${rejectRow}")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(rejectPoint)
        }

        if (!delegated) {
            logger.info("LOUD SKIP: cases 1-3 need a worker that actually launches, and this runner " +
                    "profile cannot provide one (see the probe result above); the delegation-less " +
                    "profile's coverage lives in test_lance_index_worker_negative, and case 4's " +
                    "envelope assertions above already ran on this profile.")
        } else {
            // ---- Case 1: precise-PID kill of the hung worker -------------------
            // The hang point blocks the worker after dispatch validation, before
            // the first FFI call, so no mutation can ever happen for this job.
            // Killing the exact worker PID must converge the job UNKNOWN at the
            // deadline sweep, land the CHILD_REAPED proof much earlier, and leave
            // the backend alive and executing.
            GetDebugPoint().enableDebugPointForAllBEs(hangPoint)
            String killJobId = null
            try {
                def killCreateRows = sql """CREATE INDEX `${killIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                        PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
                assertEquals(1, killCreateRows.size())
                assertEquals(1, killCreateRows[0].size())
                killJobId = killCreateRows[0][0].toString()

                def runningRow = waitForState(killJobId, "RUNNING", 300000L)
                assertTrue(runningRow != null && runningRow.State.toString() == "RUNNING",
                        "kill job ${killJobId} never reached RUNNING: ${runningRow}")
                String backendId = runningRow.BackendId.toString()
                String beIp = backendIdToIp[backendId]
                assertTrue(beIp != null, "kill job ${killJobId} dispatched to unknown backend ${backendId}")

                if (!isShellReachable(beIp)) {
                    // No shell on the BE host from this driver: the supervisor's
                    // 70-second wall clock terminates the hung worker instead, and
                    // the convergence and survival assertions below still carry
                    // the case. Record loudly that the manual-kill evidence is
                    // missing on this runner.
                    logger.info("LOUD: BE host ${beIp} is not shell-reachable from the regression " +
                            "driver, so the precise-PID kill is not exercised this run; the " +
                            "supervisor's wall-clock deadline terminates the hung worker instead")
                } else {
                    String workerPid = findWorkerPid(beIp, killJobId, 120000L)
                    if (workerPid == null) {
                        // The supervisor's 70-second wall clock terminates the
                        // hung worker anyway, and the convergence and survival
                        // assertions below still carry the case; record loudly
                        // that the manual-kill evidence is missing this run.
                        logger.info("LOUD: the hung worker of job ${killJobId} was never located on " +
                                "${beIp} within the poll window, so the precise-PID kill is not " +
                                "exercised this run; the supervisor's wall-clock deadline terminates " +
                                "the worker instead")
                    } else {
                        logger.info("killing the exact worker pid ${workerPid} of job ${killJobId} on ${beIp}")
                        try {
                            execOnBackend(beIp, "kill -9 ${workerPid}")
                        } catch (Throwable t) {
                            // The supervisor's wall clock may have reaped the worker
                            // first; the job still converges UNKNOWN either way.
                            logger.info("kill -9 ${workerPid} on ${beIp} failed (the supervisor may have " +
                                    "terminated the hung worker first): ${t}")
                        }
                    }
                }

                def liveRow = waitForPossibleLiveRelease(killJobId, commitProofStrict ? 240000L : 60000L)
                assertTrue(liveRow != null)
                assertTerminationProof("case 1 kill", killJobId, liveRow)

                def killRow = waitForSettledJob(killJobId, 700000L)
                assertTrue(killRow != null)
                assertEquals("UNKNOWN", killRow.State.toString(),
                        "a killed worker must converge the job UNKNOWN: ${killRow}")
                assertEquals("NO_TRUSTED_RESULT", killRow.ResultCode.toString(),
                        "the deadline sweep converges with NO_TRUSTED_RESULT: ${killRow}")
                assertEquals("NOT_REQUIRED", killRow.RefreshState.toString(),
                        "an UNKNOWN job owes no refresh: ${killRow}")
                assertAllBackendsAlive("case 1 kill -9 of the lance worker")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(hangPoint)
            }

            // Backend survival, end to end: with the fault cleared, a follow-up
            // job on the same backend executes and commits normally.
            def followupCreateRows = sql """CREATE INDEX `${followupIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, followupCreateRows.size())
            assertEquals(1, followupCreateRows[0].size())
            String followupJobId = followupCreateRows[0][0].toString()
            cleanupCandidates.add([jobId: followupJobId, indexName: followupIndexName])
            def followupRow = waitForSettledJob(followupJobId, 300000L)
            assertTrue(followupRow != null)
            assertEquals("COMMITTED", followupRow.State.toString(),
                    "the follow-up job must commit on the backend that survived the worker kill: ${followupRow}")

            // ---- Case 2: hang + short deadline (the supervisor self-kill) ------
            // Nobody kills this worker: the supervisor's own wall clock
            // (deadline 480s - report margin 400s - term grace 10s = 70s) expires,
            // TERM->KILL escalates, the CHILD_REAPED proof lands, and the FE
            // deadline sweep converges the job UNKNOWN at the deadline - the
            // bounded UNKNOWN path for a worker that simply never comes back.
            GetDebugPoint().enableDebugPointForAllBEs(hangPoint)
            try {
                def hangCreateRows = sql """CREATE INDEX `${hangIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                        PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
                assertEquals(1, hangCreateRows.size())
                assertEquals(1, hangCreateRows[0].size())
                String hangJobId = hangCreateRows[0][0].toString()

                def hangRunningRow = waitForState(hangJobId, "RUNNING", 300000L)
                assertTrue(hangRunningRow != null && hangRunningRow.State.toString() == "RUNNING",
                        "hang job ${hangJobId} never reached RUNNING: ${hangRunningRow}")

                def hangLiveRow = waitForPossibleLiveRelease(hangJobId, commitProofStrict ? 300000L : 60000L)
                assertTrue(hangLiveRow != null)
                assertTerminationProof("case 2 deadline kill", hangJobId, hangLiveRow)

                def hangRow = waitForSettledJob(hangJobId, 700000L)
                assertTrue(hangRow != null)
                assertEquals("UNKNOWN", hangRow.State.toString(),
                        "a worker that outlives its wall clock must converge the job UNKNOWN: ${hangRow}")
                assertEquals("NO_TRUSTED_RESULT", hangRow.ResultCode.toString(),
                        "the deadline sweep converges with NO_TRUSTED_RESULT: ${hangRow}")
                assertEquals("NOT_REQUIRED", hangRow.RefreshState.toString(),
                        "an UNKNOWN job owes no refresh: ${hangRow}")
                assertAllBackendsAlive("case 2 supervisor TERM->KILL of the hung worker")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(hangPoint)
            }

            // ---- Case 3: skip_report on a DROP job -----------------------------
            // The target index is built normally first (no fault armed). The DROP
            // under skip_report then executes the native removal and exits 0
            // without a result frame: the dataset-side mutation happened but the
            // job never learns it, converging UNKNOWN at the deadline. See the
            // header for why the fault targets a DROP and why no entries
            // assertion is made for this job.
            def skipCreateRows = sql """CREATE INDEX `${skipIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, skipCreateRows.size())
            assertEquals(1, skipCreateRows[0].size())
            skipCreateJobId = skipCreateRows[0][0].toString()
            def skipCreateRow = waitForSettledJob(skipCreateJobId, 300000L)
            assertTrue(skipCreateRow != null)
            assertEquals("COMMITTED", skipCreateRow.State.toString(),
                    "the skip_report target index must commit before its fault drop: ${skipCreateRow}")
            skipIndexCommitted = true
            def skipCreateRefresh = waitForRefreshState(skipCreateJobId, "DONE", 240000L)
            assertTrue(skipCreateRefresh != null)
            assertEquals("DONE", skipCreateRefresh.RefreshState.toString(),
                    "the skip_report target's refresh never completed: ${skipCreateRefresh}")

            GetDebugPoint().enableDebugPointForAllBEs(skipReportPoint)
            try {
                def skipDropRows = sql """DROP INDEX `${skipIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                assertEquals(1, skipDropRows.size())
                assertEquals(1, skipDropRows[0].size())
                String skipDropJobId = skipDropRows[0][0].toString()

                def skipLiveRow = waitForPossibleLiveRelease(skipDropJobId, commitProofStrict ? 240000L : 60000L)
                assertTrue(skipLiveRow != null)
                assertTerminationProof("case 3 silent exit", skipDropJobId, skipLiveRow)

                def skipRow = waitForSettledJob(skipDropJobId, 700000L)
                assertTrue(skipRow != null)
                assertEquals("UNKNOWN", skipRow.State.toString(),
                        "a worker that never reports must converge the job UNKNOWN: ${skipRow}")
                assertEquals("NO_TRUSTED_RESULT", skipRow.ResultCode.toString(),
                        "the deadline sweep converges with NO_TRUSTED_RESULT: ${skipRow}")
                assertEquals("NOT_REQUIRED", skipRow.RefreshState.toString(),
                        "an UNKNOWN job owes no refresh: ${skipRow}")
                assertAllBackendsAlive("case 3 silent worker exit after the native drop")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(skipReportPoint)
            }
        }
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Disarm every point this suite armed, tolerantly: a hung worker launched
        // before the disarm still dies on its own wall clock, and future launches
        // see a clean registry.
        [
            { GetDebugPoint().disableDebugPointForAllBEs(hangPoint) },
            { GetDebugPoint().disableDebugPointForAllBEs(skipReportPoint) },
            { GetDebugPoint().disableDebugPointForAllBEs(rejectPoint) }
        ].each { disarm ->
            try {
                disarm()
            } catch (Throwable t) {
                logger.info("disarming a lance fault debug point failed: ${t}")
            }
        }
        // Best-effort fixture hygiene while the gate is still open and dispatch is
        // fast: for every committed index, wait the same-name fence released
        // (refresh DONE) and drop it, so the shared preinstalled fixture keeps
        // the index set the entries/show_index suites pin. Indexes whose jobs
        // converged UNKNOWN under the pre-FFI fault points (kill, hang) provably
        // never existed physically and need no drop.
        for (candidate in cleanupCandidates) {
            try {
                def candidateRow = waitForSettledJob(candidate.jobId, 120000L)
                if (candidateRow == null || candidateRow.State.toString() != "COMMITTED") {
                    continue
                }
                waitForRefreshState(candidate.jobId, "DONE", 120000L)
                def dropRows = sql """DROP INDEX IF EXISTS `${candidate.indexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                if (!dropRows.isEmpty()) {
                    def dropRow = waitForSettledJob(dropRows[0][0].toString(), 300000L)
                    if (dropRow == null || dropRow.State.toString() != "COMMITTED") {
                        logger.info("cleanup DROP INDEX ${candidate.indexName} did not commit: ${dropRow}")
                    }
                }
            } catch (Throwable t) {
                logger.info("cleanup of index ${candidate.indexName} failed: ${t}")
            }
        }
        // The skip_report target: its create committed (the index physically
        // existed) and its fault-DROP converged UNKNOWN, which holds the
        // same-name fence on this catalog until FORCE_RELEASE lands in a later
        // slice. The worker normally removed the index physically before going
        // silent; the residual FFI-failure case leaves it behind, and only a
        // separate catalog id escapes the fence. A fresh catalog reads the
        // current dataset state, so IF EXISTS no-ops when nothing is left.
        if (skipIndexCommitted) {
            try {
                sql """
                    CREATE CATALOG `${cleanupCatalog}` PROPERTIES (
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
                def cleanDropRows = sql """DROP INDEX IF EXISTS `${skipIndexName}` ON `${cleanupCatalog}`.`doris`.`${tableName}`"""
                if (!cleanDropRows.isEmpty()) {
                    String cleanDropJobId = cleanDropRows[0][0].toString()
                    def cleanDropRow = waitForSettledJob(cleanDropJobId, 300000L)
                    if (cleanDropRow == null || cleanDropRow.State.toString() != "COMMITTED") {
                        logger.info("cleanup-catalog DROP INDEX ${skipIndexName} did not commit: ${cleanDropRow}")
                    } else {
                        // Let the drop job's own fence release so the cleanup
                        // catalog itself can be dropped below.
                        waitForRefreshState(cleanDropJobId, "DONE", 120000L)
                    }
                }
            } catch (Throwable t) {
                logger.info("cleanup-catalog drop of index ${skipIndexName} failed: ${t}")
            }
        }
        // Attempt every config restore, but never report success after a failed
        // restore. Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "${originalInterval}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_execute_deadline_second" = "${originalDeadline}")""" }
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
        // Catalogs holding unresolved jobs (UNKNOWN fault jobs awaiting
        // FORCE_RELEASE in a later slice) are guarded against DROP CATALOG and
        // stay behind, exactly like the worker-negative suite's per-run catalog.
        // These drops are deliberately tolerant.
        try_sql """DROP CATALOG IF EXISTS `${cleanupCatalog}`"""
        try_sql """DROP CATALOG IF EXISTS `${filesystemCatalog}`"""
    }
}
