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

// PR4B positive tracer: an IVF_PQ index build through the cgroup-isolated Lance
// index worker, end to end. The suite proves the full delegation chain on the
// shared preinstalled fixture: the CREATE INDEX job is admitted and dispatched, a
// real worker process runs the lance build inside its cgroup, the job commits, the
// catalog refresh exposes the new index through lance_index_entries at an advanced
// dataset version, vector_search genuinely consumes an index (the nprobes
// discriminator), and a second committed job drops the index and advances the
// dataset version again. The preinstalled index row must be undisturbed
// throughout, and every index this suite commits is dropped before exit so the
// shared fixture keeps the index set that the entries/show_index suites pin.
//
// Runner-profile gate (amendment G11): the positive proof needs a runner whose
// backend can actually launch the isolated worker (a writable delegated cgroup v2
// subtree). Groovy cannot read BE cgroup state, so the profile is derived
// empirically from the tracer job's own terminal result, mirroring the
// worker-negative suite: NOT_COMMITTED with ResultCode=
// PRE_INVOCATION_RESOURCE_REJECTED means the startup isolation preflight rejected
// the dispatch (no cgroup delegation), and the positive remainder is skipped with
// a loud log line - that profile's coverage lives in
// test_lance_index_worker_negative. UNKNOWN (or any other outcome) means the
// dispatch path is broken in a way this suite must not paper over, and the suite
// fails loudly. COMMITTED is itself the delegation proof: a worker genuinely ran
// inside its cgroup and committed the index.
//
// PossibleLive gating note (the design-tolerated path documented in the
// worker-negative suite): a COMMITTED job's CHILD_REAPED proof is BE-side evidence
// that needs a kernel with pidfd/waitid(P_PIDFD); where the supervisor cannot form
// it, the slot is deliberately retained until the BE epoch changes (D5/D16). The
// strict PossibleLive=NO assertion is therefore derived from the tracer job's own
// outcome: COMMITTED with the slot visibly released proves the evidence lands on
// this runner.
//
// Timing note: the dispatcher daemon sleeps in bounded slices (never longer than
// MAX_SLEEP_SLICE_MS) and re-reads lance_index_job_dispatch_interval_second at every
// wake, so the ADMIN SET below takes effect within one slice and no inherited-sleep
// barrier is needed; polling budgets are generous but bounded (the worker build on
// the 1024-row fixture is seconds).

suite("test_lance_index_worker_tracer", "p0,external,nonConcurrent") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance index worker tracer test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    // The catalog name carries this per-run suffix so that rerunning the suite on a
    // shared pipeline cluster can never collide with a previous run's leftovers
    // (fence/quota keys include the persisted catalog id).
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_worker_tracer_${runSuffix}"
    String tableName = "vs_ivf_pq_f32"
    // vs_ivf_pq_f32 ships with exactly one preloaded IVF_PQ index on the embedding
    // column; its entries row is the undisturbed-row anchor for every assertion.
    String preloadedIndex = "embedding_ivf_pq_f32"
    String tracerIndexName = "idx_tracer_${runSuffix}"
    // The tracer job's parameters genuinely train on the 1024-row fixture
    // (num_partitions <= 4 per the Lance sample-rate default of 256 training
    // points per partition, num_sub_vectors divides the 16-dimensional embedding,
    // num_bits is pinned to 8 by static validation).
    String headQuery = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    List<String> terminalStates = ["COMMITTED", "NOT_COMMITTED", "UNKNOWN"]

    // Both settings are masterOnly. Read them on the master even if the suite's
    // ordinary JDBC connection points at a follower. SHOW uses the experimental
    // display name for the gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    def intervalRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'lance_index_job_dispatch_interval_second'"""
    assertEquals(1, gateRows.size())
    assertEquals(1, intervalRows.size())
    String originalGate = gateRows[0][1].toString()
    String originalInterval = intervalRows[0][1].toString()
    // The shipped default is asserted (not just captured) so a drifted shared
    // cluster fails loudly instead of being silently restored to a non-default
    // value afterwards.
    assertEquals("10", originalInterval)
    Throwable suiteFailure = null
    String tracerJobId = null
    String dropJobId = null
    // Runner-profile state, derived from the tracer job's terminal row (see the
    // header): strict PossibleLive assertions apply to COMMITTED rows only when
    // the reap-proof evidence is visible on this runner.
    boolean commitProofStrict = false
    Set lenientCommittedLogged = new HashSet()

    // One SHOW LANCE INDEX JOB lookup by id: the singular form authorizes by job,
    // needs no FROM resolution, and exposes the inspection columns (ResultCode,
    // CompletionReason, TerminationProof, ...) the plural listing lacks.
    def fetchJobRow = { String jobId ->
        def rows = sql_return_maparray """SHOW LANCE INDEX JOB ${jobId}"""
        return rows.isEmpty() ? null : rows[0]
    }

    // An observation is settled once the state is terminal and the possible-live
    // slot release that must ride with it has landed. UNKNOWN settles on the state
    // alone (its proof may arrive later or ride the epoch sweep); NOT_COMMITTED
    // always requires PossibleLive=NO (its proofs are FE-internal or
    // envelope-carried, so the release always lands with the result); COMMITTED is
    // runner-profile gated (see the header).
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

    // The physical index entries of the fixture table through our catalog.
    def entriesRows = {
        def rows = sql_return_maparray """SELECT IndexName, IndexUuid, DatasetVersion
                FROM lance_index_entries("table" = "${filesystemCatalog}.doris.${tableName}")"""
        return rows
    }

    // The dataset version the scan planner pins for this table right now, parsed
    // out of EXPLAIN's lanceVersion= field: the observable that proves each
    // committed mutation advanced the dataset.
    def explainDatasetVersion = { String label ->
        def planRows = sql """EXPLAIN SELECT row_id, _distance FROM
                vector_search("table"="${filesystemCatalog}.doris.${tableName}", "column"="embedding",
                "query_vector"="${headQuery}", "top_k"="1", "metric"="l2", "use_index"="false")"""
        String planText = planRows.collect { row ->
            row.collect { cell -> cell == null ? "" : cell.toString() }.join(" ")
        }.join("\n")
        def matcher = (planText =~ /lanceVersion=(\d+)/)
        assertTrue(matcher.find(), "${label}: EXPLAIN must expose lanceVersion=: ${planText}")
        return matcher.group(1).toLong()
    }

    def asDouble = { cell ->
        return cell instanceof Number ? ((Number) cell).doubleValue() : Double.parseDouble(cell.toString())
    }

    // Distance sequences are compared with a small tolerance: _distance arrives
    // as a floating-point column and only genuinely missed neighbours may change
    // a sequence, never representation noise.
    def sameDistances = { leftRows, rightRows ->
        if (leftRows.size() != rightRows.size()) {
            return false
        }
        for (int i = 0; i < leftRows.size(); i++) {
            if (Math.abs(asDouble(leftRows[i][1]) - asDouble(rightRows[i][1])) > 1e-3) {
                return false
            }
        }
        return true
    }

    // The fixture is collinear (embedding[j] = (row_id - 1) + j), so row r's
    // vector is the deterministic literal this builds.
    def vectorOfRow = { long r ->
        return "[" + ((r - 1)..(r + 14)).collect { it.toString() }.join(",") + "]"
    }

    def flatSearch = { String query, String topK ->
        def rows = sql """SELECT row_id, _distance FROM
                vector_search("table"="${filesystemCatalog}.doris.${tableName}", "column"="embedding",
                "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "use_index"="false")
                ORDER BY _distance, row_id"""
        return rows
    }

    def indexedSearch = { String query, String topK, String nprobes ->
        def rows = sql """SELECT row_id, _distance FROM
                vector_search("table"="${filesystemCatalog}.doris.${tableName}", "column"="embedding",
                "query_vector"="${query}", "top_k"="${topK}", "metric"="l2", "nprobes"="${nprobes}",
                "refine_factor"="10", "use_index"="true")
                ORDER BY _distance, row_id"""
        return rows
    }

    try {
        // Fast dispatch rounds and the admission gate for this suite only; the
        // finally block restores every captured setting no matter where the suite
        // fails. The interval SET takes effect within one bounded sleep slice (the
        // daemon re-reads the config at every wake), so the first dispatch round
        // fires within seconds.
        master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "1")"""
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

        // ---- Case 1: the tracer job doubles as the runner-profile probe --------
        // Snapshot the preinstalled index row and the current dataset version
        // before any mutation: the preinstalled row must survive every step
        // undisturbed, and each committed mutation must advance the version.
        def preSnapshotEntries = entriesRows()
        def preloadedRow = preSnapshotEntries.find { it.IndexName.toString() == preloadedIndex }
        assertTrue(preloadedRow != null,
                "the preinstalled index ${preloadedIndex} must be listed before the tracer run: ${preSnapshotEntries}")
        String preloadedUuid = preloadedRow.IndexUuid.toString()
        long preloadedVersion = preloadedRow.DatasetVersion.toString().toLong()
        long versionBeforeCreate = explainDatasetVersion("pre-tracer snapshot")
        logger.info("pre-tracer snapshot: preloaded index at dataset version ${preloadedVersion}, " +
                "current dataset version ${versionBeforeCreate}")

        // CREATE INDEX is admitted and returns a single-column JobId result set
        // with one row. The job's terminal result decides what this runner can
        // prove (see the header).
        def createRows = sql """CREATE INDEX `${tracerIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
        assertEquals(1, createRows.size())
        assertEquals(1, createRows[0].size())
        tracerJobId = createRows[0][0].toString()

        def tracerRow = waitForSettledJob(tracerJobId, 300000L)
        assertTrue(tracerRow != null, "tracer job ${tracerJobId} never became visible")
        String tracerState = tracerRow.State.toString()
        assertTrue(terminalStates.contains(tracerState),
                "tracer job ${tracerJobId} never converged; stuck in ${tracerState}: ${tracerRow}")
        String tracerResultCode = tracerRow.ResultCode == null ? "" : tracerRow.ResultCode.toString()
        if (tracerState == "NOT_COMMITTED" && tracerResultCode == "PRE_INVOCATION_RESOURCE_REJECTED") {
            logger.info("LOUD SKIP: this runner has no cgroup v2 delegation for the Lance index worker " +
                    "(the tracer job was rejected pre-invocation with PRE_INVOCATION_RESOURCE_REJECTED), " +
                    "so the positive tracer chain cannot be proven here; that runner profile is covered " +
                    "by test_lance_index_worker_negative. Amendment G11 requires the delegated-runner " +
                    "evidence to come from a delegation-capable runner, not from a skip.")
            return
        }
        assertEquals("COMMITTED", tracerState,
                "the tracer job must commit on a delegation-capable runner and reject cleanly on a " +
                "delegation-less one; any other outcome is a broken dispatch path: ${tracerRow}")
        // COMMITTED is the delegation proof itself. Derive the PossibleLive
        // strictness from the observed slot release (see the header): the proof
        // rides with the result report when the kernel can form it.
        def tracerLiveRow = waitForPossibleLiveRelease(tracerJobId, 20000L)
        commitProofStrict = tracerLiveRow != null && tracerLiveRow.PossibleLive.toString() == "NO"
        logger.info("lance worker runner profile: delegated (the tracer job ${tracerJobId} committed); " +
                "commit-proof-strict=${commitProofStrict}")
        if (commitProofStrict) {
            assertEquals("NO", tracerLiveRow.PossibleLive.toString())
        }

        // ---- Case 2: commit chain - refresh, entries, dataset version ---------
        // NATIVE_OK classifies as COMMITTED with refresh REQUIRED; the dispatcher
        // drives REQUIRED -> RUNNING -> DONE through the idempotent external-table
        // refresh, which is what exposes the new index to the catalog read path.
        def refreshRow = waitForRefreshState(tracerJobId, "DONE", 240000L)
        assertTrue(refreshRow != null)
        assertEquals("DONE", refreshRow.RefreshState.toString(),
                "the committed tracer job's refresh never completed: ${refreshRow}")

        // The entries TVF now lists the new index at a version advanced past the
        // pre-snapshot, while the preinstalled row is undisturbed.
        long entriesDeadlineMs = System.currentTimeMillis() + 120000L
        def committedEntries = entriesRows()
        while (committedEntries.find { it.IndexName.toString() == tracerIndexName } == null &&
                System.currentTimeMillis() < entriesDeadlineMs) {
            sleep(1000)
            committedEntries = entriesRows()
        }
        def newEntry = committedEntries.find { it.IndexName.toString() == tracerIndexName }
        assertTrue(newEntry != null,
                "lance_index_entries never listed the committed tracer index: ${committedEntries}")
        long newEntryVersion = newEntry.DatasetVersion.toString().toLong()
        assertTrue(newEntryVersion >= preloadedVersion + 1,
                "the tracer index entry must record a dataset version past the pre-snapshot " +
                "${preloadedVersion}, got ${newEntryVersion}: ${committedEntries}")
        def preloadedAfterCreate = committedEntries.find { it.IndexName.toString() == preloadedIndex }
        assertTrue(preloadedAfterCreate != null,
                "the preinstalled index row vanished after the tracer commit: ${committedEntries}")
        assertEquals(preloadedUuid, preloadedAfterCreate.IndexUuid.toString())
        assertEquals(preloadedVersion.toString(), preloadedAfterCreate.DatasetVersion.toString())

        long versionAfterCreate = explainDatasetVersion("post-commit")
        assertTrue(versionAfterCreate >= versionBeforeCreate + 1,
                "the committed build must advance the dataset version: before=${versionBeforeCreate} " +
                "after=${versionAfterCreate}")

        // ---- Case 3: vector_search really consumes an index -------------------
        // IVF_PQ is quantized, so the assertions are a top1 hit and a recall floor
        // (never exact equality with the flat search), plus the nprobes
        // discriminator that proves the single-partition restriction takes effect.
        sql """SET enable_file_scanner_v2 = true"""

        // The flat baseline is exact and deterministic on this frozen fixture:
        // headQuery is row 1's vector, so the ladder is 16 * (n - 1)^2.
        def flatHead = flatSearch(headQuery, "10")
        assertEquals(10, flatHead.size())
        for (int i = 0; i < 10; i++) {
            assertEquals((i + 1).toString(), flatHead[i][0].toString())
            double expectedDistance = 16.0 * i * i
            assertTrue(Math.abs(asDouble(flatHead[i][1]) - expectedDistance) < 1e-3,
                    "flat ladder broken at rank ${i}: ${flatHead}")
        }

        // Indexed search over all four partitions with exact reranking: the top1
        // must be row 1 (the query is its own vector; its PQ codes minimize the
        // approximate distance, and any tie breaks to the lowest row id), and at
        // least 8 of the flat top-10 rows must survive quantization.
        def indexedHead = indexedSearch(headQuery, "10", "4")
        assertEquals(10, indexedHead.size())
        assertEquals("1", indexedHead[0][0].toString())
        Set flatHeadIds = flatHead.collect { it[0].toString() } as Set
        int recallHits = indexedHead.collect { it[0].toString() }.count { flatHeadIds.contains(it) }
        assertTrue(recallHits >= 8,
                "IVF_PQ recall floor violated: ${recallHits}/10 flat top-10 rows in the indexed " +
                "top-10: indexed=${indexedHead} flat=${flatHead}")

        // The dataset's far end catches a fragment-scoped search that happens to
        // contain only the first rows.
        String tailQuery = vectorOfRow(1024L)
        def indexedTail = indexedSearch(tailQuery, "3", "4")
        assertEquals(3, indexedTail.size())
        assertEquals("1024", indexedTail[0][0].toString())

        // Silent-fallback discriminator: a genuine single-partition probe must
        // miss the neighbours on the far side of an IVF partition edge and differ
        // from the flat search even after exact reranking; a pipeline that ignores
        // use_index/nprobes and silently scans flat fails this. top_k is 9, not
        // 10: distances come in symmetric pairs and 9 is the last cut that lands
        // on a complete pair. The partition edge moves a few rows on every retrain
        // (the tracer index was trained by this run, alongside the preinstalled
        // one), so the discriminator sweeps a window of candidate boundary rows
        // and requires at least one to discriminate.
        boolean nprobesDiscriminated = false
        String nprobesEvidence = "none of the candidate boundary rows discriminated"
        for (long probeRow : [257L, 249L, 265L, 241L, 273L]) {
            String probeQuery = vectorOfRow(probeRow)
            def singleProbe = indexedSearch(probeQuery, "9", "1")
            def flatNine = flatSearch(probeQuery, "9")
            assertEquals(9, singleProbe.size())
            assertEquals(9, flatNine.size())
            if (!sameDistances(singleProbe, flatNine)) {
                nprobesDiscriminated = true
                nprobesEvidence = "row ${probeRow}: nprobes=1 distances " +
                        "${singleProbe.collect { it[1] }} vs flat ${flatNine.collect { it[1] }}"
                break
            }
            logger.info("nprobes discriminator inconclusive at row ${probeRow} (the partition edge " +
                    "moved away from this row on the retrained index); trying the next candidate")
        }
        assertTrue(nprobesDiscriminated,
                "nprobes=1 produced the flat distance sequence at every candidate boundary row, so " +
                "the single-partition restriction had no effect: no IVF_PQ index was consumed " +
                "(silent flat fallback or ignored nprobes). ${nprobesEvidence}")
        logger.info("nprobes discriminator evidence: ${nprobesEvidence}")

        // ---- Case 4: DROP INDEX as a second committed job ---------------------
        // The DROP contract is closed on this stack: the FE persists a
        // recomputable DROP contract and the worker revalidates it against the
        // pinned dataset version. IF EXISTS resolves the committed name in the
        // authoritative snapshot, so a real job is admitted.
        def dropRows = sql """DROP INDEX IF EXISTS `${tracerIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
        assertEquals(1, dropRows.size())
        assertEquals(1, dropRows[0].size())
        dropJobId = dropRows[0][0].toString()
        assertTrue(dropJobId != tracerJobId)

        def dropRow = waitForSettledJob(dropJobId, 300000L)
        assertTrue(dropRow != null, "drop job ${dropJobId} never became visible")
        assertEquals("COMMITTED", dropRow.State.toString(),
                "the tracer DROP job must commit on this proven-delegated runner: ${dropRow}")
        if (commitProofStrict) {
            assertEquals("NO", dropRow.PossibleLive.toString(),
                    "the committed drop must release its worker slot on this runner: ${dropRow}")
        }
        def dropRefreshRow = waitForRefreshState(dropJobId, "DONE", 240000L)
        assertTrue(dropRefreshRow != null)
        assertEquals("DONE", dropRefreshRow.RefreshState.toString(),
                "the committed drop job's refresh never completed: ${dropRefreshRow}")

        long entriesGoneDeadlineMs = System.currentTimeMillis() + 120000L
        def postDropEntries = entriesRows()
        while (postDropEntries.find { it.IndexName.toString() == tracerIndexName } != null &&
                System.currentTimeMillis() < entriesGoneDeadlineMs) {
            sleep(1000)
            postDropEntries = entriesRows()
        }
        assertTrue(postDropEntries.find { it.IndexName.toString() == tracerIndexName } == null,
                "the dropped tracer index is still listed: ${postDropEntries}")
        def preloadedAfterDrop = postDropEntries.find { it.IndexName.toString() == preloadedIndex }
        assertTrue(preloadedAfterDrop != null,
                "the preinstalled index row vanished after the drop: ${postDropEntries}")
        assertEquals(preloadedUuid, preloadedAfterDrop.IndexUuid.toString())
        assertEquals(preloadedVersion.toString(), preloadedAfterDrop.DatasetVersion.toString())

        long versionAfterDrop = explainDatasetVersion("post-drop")
        assertTrue(versionAfterDrop >= versionAfterCreate + 1,
                "the committed drop must advance the dataset version again: after-create=" +
                "${versionAfterCreate} after-drop=${versionAfterDrop}")
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Committed-index hygiene: if the tracer index committed but the scenario
        // drop never ran (or never committed), drop it now so the shared
        // preinstalled fixture keeps the index set the entries/show_index suites
        // pin. The same-name fence releases only once the create job's refresh is
        // DONE, so wait for that first. Everything here is tolerant: a failure of
        // the scenario being cleaned up after must never be masked by cleanup
        // noise.
        try {
            if (tracerJobId != null) {
                def createRow = waitForSettledJob(tracerJobId, 120000L)
                boolean dropCommitted = false
                if (dropJobId != null) {
                    def dropRow = waitForSettledJob(dropJobId, 120000L)
                    dropCommitted = dropRow != null && dropRow.State.toString() == "COMMITTED"
                }
                if (createRow != null && createRow.State.toString() == "COMMITTED" && !dropCommitted) {
                    waitForRefreshState(tracerJobId, "DONE", 120000L)
                    def cleanupDropRows = sql """DROP INDEX IF EXISTS `${tracerIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                    if (!cleanupDropRows.isEmpty()) {
                        def cleanupDropRow = waitForSettledJob(cleanupDropRows[0][0].toString(), 300000L)
                        if (cleanupDropRow == null || cleanupDropRow.State.toString() != "COMMITTED") {
                            logger.info("cleanup DROP INDEX ${tracerIndexName} did not commit: ${cleanupDropRow}")
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.info("cleanup of index ${tracerIndexName} failed: ${t}")
        }
        // Attempt every config restore, but never report success after a failed
        // restore. Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            { master_sql """ADMIN SET FRONTEND CONFIG ("lance_index_job_dispatch_interval_second" = "${originalInterval}")""" }
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
        // A catalog whose jobs are all resolved can be dropped; one holding an
        // unresolved job (a still-running tracer job on a failure path) is guarded
        // against DROP CATALOG and stays behind, exactly like the admission
        // suite's per-run catalog. This drop is deliberately tolerant.
        try_sql """DROP CATALOG IF EXISTS `${filesystemCatalog}`"""
    }
}
