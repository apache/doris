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
    // The authoritative admission preflight runs against real Lance metadata; with the
    // mutation gate open an admitted mutation dispatches synchronously to the backend's
    // isolated lance index worker inside the statement. What that dispatch produces now
    // depends on the runner profile: a runner with a delegated writable cgroup v2 parent
    // launches the real worker, so a committed mutation ends in the default OK packet,
    // while a delegation-less runner refuses every launch before any worker exists, so
    // the same statement ends as the 5108 confirmed-failure class with the never-launched
    // envelope. This suite derives the profile once with a two-leg probe (below) and
    // forks its positive-mutation assertions on the result; every case asserts a concrete
    // terminal shape on both profiles - nothing skips to green on either runner.
    // Index names still carry this per-run suffix so rerunning the suite on a shared
    // pipeline cluster can never collide with leftovers from other suites.
    String runSuffix = "${System.currentTimeMillis()}"
    String filesystemCatalog = "test_lance_index_admission_${runSuffix}"
    String restCatalog = "test_lance_index_admission_rest"
    String tableName = "vs_ivf_pq_f32"
    // vs_ivf_pq_f32 ships with one preloaded IVF_PQ index on the embedding column; its
    // authoritative logical metadata (metric_type=L2, compression.num_sub_vectors=4,
    // compression.num_bits=4) is the anchor for the CREATE IF NOT EXISTS mismatch cases
    // below. The suite's own admitted CREATEs must stay genuinely buildable when the real
    // worker executes them: the 1024-row fixture supports num_partitions <= 4 (the Lance
    // sample rate trains 256 points per partition) and num_sub_vectors must divide the
    // 16-dimensional embedding, while static validation pins num_bits to 8 - the same
    // buildable shape the worker-side suites proved on this fixture.
    String preloadedIndex = "embedding_ivf_pq_f32"
    String createIndexName = "idx_create_${runSuffix}"
    String absentIndexName = "idx_absent_${runSuffix}"
    // The BE-side deterministic rejection point: armed through the backend HTTP
    // debug-point interface, it fires inside the backend process after the dequeue
    // gates and before any cgroup/fork work, so it is reachable on every runner
    // profile without ever needing a worker (sync2 design D11).
    String rejectPoint = "LanceIndexSupervisor.reject_after_enqueue"

    sql """DROP CATALOG IF EXISTS `${restCatalog}`"""

    // The gate is masterOnly. Read it on the master even if the suite's ordinary JDBC
    // connection points at a follower. SHOW uses the experimental display name for the
    // gate, while ADMIN SET accepts its unprefixed alias.
    def gateRows = master_sql """ADMIN SHOW FRONTEND CONFIG LIKE 'experimental_enable_lance_index_mutation'"""
    assertEquals(1, gateRows.size())
    String originalGate = gateRows[0][1].toString()
    Throwable suiteFailure = null
    // Derived by the profile probe below: true means this runner launches the real
    // isolated worker, false means every launch is refused before the worker exists.
    boolean delegated = false
    // The armed probe leg's rejection text, kept for the retrospective check after the
    // profile is derived (see below).
    String armedLegMessage = null

    // The dispatch-level never-launched shape of the 5108 confirmed-failure class: the
    // typed MySQL error code of the client exception (5108 = the confirmed-failure
    // class; the message text itself only carries Doris's generic internal prefix) plus
    // the FE classification's backend-refusal branch inside the detail message. The code
    // alone would also accept other 5108 branches (a real worker's pre-invocation
    // rejection), so the branch marker pins that the refusal happened on the backend side
    // before any worker ran. The optional cause needle (a backend static category text)
    // sharpens cases whose expected cause is known; a shape outside this envelope fails
    // loudly instead of being treated as any profile.
    def assertNeverLaunchedRejection = { Throwable failure, String causeNeedle, String label ->
        String message = failure == null ? "" : failure.toString()
        assertTrue(failure != null,
                "${label}: the statement must be rejected, but it completed: ${message}")
        assertTrue(failure instanceof java.sql.SQLException
                        && failure.getErrorCode() == 5108,
                "${label}: a never-launched dispatch must surface the 5108 confirmed-failure"
                        + " class as the client exception's typed error code: ${message}")
        assertTrue(message.contains("the backend refused the invocation before executing it"),
                "${label}: a never-launched dispatch must carry the backend-refusal branch"
                        + " of the typed error: ${message}")
        if (causeNeedle != null) {
            assertTrue(message.contains(causeNeedle),
                    "${label}: the rejection must carry the expected backend cause"
                            + " category: ${message}")
        }
    }

    // The physical index entries of the fixture table through the per-run catalog.
    def entriesIndexNames = {
        def rows = sql """SELECT IndexName FROM lance_index_entries("table" = "${filesystemCatalog}.doris.${tableName}")"""
        return rows.collect { row -> row[0].toString() }
    }

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

        // ---- Runner-profile probe, leg 1: the armed deterministic rejection -------
        // CREATE INDEX passes static validation and the whole authoritative preflight
        // (snapshot read, case analysis, column resolution, schema contract), then
        // executes synchronously with the rejection point armed. On a delegated runner
        // the point fires after dequeue and refuses the invocation before any fork work;
        // on a delegation-less runner the isolation gate refuses the same dispatch even
        // earlier. Either way the statement must end as the 5108 confirmed-failure class
        // with the never-launched envelope, which proves the whole
        // FE -> dispatch -> backend -> classification chain (and the HTTP debug-point
        // arming channel) is live on this runner before any profile is derived. No index
        // is created: the proven never-launched rejection commits nothing, so the
        // same-name CREATE of leg 2 admits again.
        GetDebugPoint().enableDebugPointForAllBEs(rejectPoint)
        try {
            Throwable armedFailure = null
            try {
                sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                        PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            } catch (Throwable failure) {
                armedFailure = failure
            }
            assertNeverLaunchedRejection(armedFailure, null,
                    "the armed probe CREATE must be refused before any worker launch")
            armedLegMessage = armedFailure.toString()
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(rejectPoint)
        }

        // ---- Runner-profile probe, leg 2: the unarmed terminal state --------------
        // The identical CREATE with no fault armed derives this runner's profile from
        // the real terminal state. The default OK packet (a single zero update-count
        // row) means an isolated worker really launched and committed - the delegated
        // profile. The 5108 never-launched envelope carrying the isolation-unavailable
        // category means no worker can ever launch on this runner - the delegation-less
        // profile. Any third shape (for example the 5104 indeterminate class or a real
        // worker pre-invocation rejection) fails the assertions loudly instead of
        // silently degrading to either profile: a false profile verdict would turn every
        // forked assertion below into a false green, which is worse than a red suite.
        Throwable probeFailure = null
        try {
            def committedRows = sql """CREATE INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}` (embedding) USING ANN
                    PROPERTIES("index_type"="IVF_PQ", "metric"="l2", "num_partitions"="4", "num_sub_vectors"="4", "num_bits"="8")"""
            assertEquals(1, committedRows.size())
            assertEquals(0, committedRows[0][0])
            delegated = true
            logger.info("lance index runner profile: delegated - the real isolated worker"
                    + " executed and committed the probe CREATE")
        } catch (Throwable failure) {
            probeFailure = failure
        }
        if (probeFailure != null) {
            assertNeverLaunchedRejection(probeFailure, "lance index worker isolation is not verified",
                    "the unarmed probe CREATE must either commit or be refused for isolation")
            delegated = false
            logger.info("lance index runner profile: delegation-less - no isolated worker can"
                    + " launch on this runner, every dispatched mutation is refused before it")
        }

        // Retrospective leg-1 check, now that the profile is known: on a delegated runner
        // the armed refusal must have been the point's own category (the arming channel
        // demonstrably works - the precondition the delegated armed-DROP case below relies
        // on for the fixture's safety), while on a delegation-less runner it must have
        // been the isolation gate refusing even earlier. A mismatch fails loudly: a
        // silently broken arming channel would turn every later armed case into an
        // untested assumption.
        assertTrue(delegated ? armedLegMessage.contains("debug point rejection")
                : armedLegMessage.contains("lance index worker isolation is not verified"),
                "the armed probe leg's refusal cause does not match the derived runner"
                        + " profile - the debug-point arming channel is broken on this runner:"
                        + " ${armedLegMessage}")

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

        // ---- The admitted DROP, forked on the derived runner profile --------------
        if (!delegated) {
            // Delegation-less runner: DROP INDEX IF EXISTS of the preloaded index resolves
            // to a name that is present in the authoritative snapshot, so the preflight
            // passes and the statement executes synchronously - and the dispatch is
            // refused before any worker exists. The refusal is asserted, not skipped:
            // this is the delegation-less profile's real coverage of the dispatched
            // mutation path, and the closing cross-check below proves nothing dropped.
            Throwable dropFailure = null
            try {
                sql """DROP INDEX IF EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
            } catch (Throwable failure) {
                dropFailure = failure
            }
            assertNeverLaunchedRejection(dropFailure, "lance index worker isolation is not verified",
                    "the admitted DROP on a delegation-less runner must be refused for isolation")
        } else {
            // Delegated runner: the probe CREATE really committed, so this branch first
            // closes the loop on the suite's own index through both read faces - the
            // authoritative physical listing and SHOW INDEX must resolve it, the plain
            // DROP must complete with the default OK packet, and both faces must then
            // stop resolving it. The preloaded index itself is never really dropped: it
            // is the shared preinstalled fixture that the entries and show_index suites
            // pin, so the dispatched-DROP coverage on this profile uses the armed
            // deterministic rejection instead (leg 1 already proved the arming channel
            // is live on this runner, so the armed DROP below cannot fall through to a
            // real drop of the fixture).
            assertTrue(entriesIndexNames().any { it.equalsIgnoreCase(createIndexName) },
                    "the committed probe index must be visible in the authoritative listing")
            def showAfterCreate = sql """SHOW INDEX FROM `${filesystemCatalog}`.`doris`.`${tableName}`"""
            assertTrue(showAfterCreate.any { row -> row[2].toString().equalsIgnoreCase(createIndexName) },
                    "the committed probe index must be visible through SHOW INDEX")

            def dropRows = sql """DROP INDEX `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
            assertEquals(1, dropRows.size())
            assertEquals(0, dropRows[0][0])
            assertTrue(!entriesIndexNames().any { it.equalsIgnoreCase(createIndexName) },
                    "the dropped probe index must be gone from the authoritative listing")
            def showAfterDrop = sql """SHOW INDEX FROM `${filesystemCatalog}`.`doris`.`${tableName}`"""
            assertTrue(!showAfterDrop.any { row -> row[2].toString().equalsIgnoreCase(createIndexName) },
                    "the dropped probe index must be gone from SHOW INDEX")

            // The deterministic dispatched-DROP rejection on this profile: with the point
            // armed, the admitted DROP IF EXISTS of the preloaded index must be refused
            // after dequeue with the point's own never-launched category (proving the
            // point fired rather than some other refusal), and the cross-check below
            // proves the preloaded index survived the refusal.
            GetDebugPoint().enableDebugPointForAllBEs(rejectPoint)
            try {
                Throwable armedDropFailure = null
                try {
                    sql """DROP INDEX IF EXISTS `${preloadedIndex}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                } catch (Throwable failure) {
                    armedDropFailure = failure
                }
                assertNeverLaunchedRejection(armedDropFailure, "debug point rejection",
                        "the armed admitted DROP on a delegated runner must be refused by the point")
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(rejectPoint)
            }
        }

        // The read face is unaffected on both profiles: the authoritative physical
        // listing still shows exactly the preloaded index (the delegation-less refusals
        // dropped nothing; the delegated armed refusal dropped nothing; the delegated
        // closed loop removed only the suite's own index), and SHOW INDEX still resolves
        // the preloaded name through the same per-run catalog (column 3 is the index
        // name).
        def entriesNames = entriesIndexNames()
        assertTrue(entriesNames.any { it.equalsIgnoreCase(preloadedIndex) })
        assertTrue(!entriesNames.any { it.equalsIgnoreCase(createIndexName) })
        def showRows = sql """SHOW INDEX FROM `${filesystemCatalog}`.`doris`.`${tableName}`"""
        assertTrue(showRows.any { row -> row[2].toString().equalsIgnoreCase(preloadedIndex) })
    } catch (Throwable failure) {
        suiteFailure = failure
        throw failure
    } finally {
        // Attempt every cleanup, but never report success after a failed restore.
        // Preserve the scenario failure and attach cleanup failures to it.
        Throwable cleanupFailure = suiteFailure
        [
            { master_sql """ADMIN SET FRONTEND CONFIG ("enable_lance_index_mutation" = "${originalGate}")""" },
            // On the delegated profile the probe CREATE may have committed before a later
            // step failed; the best-effort removal keeps the shared preinstalled dataset
            // clean for the suites that pin it (a no-op when the closed loop already
            // dropped the index, skipped when nothing can have committed).
            {
                if (delegated) {
                    sql """DROP INDEX IF EXISTS `${createIndexName}` ON `${filesystemCatalog}`.`doris`.`${tableName}`"""
                }
            },
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
