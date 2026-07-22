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

suite("test_export_preflight_before_delete", "p0") {
    if (!getFeConfig("enable_delete_existing_files").equalsIgnoreCase("true")) {
        logger.warn("Enable enable_delete_existing_files to run test_export_preflight_before_delete")
        return
    }

    String bucket = getS3BucketName()
    assertNotNull(bucket)
    assertFalse(bucket.isEmpty())

    String runId = UUID.randomUUID().toString()
    String rootPrefix = "find-bugs/export/tc-f-006/${runId}/"
    String targetPrefix = "${rootPrefix}target/"
    String adjacentPrefix = "${rootPrefix}target-adjacent/"
    String ownershipProbeKey = "${rootPrefix}OWNERSHIP_PROBE"
    String sentinelKey = "${targetPrefix}sentinel.keep"
    String oldResultKey = "${targetPrefix}old-success.csv"
    String adjacentKey = "${adjacentPrefix}neighbor.keep"

    def snapshotOwnedPrefix = {
        return getS3Client().listObjects(bucket, rootPrefix).getObjectSummaries()
                .sort { left, right -> left.getKey() <=> right.getKey() }
                .collectEntries { summary ->
                    [(summary.getKey()): "${summary.getETag()}:${summary.getSize()}"]
                }
    }

    def cleanupOwnedPrefix = {
        getS3Client().listObjects(bucket, rootPrefix).getObjectSummaries().each { summary ->
            assertTrue(summary.getKey().startsWith(rootPrefix))
            getS3Client().deleteObject(bucket, summary.getKey())
        }
        assertTrue(snapshotOwnedPrefix().isEmpty())
    }

    // Safety gate: a UUID root must be empty before first use, and exact-key cleanup
    // must leave it empty. No destructive EXPORT is allowed before this gate passes.
    assertTrue(snapshotOwnedPrefix().isEmpty())
    getS3Client().putObject(bucket, ownershipProbeKey, "owned-by-tc-f-006-${runId}")
    def probeSnapshot = snapshotOwnedPrefix()
    assertEquals([ownershipProbeKey], probeSnapshot.keySet().toList())
    getS3Client().deleteObject(bucket, ownershipProbeKey)
    assertTrue(snapshotOwnedPrefix().isEmpty())

    sql """DROP TABLE IF EXISTS test_export_preflight_before_delete"""
    sql """
        CREATE TABLE test_export_preflight_before_delete (
            id INT,
            epoch INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO test_export_preflight_before_delete VALUES (1, 100), (2, 100), (3, 100)"""

    def restoreFixture = {
        cleanupOwnedPrefix()
        getS3Client().putObject(bucket, sentinelKey, "sentinel-${runId}")
        getS3Client().putObject(bucket, oldResultKey, "old-result-${runId}")
        getS3Client().putObject(bucket, adjacentKey, "adjacent-${runId}")
        def fixture = snapshotOwnedPrefix()
        assertEquals([adjacentKey, oldResultKey, sentinelKey].sort(), fixture.keySet().toList().sort())
        return fixture
    }

    def waitForFinalJob = { label ->
        def finalJob = null
        for (int i = 0; i < 60; i++) {
            def jobs = sql """SHOW EXPORT WHERE LABEL = "${label}" """
            if (jobs.size() == 1 && (jobs[0][2] == "FINISHED" || jobs[0][2] == "CANCELLED")) {
                finalJob = jobs[0]
                break
            }
            sleep(1000)
        }
        return finalJob
    }

    String s3Endpoint = getS3Endpoint()
    String s3Region = getS3Region()
    String s3Provider = getS3Provider()
    String s3Ak = getS3AK()
    String s3Sk = getS3SK()
    def contractViolations = []

    def verifyPreflightBeforeDelete = { variant, Map<String, String> invalidProperties,
            String expectedMessage ->
        def before = restoreFixture()
        String label = "${variant}_${runId}"
        String propertySql = invalidProperties.collect { key, value ->
            return "\"${key}\" = \"${value}\""
        }.join(",\n")

        try {
            sql """
                EXPORT TABLE test_export_preflight_before_delete
                TO "s3://${bucket}/${targetPrefix}"
                PROPERTIES(
                    "label" = "${label}",
                    "delete_existing_files" = "true",
                    ${propertySql}
                )
                WITH S3(
                    "s3.endpoint" = "${s3Endpoint}",
                    "s3.region" = "${s3Region}",
                    "s3.secret_key" = "${s3Sk}",
                    "s3.access_key" = "${s3Ak}",
                    "provider" = "${s3Provider}"
                )
            """
            contractViolations.add("${variant}: submission succeeded")
        } catch (Exception e) {
            if (!e.getMessage()?.toLowerCase()?.contains(expectedMessage.toLowerCase())) {
                contractViolations.add("${variant}: unexpected submission error: ${e.getMessage()}")
            }
        }

        def immediatelyAfterSubmit = snapshotOwnedPrefix()
        if (immediatelyAfterSubmit != before) {
            contractViolations.add("${variant}: fixture changed before task completion; "
                    + "before=${before.keySet()}, after=${immediatelyAfterSubmit.keySet()}")
        }

        def jobs = sql """SHOW EXPORT WHERE LABEL = "${label}" """
        if (!jobs.isEmpty()) {
            def finalJob = waitForFinalJob(label)
            if (finalJob == null) {
                contractViolations.add("${variant}: created job did not reach a final state")
            } else {
                contractViolations.add("${variant}: created job state=${finalJob[2]}, error=${finalJob[10]}")
            }
        }

        def afterFinalState = snapshotOwnedPrefix()
        if (afterFinalState != before) {
            contractViolations.add("${variant}: fixture changed at final state; "
                    + "before=${before.keySet()}, after=${afterFinalState.keySet()}")
        }
    }

    try {
        verifyPreflightBeforeDelete("invalid_format", ["format": "invalid_format"], "format")
        verifyPreflightBeforeDelete("missing_column", ["columns": "id,missing_column"], "unknown column")
        verifyPreflightBeforeDelete("small_max_file_size", ["max_file_size": "1MB"], "max file size")
        verifyPreflightBeforeDelete("invalid_compression",
                ["format": "csv", "compress_type": "invalid"], "unknown compression type")
        verifyPreflightBeforeDelete("unknown_property", ["unknown_export_property": "true"],
                "invalid property key")

        assertTrue(contractViolations.isEmpty(), contractViolations.join("\n"))
    } finally {
        cleanupOwnedPrefix()
        try_sql("DROP TABLE IF EXISTS test_export_preflight_before_delete")
    }
}
