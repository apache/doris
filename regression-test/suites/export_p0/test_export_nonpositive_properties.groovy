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

suite("test_export_nonpositive_properties", "p0") {
    if (!getFeConfig("enable_outfile_to_local").equalsIgnoreCase("true")) {
        logger.warn("Enable enable_outfile_to_local to run test_export_nonpositive_properties")
        return
    }

    sql """DROP TABLE IF EXISTS test_export_nonpositive_properties"""
    sql """
        CREATE TABLE test_export_nonpositive_properties (
            id INT,
            epoch INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO test_export_nonpositive_properties VALUES (1, 100), (2, 100), (3, 100)"""
    assertEquals("3", sql("""SELECT COUNT(*) FROM test_export_nonpositive_properties""")[0][0].toString())

    String runId = UUID.randomUUID().toString()
    String positiveLabel = "positive_${runId}"
    String positivePath = "/tmp/test_export_nonpositive_properties/${runId}/positive"
    mkdirRemotePathOnAllBE("root", positivePath)

    sql """
        EXPORT TABLE test_export_nonpositive_properties
        TO "file://${positivePath}/"
        PROPERTIES(
            "label" = "${positiveLabel}",
            "parallelism" = "1",
            "timeout" = "60"
        )
    """

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

    def positiveJob = waitForFinalJob(positiveLabel)
    assertNotNull(positiveJob)
    assertEquals("FINISHED", positiveJob[2])
    def positiveOutfileInfo = parseJson(positiveJob[11])
    assertTrue(positiveOutfileInfo instanceof List)
    assertFalse(positiveOutfileInfo.isEmpty())
    assertFalse(positiveOutfileInfo[0].isEmpty())

    def contractViolations = []
    def verifyRejected = { variant, propertyName, propertyValue, expectedMessage ->
        String label = "${variant}_${runId}"
        try {
            sql """
            EXPORT TABLE test_export_nonpositive_properties
            TO "file:///tmp/test_export_nonpositive_properties/${runId}/${variant}/"
            PROPERTIES(
                "label" = "${label}",
                "${propertyName}" = "${propertyValue}"
            )
            """
            contractViolations.add("${variant}: submission succeeded")
        } catch (Exception e) {
            if (!e.getMessage()?.contains(expectedMessage)) {
                contractViolations.add("${variant}: unexpected submission error: ${e.getMessage()}")
            }
        }

        def jobs = sql """SHOW EXPORT WHERE LABEL = "${label}" """
        if (!jobs.isEmpty()) {
            def finalJob = waitForFinalJob(label)
            if (finalJob == null) {
                contractViolations.add("${variant}: created job did not reach a final state")
            } else {
                contractViolations.add("${variant}: created job state=${finalJob[2]}, "
                        + "error=${finalJob[10]}, outfile=${finalJob[11]}")
            }
        }
    }

    verifyRejected("parallelism_negative", "parallelism", "-1", "The value of parallelism is invalid")
    verifyRejected("parallelism_zero", "parallelism", "0", "The value of parallelism is invalid")
    verifyRejected("timeout_zero", "timeout", "0", "The value of timeout is invalid")
    verifyRejected("timeout_negative", "timeout", "-1", "The value of timeout is invalid")

    assertTrue(contractViolations.isEmpty(), contractViolations.join("\n"))
}
