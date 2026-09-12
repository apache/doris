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

import org.apache.doris.regression.suite.ClusterOptions
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_checkpoint_with_dump_and_restart", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = null
    options.enableDebugPoints()
    // Only start a checkpoint after the data and debug points are ready.
    // Keep it disabled on disk so restart cannot overwrite the evidence.
    options.feConfigs += ['enable_checkpoint=false', 'edit_log_roll_num=1',
                          'force_do_metadata_checkpoint=true']

    docker(options) {
        def fe = cluster.getFeByIndex(1)
        def feLog = new File(fe.logFilePath)
        def imageDir = new File(fe.path, "doris-meta/image")
        String writerPause = "MetaWriter checkpoint paused after header"
        String validationPause = "Checkpoint paused before validating image."
        String writerPoint = "MetaWriter.write.checkpoint_pause"
        String validationPoint = "Checkpoint.doCheckpoint.before_validate"

        // Search only the current phase's log, never a previous checkpoint's marker.
        def waitForLog = { int start, String marker ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until {
                feLog.getText("UTF-8").substring(start).contains(marker)
            }
            feLog.getText("UTF-8").substring(start)
        }
        def waitForValidation = { int start ->
            String log = waitForLog(start, validationPause)
            def match = log =~ /Checkpoint paused before validating image\.(\d+)/
            assertTrue(match.find(), "missing checkpoint version in FE log")
            match.group(1)
        }
        def snapshot = {
            // Compare runtime metadata and ordered rows across recovery.
            [sql("SHOW CREATE TABLE checkpoint_dump_restart"),
             sql("SELECT * FROM checkpoint_dump_restart ORDER BY k")]
        }

        sql "DROP TABLE IF EXISTS checkpoint_dump_restart"
        sql """
            CREATE TABLE checkpoint_dump_restart (k INT, v STRING)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO checkpoint_dump_restart VALUES (1, 'before checkpoint'), (2, 'before dump')"
        def beforeCheckpoint = snapshot()

        // docker() tears down this isolated cluster even when recovery fails.
        int start = feLog.getText("UTF-8").length()
        fe.enableDebugPoint(writerPoint, [timeout: "300"])
        fe.enableDebugPoint(validationPoint, [timeout: "300"])
        setFeConfig("enable_checkpoint", true)
        waitForLog(start, writerPause)

        // The checkpoint already installed its writer delegate. Finish /dump before
        // resuming it: the old static writer now sends checkpoint indices to the dump.
        httpTest {
            basicAuthorization "${context.config.jdbcUser}", "${context.config.jdbcPassword}"
            endpoint "${fe.host}:${fe.httpPort}"
            uri "/dump"
            op "get"
            check { code, body ->
                assertEquals(200, code)
                assertEquals("success", parseJson(body).msg)
            }
        }
        fe.disableDebugPoint(writerPoint)
        String version = waitForValidation(start)
        setFeConfig("enable_checkpoint", false)
        fe.disableDebugPoint(validationPoint)
        waitForLog(start, "checkpoint finished save image.${version}")
        assertTrue(new File(imageDir, "image.${version}").isFile())

        // Recovery must read the concurrently written checkpoint, not just replay
        // journals against an older image after a silently failed checkpoint.
        cluster.restartFrontends()
        context.reconnectFe()
        assertEquals(beforeCheckpoint, snapshot())

        sql "INSERT INTO checkpoint_dump_restart VALUES (3, 'recovered from journal')"
        def beforeInterruptedCheckpoint = snapshot()
        start = feLog.getText("UTF-8").length()
        fe.enableDebugPoint(validationPoint, [timeout: "300"])
        setFeConfig("enable_checkpoint", true)
        String pendingVersion = waitForValidation(start)
        assertTrue(pendingVersion.toLong() > version.toLong())
        assertFalse(new File(imageDir, "image.${pendingVersion}").exists(),
                "checkpoint must not be published before read-back validation")
        def pendingImage = new File(imageDir, "image.ckpt")
        assertTrue(pendingImage.isFile())

        // Leave an unreadable temporary image and stop while validation is paused.
        // Startup must ignore it and recover from the previous image plus journals.
        new RandomAccessFile(pendingImage, "rw").withCloseable { it.setLength(0) }
        cluster.restartFrontends()
        context.reconnectFe()
        assertEquals(beforeInterruptedCheckpoint, snapshot())
        assertTrue(new File(imageDir, "image.${version}").isFile())
        assertFalse(new File(imageDir, "image.${pendingVersion}").exists())
        assertTrue(pendingImage.isFile(), "restart must leave the ignored temporary image intact")
        assertEquals(0L, pendingImage.length())
    }
}
