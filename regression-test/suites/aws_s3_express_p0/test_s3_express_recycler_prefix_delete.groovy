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

import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions

suite("test_s3_express_recycler_prefix_delete", "p0,external,docker") {
    String enabled = context.config.otherConfigs.get("enableS3ExpressStorageVaultTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return
    }

    def requireConfig = { String key ->
        String value = context.config.otherConfigs.get(key)
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(
                    "${key} must be configured when enableS3ExpressStorageVaultTest is true")
        }
        return value
    }

    String bucket = requireConfig("s3ExpressBucketName")
    String region = requireConfig("s3ExpressRegion")
    String dataPrefix = "fixtures/v1"
    String prefix = "vault-runs/regression"
    String accessKey = requireConfig("s3ExpressAk")
    String secretKey = requireConfig("s3ExpressSk")

    if (!(bucket ==~ /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?--[a-z0-9-]+-az[0-9]+--x-s3$/)) {
        throw new IllegalArgumentException(
                "s3ExpressBucketName must be a complete S3 Express directory bucket name")
    }
    if (!(region ==~ /^[a-z0-9-]+$/)) {
        throw new IllegalArgumentException("s3ExpressRegion contains unsupported characters")
    }
    String suffix = UUID.randomUUID().toString().replace("-", "")
    String instanceId = "s3_express_recycler_${suffix}"
    String vaultName = "s3_express_recycler_${suffix}"
    String rootPath = "${prefix}/${vaultName}"

    def options = new ClusterOptions()
    options.cloudMode = true
    options.enableStorageVault = true
    options.instanceId = instanceId
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.recyclerNum = 1
    options.enableDebugPoints()
    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'heartbeat_interval_second=1'
    ]
    options.beConfigs += [
            'disable_auto_compaction=true',
            'enable_packed_file=false'
    ]
    options.recycleConfigs += [
            'recycler_sleep_before_scheduling_seconds=0',
            'recycle_interval_seconds=1',
            'retention_seconds=0'
    ]
    options.cloudStoreConfigs += [
            'DORIS_CLOUD_USER=s3express-regression',
            "DORIS_CLOUD_AK=${accessKey}",
            "DORIS_CLOUD_SK=${secretKey}",
            "DORIS_CLOUD_BUCKET=${bucket}",
            "DORIS_CLOUD_ENDPOINT=s3.${region}.amazonaws.com",
            "DORIS_CLOUD_EXTERNAL_ENDPOINT=s3.${region}.amazonaws.com",
            "DORIS_CLOUD_REGION=${region}",
            'DORIS_CLOUD_PROVIDER=S3EXPRESS'
    ]

    def runAws = { List<String> arguments ->
        List<String> command = ["aws", "--no-cli-pager"]
        command.addAll(arguments.collect { it.toString() })
        String lastOutput = ""
        int lastExitCode = -1
        for (int attempt = 1; attempt <= 4; attempt++) {
            ProcessBuilder builder = new ProcessBuilder(command)
            builder.redirectErrorStream(true)
            builder.environment().put("AWS_ACCESS_KEY_ID", accessKey)
            builder.environment().put("AWS_SECRET_ACCESS_KEY", secretKey)
            builder.environment().put("AWS_REGION", region)
            builder.environment().put("AWS_DEFAULT_REGION", region)
            builder.environment().put("AWS_EC2_METADATA_DISABLED", "true")
            Process process = builder.start()
            lastOutput = process.inputStream.getText("UTF-8")
            lastExitCode = process.waitFor()
            if (lastExitCode == 0) {
                return lastOutput
            }
            sleep(attempt * 1000)
        }
        throw new IllegalStateException(
                "AWS CLI failed with exit code ${lastExitCode} after 4 attempts: ${lastOutput}")
    }

    def listKeys = { String keyPrefix ->
        String output = runAws([
                "s3api", "list-objects-v2",
                "--bucket", bucket,
                "--region", region,
                "--prefix", keyPrefix,
                "--query", "Contents[].Key",
                "--output", "json"
        ]).trim()
        if (output.isEmpty() || output == "null") {
            return new HashSet<String>()
        }
        return new HashSet<String>((parseJson(output) as List).collect { it.toString() })
    }

    def listPage = { String keyPrefix, String continuationToken ->
        List<String> arguments = [
                "--no-paginate",
                "s3api", "list-objects-v2",
                "--bucket", bucket,
                "--region", region,
                "--prefix", keyPrefix,
                "--max-keys", "1000",
                "--output", "json"
        ]
        if (continuationToken != null && !continuationToken.isEmpty()) {
            arguments.addAll(["--continuation-token", continuationToken])
        }
        def result = parseJson(runAws(arguments))
        Set<String> keys = new HashSet<>()
        if (result.Contents instanceof List) {
            keys.addAll(result.Contents.collect { it.Key.toString() })
        }
        return [
                keys: keys,
                truncated: Boolean.TRUE.equals(result.IsTruncated),
                nextToken: result.NextContinuationToken?.toString()
        ]
    }

    def copyNonTargetFixtures = { String tabletPrefix ->
        runAws([
                "s3", "cp",
                "s3://${bucket}/${dataPrefix}/pagination/",
                "s3://${bucket}/${tabletPrefix}",
                "--region", region,
                "--recursive",
                "--exclude", "*",
                "--include", "part-0???.csv",
                "--exclude", "part-0998.csv",
                "--exclude", "part-0999.csv",
                "--only-show-errors"
        ])
    }

    def triggerRecycler = {
        def recycler = cluster.getAllRecyclers(true).first()
        String request = JsonOutput.toJson([instance_ids: [instanceId]])
        httpTest {
            endpoint "${recycler.host}:${recycler.httpPort}"
            uri "/RecyclerService/http/recycle_instance?token=greedisgood9999"
            body request
            check { respCode, body ->
                assertEquals(200, respCode)
                assertEquals("OK", body.trim())
            }
        }
    }

    boolean recyclerStopped = false
    docker(options) {
        try {
            sql """DROP TABLE IF EXISTS test_s3_express_recycler_prefix_delete FORCE"""
            sql """
                CREATE STORAGE VAULT ${vaultName}
                PROPERTIES (
                    "type" = "S3",
                    "provider" = "S3EXPRESS",
                    "s3.region" = "${region}",
                    "s3.bucket" = "${bucket}",
                    "s3.root.path" = "${rootPath}",
                    "s3.access_key" = "${accessKey}",
                    "s3.secret_key" = "${secretKey}",
                    "use_path_style" = "false",
                    "s3_validity_check" = "true"
                )
            """
            sql """
                CREATE TABLE test_s3_express_recycler_prefix_delete (
                    k INT NOT NULL,
                    v VARCHAR(32),
                    score INT
                )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "true",
                    "replication_num" = "1",
                    "storage_vault_name" = "${vaultName}"
                )
            """
            sql """INSERT INTO test_s3_express_recycler_prefix_delete VALUES (1, 'baseline', 100)"""
            sql """SYNC"""

            def tablets = sql_return_maparray("SHOW TABLETS FROM test_s3_express_recycler_prefix_delete")
            assertEquals(1, tablets.size())
            String tabletId = tablets.first().TabletId
            String tabletPrefix = "${rootPath}/data/${tabletId}/"

            Set<String> baselineKeys = new HashSet<>()
            for (int retry = 0; retry < 30; retry++) {
                baselineKeys = listKeys(tabletPrefix)
                if (!baselineKeys.isEmpty()) {
                    break
                }
                sleep(1000)
            }
            assertFalse(baselineKeys.isEmpty(), "The baseline rowset did not create S3 objects")

            cluster.stopRecyclers()
            recyclerStopped = true

            GetDebugPoint().enableDebugPointForAllFEs("FE.mow.commit.exception")
            try {
                streamLoad {
                    table "test_s3_express_recycler_prefix_delete"
                    set "column_separator", ","
                    set "columns", "k,v"
                    set "partial_columns", "true"
                    set "timeout", "5"
                    inputStream new ByteArrayInputStream("1,orphan\n".getBytes("UTF-8"))
                    time 30000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        def json = parseJson(result)
                        assertEquals("fail", json.Status.toLowerCase())
                        assertTrue(json.Message.contains("FE.mow.commit.exception"))
                    }
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs("FE.mow.commit.exception")
            }

            Set<String> keysAfterFailedLoad = new HashSet<>()
            Set<String> orphanKeys = new HashSet<>()
            for (int retry = 0; retry < 30; retry++) {
                keysAfterFailedLoad = listKeys(tabletPrefix)
                orphanKeys = new HashSet<>(keysAfterFailedLoad)
                orphanKeys.removeAll(baselineKeys)
                if (!orphanKeys.isEmpty()) {
                    break
                }
                sleep(1000)
            }
            assertFalse(orphanKeys.isEmpty(), "The failed load did not leave an S3 rowset to recycle")

            Set<String> rowsetIds = new HashSet<>()
            orphanKeys.each { String key ->
                String fileName = key.substring(tabletPrefix.length())
                def matcher = fileName =~ /^([A-Za-z0-9]+)_\d+\.(?:dat|idx)$/
                if (matcher.matches()) {
                    rowsetIds.add(matcher.group(1))
                }
            }
            assertEquals(1, rowsetIds.size(), "Expected one failed rowset, keys=${orphanKeys}")
            String failedRowsetId = rowsetIds.first()
            String failedRowsetPrefix = "${tabletPrefix}${failedRowsetId}_"
            Set<String> failedRowsetKeys = orphanKeys.findAll {
                it.startsWith(failedRowsetPrefix)
            } as Set<String>
            assertFalse(failedRowsetKeys.isEmpty())

            Set<String> nonTargetFixtureKeys = new HashSet<>()
            assertEquals(1, baselineKeys.size(),
                    "The test expects one baseline object so total parent objects equal 1001")
            // The baseline object plus these 998 fixed objects are the 999 siblings that must
            // survive Recycler prefix deletion.
            for (int index = 0; index < 998; index++) {
                String fileName = String.format("part-%04d.csv", index)
                nonTargetFixtureKeys.add("${tabletPrefix}${fileName}".toString())
            }
            copyNonTargetFixtures(tabletPrefix)

            assertEquals(1, failedRowsetKeys.size())
            String manualTargetKey = "${failedRowsetPrefix}1.dat"
            runAws([
                    "s3api", "copy-object",
                    "--bucket", bucket,
                    "--region", region,
                    "--copy-source", "${bucket}/${dataPrefix}/pagination/part-1000.csv",
                    "--key", manualTargetKey,
                    "--output", "json"
            ])
            failedRowsetKeys.add(manualTargetKey)

            Map firstPage = listPage(tabletPrefix, null)
            assertTrue(firstPage.truncated, "The first page must have a continuation token")
            assertNotNull(firstPage.nextToken)
            Map secondPage = listPage(tabletPrefix, firstPage.nextToken)
            assertFalse(secondPage.truncated, "The fixture must contain exactly two pages")

            Set<String> firstPageTargets = firstPage.keys.intersect(failedRowsetKeys) as Set<String>
            Set<String> secondPageTargets = secondPage.keys.intersect(failedRowsetKeys) as Set<String>
            assertEquals(1000, firstPage.keys.size())
            assertEquals(1, secondPage.keys.size())

            // Directory buckets do not guarantee object ordering. Target placement within the two
            // pages is irrelevant: Recycler must consume the continuation token and filter the
            // complete parent directory before deleting the two matching keys.
            Set<String> keysBeforeRecycle = new HashSet<>(firstPage.keys)
            keysBeforeRecycle.addAll(secondPage.keys)
            assertEquals(1001, keysBeforeRecycle.size())
            assertEquals(keysBeforeRecycle, listKeys(tabletPrefix))
            assertTrue(keysBeforeRecycle.containsAll(failedRowsetKeys))
            assertEquals(2, firstPageTargets.size() + secondPageTargets.size())

            Set<String> nonTargetKeys = new HashSet<>(keysBeforeRecycle)
            nonTargetKeys.removeAll(failedRowsetKeys)
            assertEquals(999, nonTargetKeys.size())
            assertTrue(nonTargetKeys.containsAll(baselineKeys))
            assertTrue(nonTargetKeys.containsAll(nonTargetFixtureKeys))

            sleep(6000)
            cluster.startRecyclers()
            recyclerStopped = false
            triggerRecycler()

            Set<String> keysAfterRecycle = new HashSet<>()
            boolean recycled = false
            for (int retry = 0; retry < 60; retry++) {
                keysAfterRecycle = listKeys(tabletPrefix)
                if (failedRowsetKeys.every { !keysAfterRecycle.contains(it) }) {
                    recycled = true
                    break
                }
                sleep(2000)
            }
            assertTrue(recycled, "Recycler did not delete prefix ${failedRowsetPrefix}")
            assertEquals(nonTargetKeys, keysAfterRecycle,
                    "Recycler deleted objects outside the failed rowset prefix")

            String recyclerLog = new File(cluster.getAllRecyclers(true).first().getLogFilePath()).text
            assertTrue(recyclerLog.contains("rowset_id=${failedRowsetId}"))
            assertTrue(recyclerLog.contains("task_type=recycle_tmp_rowsets"))
            assertTrue(recyclerLog.contains("delete prefix"))
            assertTrue(recyclerLog.contains(failedRowsetPrefix))
            assertTrue(recyclerLog.contains(
                    "tasks_in_batch=1, total_deleted=${failedRowsetKeys.size()}"))
            assertTrue(recyclerLog.contains(
                    "num_deleted=${failedRowsetKeys.size()}, error_count=0"))

            order_qt_s3_express_recycler_prefix_delete """
                SELECT k, v, score
                FROM test_s3_express_recycler_prefix_delete
                ORDER BY k
            """
        } finally {
            try {
                GetDebugPoint().clearDebugPointsForAllFEs()
            } catch (Throwable t) {
                logger.warn("Failed to clear FE debug points during cleanup: ${t.message}")
            }
            if (recyclerStopped) {
                try {
                    cluster.startRecyclers()
                } catch (Throwable t) {
                    logger.warn("Failed to restart Recycler during cleanup: ${t.message}")
                }
            }
            assertTrue(rootPath.startsWith("${prefix}/s3_express_recycler_"))
            runAws([
                    "s3", "rm", "s3://${bucket}/${rootPath}/",
                    "--region", region,
                    "--recursive",
                    "--only-show-errors"
            ])
        }
    }
}
