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

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

suite("test_cloud_variant_desc_rpc_lock", "p0") {
    if (!isCloudMode() || !getFeConfig("enable_debug_points").toBoolean()) {
        logger.info("This suite requires a cloud cluster with FE debug points enabled")
        return
    }

    def dbName = sql("SELECT DATABASE()")[0][0]
    def master = sql_return_maparray("SHOW FRONTENDS").find { it.IsMaster.toBoolean() }
    def masterHttpPort = master.HttpPort.toInteger()
    def masterUrl = context.config.jdbcUrl.replaceFirst("(?<=//)[^/]+", "${master.Host}:${master.QueryPort}")
    connect(context.config.jdbcUser, context.config.jdbcPassword, masterUrl) {
        sql "USE `${dbName}`"
        sql "DROP TABLE IF EXISTS cloud_variant_desc_rpc_lock"
        sql """
            CREATE TABLE cloud_variant_desc_rpc_lock (id INT, payload VARIANT<PROPERTIES(
                    "variant_enable_doc_mode" = "false")>)
            DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (PARTITION p1 VALUES LESS THAN (100))
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("light_schema_change" = "true")
        """
        sql """INSERT INTO cloud_variant_desc_rpc_lock VALUES (0, PARSE_TO_VARIANT('{"detail":1}'))"""
        sql "SELECT * FROM cloud_variant_desc_rpc_lock LIMIT 1"

        def dbId = get_database_id("internal", dbName)
        def tableId = get_table_id("internal", dbName, "cloud_variant_desc_rpc_lock")
        def indexPath = "/dbs/${dbId}/${tableId}/index_schema"
        def baseIndex = sql("SHOW PROC '${indexPath}'").find { it[1] == "cloud_variant_desc_rpc_lock" }[0]
        def schemaPath = "${indexPath}/${baseIndex}"
        def wholeTableDesc = "DESC cloud_variant_desc_rpc_lock"
        def wholeTableProc = "SHOW PROC '${schemaPath}'"

        def whileSchemaRequestIsPaused = { String query, String debugPoint, String label, Closure action ->
            def connectionIds = new ArrayBlockingQueue(1)
            def startQuery = new CountDownLatch(1)
            def schema = extraThread("schema-${label}") {
                sql "USE `${dbName}`"
                sql "SET describe_extend_variant_column = true"
                sql "SET cloud_partition_version_cache_ttl_ms = 0"
                connectionIds.add(sql("SELECT CONNECTION_ID()")[0][0])
                assertTrue(startQuery.await(30, TimeUnit.SECONDS))
                sql query
            }
            try {
                def connectionId = connectionIds.poll(15, TimeUnit.SECONDS)
                assertNotNull(connectionId)
                DebugPoint.enableDebugPoint(master.Host, masterHttpPort, NodeType.FE, debugPoint,
                        [timeout: "60", connection_id: connectionId.toString()])
                startQuery.countDown()
                awaitUntil(10, 0.1) {
                    sql_return_maparray("SHOW FULL PROCESSLIST").any {
                        it.Id.toString() == connectionId.toString() && query.equalsIgnoreCase(it.Info)
                                && it.Time.toInteger() >= 1
                    }
                }
                def ddl = extraThread("ddl-${label}") {
                    sql "USE `${dbName}`"
                    action()
                }
                ddl.get(10, TimeUnit.SECONDS)
                assertFalse(schema.isDone(), "DDL must complete while ${query} is still paused")
            } finally {
                startQuery.countDown()
                DebugPoint.disableDebugPoint(master.Host, masterHttpPort, NodeType.FE, debugPoint)
            }
            return schema
        }
        def checkConcurrentDdl = { String query, String debugPoint, String comment ->
            def schema = whileSchemaRequestIsPaused(query, debugPoint, comment) {
                sql "ALTER TABLE cloud_variant_desc_rpc_lock MODIFY COMMENT '${comment}'"
            }
            def columns = schema.get(30, TimeUnit.SECONDS).collect { it[0] }
            assertTrue(columns.contains("payload.detail"), "${query} returned ${columns}".toString())
        }

        assertTrue(getFeConfig("enable_auto_start_for_cloud_cluster").toBoolean())
        def resume = "CloudSystemInfoService.waitForClusterToResume.pause"
        [wholeTableDesc, "DESC cloud_variant_desc_rpc_lock PARTITION (p1)",
                wholeTableProc, "SHOW PROC '${schemaPath}/p1'"].eachWithIndex { query, index ->
            checkConcurrentDdl(query, resume, "resume-${index}")
        }
        def version = "CloudPartition.getVisibleVersionFromMs.pause"
        checkConcurrentDdl(wholeTableDesc, version, "version-desc")
        checkConcurrentDdl(wholeTableProc, version, "version-proc")

        def schema = whileSchemaRequestIsPaused(wholeTableDesc, version, "light-add") {
            sql """
                ALTER TABLE cloud_variant_desc_rpc_lock ADD COLUMN extra_payload
                VARIANT<PROPERTIES("variant_enable_doc_mode" = "false")>
            """
            sql """
                INSERT INTO cloud_variant_desc_rpc_lock (id, payload, extra_payload)
                VALUES (1, PARSE_TO_VARIANT('{"detail":2}'), PARSE_TO_VARIANT('{"new_key":3}'))
            """
            sql "SELECT CAST(extra_payload AS STRING) FROM cloud_variant_desc_rpc_lock WHERE id = 1"
        }
        try {
            def columns = schema.get(30, TimeUnit.SECONDS).collect { it[0] }
            assertTrue(false, "Extended DESC returned a snapshot across a schema change: ${columns}".toString())
        } catch (ExecutionException error) {
            assertTrue(error.cause.message.contains("Table schema changed during extended DESCRIBE"), error.toString())
        }
        sql "SET describe_extend_variant_column = true"
        def recovered = sql(wholeTableDesc).collect { it[0] }
        assertTrue(recovered.containsAll(["payload.detail", "extra_payload", "extra_payload.new_key"]))
    }
}
