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

import groovy.json.JsonSlurper

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("test_lance_index_prewarm", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("Lance prewarm requires the Iceberg MinIO fixtures")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_index_prewarm"
    String tableName = "${catalogName}.doris.vs_ivf_flat_f32"
    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
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
    sql "SET enable_file_scanner_v2 = true"
    def entries = sql """SELECT IndexName, DatasetVersion FROM lance_index_entries("table"="${tableName}")"""
    assertFalse(entries.isEmpty())
    String statement = """WARM UP SELECT embedding FROM ${tableName} PROPERTIES ("read_index_only" = "true")"""
    String query = """
        SELECT row_id, _distance FROM vector_search(
            "table"="${tableName}", "column"="embedding",
            "query_vector"="[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]",
            "top_k"="10", "metric"="l2", "nprobes"="4", "use_index"="true")
        ORDER BY _distance, row_id
    """
    def before = sql(query)
    def warm = sql(statement)
    assertEquals(1, warm.size())
    String indexName = warm[0][1]
    assertTrue(entries.any { it[0] == indexName })
    // Physical entries record the index build version; prewarm pins the current dataset snapshot.
    assertTrue(warm[0][2].toLong() >= entries.find { it[0] == indexName }[1].toLong())
    assertTrue(warm[0][3].toInteger() > 0)
    assertEquals(before, sql(query), "Prewarm must preserve query results")
    def retry = sql(statement)
    assertEquals(warm[0][2], retry[0][2], "Prewarm must not change the dataset version")
    assertEquals(warm[0][3], retry[0][3], "Retry must cover the same eligible backend set")

    // Binary PREPARE and EXECUTE must agree on the result schema, including repeated executions.
    // Force server preparation even when the JDBC URL defaults to client-side emulation.
    def prepared = context.getConnection().unwrap(com.mysql.cj.jdbc.JdbcConnection)
            .serverPrepareStatement(statement)
    try {
        assertTrue(prepared instanceof com.mysql.cj.jdbc.ServerPreparedStatement)
        assertEquals(5, prepared.getMetaData().getColumnCount())
        assertEquals("DatasetVersion", prepared.getMetaData().getColumnName(3))
        for (int execution = 0; execution < 2; execution++) {
            def result = exec(prepared)
            assertEquals(1, result.size())
            assertEquals(indexName, result[0][1])
            assertEquals(warm[0][2].toString(), result[0][2].toString())
            assertEquals(warm[0][3].toString(), result[0][3].toString())
        }
    } finally {
        prepared.close()
    }

    // Selecting * must cover every logical index, even if physical segments repeat its name.
    def allIndexes = sql """WARM UP SELECT * FROM ${tableName} PROPERTIES ("read_index_only" = true)"""
    assertEquals(entries.collect { it[0] }.toSet(), allIndexes.collect { it[1] }.toSet())
    assertEquals(allIndexes.size(), allIndexes.collect { it[1] }.toSet().size())
    assertTrue(allIndexes.every { it[2] == warm[0][2] && it[3] == warm[0][3] })
    def qualified = sql """WARM UP SELECT t.embedding, t.embedding FROM ${tableName} t
            PROPERTIES ("read_index_only" = true)"""
    assertEquals(1, qualified.size())
    assertEquals(indexName, qualified[0][1])
    test {
        sql """WARM UP SELECT missing_column FROM ${tableName} PROPERTIES ("read_index_only" = true)"""
        exception "Unknown or ambiguous index prewarm column"
    }
    test {
        sql """WARM UP SELECT * FROM ${tableName} WHERE row_id = 1 PROPERTIES ("read_index_only" = true)"""
        exception "does not support WHERE or EXPLAIN"
    }
    test {
        sql """WARM UP SELECT * FROM ${tableName} PROPERTIES ("read_index_only" = "invalid")"""
        exception "read_index_only must be true or false"
    }
    test {
        sql """WARM UP SELECT * FROM ${tableName} PROPERTIES ("unknown_option" = true)"""
        exception "Unknown WARM UP SELECT property"
    }
    test {
        sql """WARM UP SELECT * FROM internal.information_schema.tables PROPERTIES ("read_index_only" = true)"""
        exception "requires a Lance catalog table"
    }
    // Index mode uses the SDK cache independently of the data-file-cache session switch.
    String cacheVariable = isCloudMode() ? "disable_file_cache" : "enable_file_cache"
    def oldCache = sql "SELECT @@${cacheVariable}"
    try {
        sql "SET ${cacheVariable} = ${isCloudMode() ? 'true' : 'false'}"
        assertEquals(indexName, sql(statement)[0][1])
        test {
            sql "WARM UP SELECT * FROM ${tableName}"
            exception "requires session variable"
        }
    } finally {
        sql "SET ${cacheVariable} = ${oldCache[0][0]}"
    }
    sql "SET ${cacheVariable} = ${isCloudMode() ? 'false' : 'true'}"
    try {
        for (String suffix : ["", ' PROPERTIES ("read_index_only" = false)']) {
            def dataWarm = sql "WARM UP SELECT row_id FROM ${tableName} WHERE row_id >= 0${suffix}"
            assertTrue(dataWarm.every { it.size() == 6 })
            assertEquals("TOTAL", dataWarm[-1][0])
        }
    } finally {
        sql "SET ${cacheVariable} = ${oldCache[0][0]}"
    }

    // Fail one target only: successful prewarm on other BEs must not hide the failure.
    String currentGroup = null
    if (isCloudMode()) {
        currentGroup = (sql_return_maparray "SHOW CLUSTERS").find { it.is_current == "TRUE" }?.cluster
        assertNotNull(currentGroup)
    }
    def backends = sql_return_maparray "SHOW BACKENDS"
    def target = backends.find {
        def status = new JsonSlurper().parseText(it.Status)
        def tag = new JsonSlurper().parseText(it.Tag)
        it.Alive.toString().equalsIgnoreCase("true") &&
                !it.SystemDecommissioned.toString().equalsIgnoreCase("true") &&
                !status.isQueryDisabled && !status.isLoadDisabled &&
                (currentGroup == null || (tag.compute_group_name ?: tag.cloud_cluster_name) == currentGroup)
    }
    assertNotNull(target)
    String failureStatement = statement
    String point = "PInternalService.prewarm_lance_index.fail"
    try {
        DebugPoint.enableDebugPoint(target.Host, target.HttpPort.toInteger(), NodeType.BE, point)
        test {
            sql failureStatement
            exception "Backend rejected or failed index prewarm"
        }
    } finally {
        DebugPoint.disableDebugPoint(target.Host, target.HttpPort.toInteger(), NodeType.BE, point)
    }
    assertEquals(before, sql(query))
    sql(statement)

    String user = "test_lance_prewarm_user"
    String password = "Test_only_123"
    try_sql "DROP USER '${user}'@'%'"
    sql "CREATE USER '${user}'@'%' IDENTIFIED BY '${password}'"
    sql "GRANT SELECT_PRIV ON `${catalogName}`.*.* TO '${user}'@'%'"
    sql "GRANT SELECT_PRIV ON regression_test TO '${user}'@'%'"
    if (isCloudMode()) {
        def groups = sql "SHOW CLUSTERS"
        sql "GRANT USAGE_PRIV ON CLUSTER `${groups[0][0]}` TO '${user}'@'%'"
    }
    connect(user, password, context.config.jdbcUrl) {
        test {
            sql statement
            exception "ADMIN privilege is required"
        }
    }
    // Keep the catalog and user for investigating a failed run.
}
