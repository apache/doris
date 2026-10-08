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
    String indexName = entries[0][0]
    String statement = "WARM UP INDEX `${indexName}` ON ${tableName}"
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
    assertEquals(indexName, warm[0][1])
    assertEquals(entries[0][1].toString(), warm[0][2].toString())
    assertTrue(warm[0][3].toInteger() > 0)
    assertEquals(before, sql(query), "Prewarm must preserve query results")
    assertEquals(warm[0][3], sql(statement)[0][3], "Retry must cover the same eligible backend set")

    test {
        sql "WARM UP INDEX missing_index ON ${tableName}"
        exception "does not exist in dataset version"
    }
    if (isCloudMode()) {
        def groups = sql "SHOW CLUSTERS"
        assertFalse(groups.isEmpty())
        def explicit = sql "${statement} WITH COMPUTE GROUP `${groups[0][0]}`"
        assertTrue(explicit[0][3].toInteger() > 0)
    }

    // Fail one target only: successful prewarm on other BEs must not hide the failure.
    def backends = sql_return_maparray "SHOW BACKENDS"
    def target = backends.find {
        def status = new JsonSlurper().parseText(it.Status)
        it.Alive.toString().equalsIgnoreCase("true") &&
                !it.SystemDecommissioned.toString().equalsIgnoreCase("true") &&
                !status.isQueryDisabled && !status.isLoadDisabled
    }
    assertNotNull(target)
    String failureStatement = statement
    if (isCloudMode()) {
        String targetGroup = new JsonSlurper().parseText(target.Tag).cloud_cluster_name
        failureStatement += " WITH COMPUTE GROUP `${targetGroup}`"
    }
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
