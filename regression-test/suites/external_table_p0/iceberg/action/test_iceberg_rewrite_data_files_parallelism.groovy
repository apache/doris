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

suite("test_iceberg_rewrite_data_files_parallelism", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_rewrite_data_files_parallelism"
    String db_name = "test_db_parallelism"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name} """
    sql """use ${db_name}"""

    def getAliveBeCount = {
        def backends = sql_return_maparray "SHOW BACKENDS"
        def alive = backends.findAll { be ->
            def aliveVal = be.Alive
            if (aliveVal == null) {
                aliveVal = be.IsAlive
            }
            if (aliveVal == null) {
                aliveVal = be.alive
            }
            return aliveVal != null && aliveVal.toString().equalsIgnoreCase("true")
        }.size()
        if (alive == 0 && backends.size() > 0) {
            logger.warn("SHOW BACKENDS does not report Alive, fallback to total count")
            alive = backends.size()
        }
        assertTrue(alive > 0, "No alive backend found for rewrite test")
        return alive
    }

    def createAndInsertSmallBatches = { String tableName, int batches ->
        sql """DROP TABLE IF EXISTS ${db_name}.${tableName}"""
        sql """
            CREATE TABLE ${db_name}.${tableName} (
                id INT,
                payload STRING
            ) ENGINE=iceberg
        """
        (1..batches).each { batch ->
            int base = (batch - 1) * 5
            sql """
                INSERT INTO ${db_name}.${tableName} VALUES
                (${base + 1}, lpad('x', 1024, 'x')),
                (${base + 2}, lpad('x', 1024, 'x')),
                (${base + 3}, lpad('x', 1024, 'x')),
                (${base + 4}, lpad('x', 1024, 'x')),
                (${base + 5}, lpad('x', 1024, 'x'))
            """
        }
    }

    def getTotalSize = { String tableName ->
        List<List<Object>> files = sql """SELECT file_size_in_bytes FROM ${tableName}\$files"""
        assertTrue(files.size() > 0, "Expected at least 1 file before rewrite")
        return files.inject(0L) { acc, row -> acc + (row[0] as long) }
    }

    def expectedUpperBound = { long expectedFileCount, int aliveBeCount, int pipelineParallelism ->
        boolean useGather = expectedFileCount <= aliveBeCount
        if (useGather) {
            return (int) Math.min(expectedFileCount, (long) pipelineParallelism)
        }
        long perBeParallelism = Math.max(1L, (long)(expectedFileCount / (long) aliveBeCount))
        perBeParallelism = Math.min(perBeParallelism, (long) pipelineParallelism)
        return (int) Math.min(expectedFileCount, perBeParallelism * aliveBeCount)
    }

    int aliveBeCount = getAliveBeCount()

    // =====================================================================================
    // Case 1: expectedFileCount <= aliveBeCount (GATHER), upper bound is expectedFileCount
    // =====================================================================================
    logger.info("Starting gather boundary case (expectedFileCount <= aliveBeCount)")
    def table_name_gather = "test_rewrite_gather_boundary"
    createAndInsertSmallBatches(table_name_gather, 10)
    long totalSizeGather = getTotalSize(table_name_gather)
    int pipelineParallelism = 16
    sql """set parallel_pipeline_task_num=${pipelineParallelism}"""

    // Calculate target file size to achieve gather case
    long targetFileSizeGather = Math.max(1L, (long) Math.ceil(totalSizeGather * 1.0 / aliveBeCount))
    long expectedFileCountGather = (long) Math.ceil(totalSizeGather * 1.0 / targetFileSizeGather)

    // Due to file size granularity and metadata overhead, allow slight deviation from ideal
    // The test validates expectedUpperBound function behavior near the gather threshold
    assertTrue(expectedFileCountGather <= aliveBeCount + 1,
        "Expected gather case (with tolerance): expectedFileCount=${expectedFileCountGather}, aliveBeCount=${aliveBeCount}")

    def rewriteGatherResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name_gather}
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "${targetFileSizeGather}",
            "min-input-files" = "1",
            "rewrite-all" = "true",
            "max-file-group-size-bytes" = "1099511627776"
        )
    """
    int addedFilesGather = rewriteGatherResult[0][1] as int
    int expectedUpperGather = expectedUpperBound(expectedFileCountGather, aliveBeCount, pipelineParallelism)
    assertTrue(addedFilesGather > 0, "Expected added files > 0 in gather case")
    assertTrue(addedFilesGather <= expectedUpperGather,
        "addedFiles=${addedFilesGather}, expectedUpper=${expectedUpperGather}, "
        + "expectedFileCount=${expectedFileCountGather}")

    // =====================================================================================
    // Case 2: expectedFileCount > aliveBeCount (non-GATHER), limit by expected files per BE
    // =====================================================================================
    logger.info("Starting non-gather case (expectedFileCount > aliveBeCount)")
    def table_name_nongather = "test_rewrite_non_gather"
    createAndInsertSmallBatches(table_name_nongather, 10)
    long totalSizeNonGather = getTotalSize(table_name_nongather)
    pipelineParallelism = 64
    sql """set parallel_pipeline_task_num=${pipelineParallelism}"""

    long targetFileSizeNonGather = Math.max(1L, (long) Math.floor(totalSizeNonGather / (aliveBeCount + 1)))
    long expectedFileCountNonGather = (long) Math.ceil(totalSizeNonGather * 1.0 / targetFileSizeNonGather)
    assertTrue(expectedFileCountNonGather > aliveBeCount,
        "Expected non-gather case: expectedFileCount=${expectedFileCountNonGather}, aliveBeCount=${aliveBeCount}")

    def rewriteNonGatherResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name_nongather}
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "${targetFileSizeNonGather}",
            "min-input-files" = "1",
            "rewrite-all" = "true",
            "max-file-group-size-bytes" = "1099511627776"
        )
    """
    int addedFilesNonGather = rewriteNonGatherResult[0][1] as int
    int expectedUpperNonGather = expectedUpperBound(expectedFileCountNonGather, aliveBeCount, pipelineParallelism)
    assertTrue(addedFilesNonGather > 0, "Expected added files > 0 in non-gather case")
    assertTrue(addedFilesNonGather <= expectedUpperNonGather,
        "addedFiles=${addedFilesNonGather}, expectedUpper=${expectedUpperNonGather}, "
        + "expectedFileCount=${expectedFileCountNonGather}")

    // =====================================================================================
    // Case 3: Moved from test_iceberg_rewrite_data_files, validates upper bound dynamically
    // =====================================================================================
    logger.info("Starting rewrite output file count cap test case (moved)")
    def table_name_limit = "test_rewrite_file_count_cap"
    createAndInsertSmallBatches(table_name_limit, 10)
    long totalSizeLimit = getTotalSize(table_name_limit)
    pipelineParallelism = 64
    sql """set parallel_pipeline_task_num=${pipelineParallelism}"""

    long targetFileSize = Math.max(1L, (long) Math.floor(totalSizeLimit / (aliveBeCount + 1)))
    long expectedFileCount = (long) Math.ceil(totalSizeLimit * 1.0 / targetFileSize)
    if (expectedFileCount <= aliveBeCount) {
        targetFileSize = 1L
        expectedFileCount = totalSizeLimit
    }

    def rewriteLimitResult = sql """
        ALTER TABLE ${catalog_name}.${db_name}.${table_name_limit}
        EXECUTE rewrite_data_files(
            "target-file-size-bytes" = "${targetFileSize}",
            "min-input-files" = "1",
            "rewrite-all" = "true",
            "max-file-group-size-bytes" = "1099511627776"
        )
    """
    int addedFilesCountLimit = rewriteLimitResult[0][1] as int
    int expectedUpper = expectedUpperBound(expectedFileCount, aliveBeCount, pipelineParallelism)
    assertTrue(addedFilesCountLimit > 0, "Expected added files count > 0 after rewrite")
    assertTrue(addedFilesCountLimit <= expectedUpper,
        "addedFilesCount=${addedFilesCountLimit}, expectedUpper=${expectedUpper}, "
        + "aliveBeCount=${aliveBeCount}, expectedFileCount=${expectedFileCount}")

    logger.info("Rewrite data files parallelism tests completed successfully")
}
