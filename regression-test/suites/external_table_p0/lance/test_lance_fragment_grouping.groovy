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

suite("test_lance_fragment_grouping", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_fragment_grouping"
    // Reuse the immutable multi-fragment fixture from the vector-search suites for ordinary scans.
    String tableName = "${catalogName}.doris.vs_ivf_pq_f32"
    def originalGroupSize = sql("SELECT @@session.lance_fragments_per_split")[0][0]

    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    try {
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

        List<String> queries = [
            "SELECT row_id, category FROM ${tableName} ORDER BY row_id",
            "SELECT row_id FROM ${tableName} WHERE row_id BETWEEN 250 AND 770 "
                    + "AND category = 'odd' ORDER BY row_id",
            "SELECT row_id FROM ${tableName} WHERE row_id < 10 OR row_id > 1000 ORDER BY row_id",
            "SELECT row_id FROM ${tableName} WHERE row_id > 250 "
                    + "AND MOD(row_id, 3) = 1 ORDER BY row_id LIMIT 17 OFFSET 3",
            "SELECT row_id FROM ${tableName} WHERE row_id < 0 ORDER BY row_id",
            "SELECT COUNT(*) FROM ${tableName}"
        ]
        sql "SET lance_fragments_per_split = 1"
        def expected = queries.collect { query -> sql(query) }
        assertTrue(expected[0].size() > 0)
        String baselinePlan = sql("EXPLAIN ${queries[0]}").collect { it[0] }.join("\n")
        def splitMatcher = baselinePlan =~ /inputSplitNum=(\d+)/
        assertTrue(splitMatcher.find())
        int fragmentCount = splitMatcher.group(1).toInteger()
        assertTrue(fragmentCount > 1)

        [2, 3, fragmentCount + 1].each { groupSize ->
            sql "SET lance_fragments_per_split = ${groupSize}"
            int expectedSplits = (fragmentCount + groupSize - 1).intdiv(groupSize)
            explain {
                sql(queries[0])
                contains "lanceFragmentsPerSplit=${groupSize}"
                contains "inputSplitNum=${expectedSplits},"
                contains "lanceFragments=${fragmentCount}"
            }
            queries.eachWithIndex { query, index ->
                assertEquals(expected[index], sql(query))
            }
        }
    } finally {
        sql "SET lance_fragments_per_split = ${originalGroupSize}"
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
