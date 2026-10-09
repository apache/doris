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

suite("test_lance_vector_search_distance_range", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        logger.info("Skip Lance distance range tests: Iceberg MinIO is disabled")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_lance_vector_search_distance_range"
    String tableName = "${catalogName}.doris.vs_ivf_flat_f32"
    String query = "[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]"
    def search = { boolean indexed, String options ->
        """vector_search("table"="${tableName}", "column"="embedding", "query_vector"="${query}",
            "top_k"="10", "metric"="l2", "nprobes"="4", "use_index"="${indexed}" ${options})"""
    }
    sql "DROP CATALOG IF EXISTS `${catalogName}`"
    try {
        sql """CREATE CATALOG `${catalogName}` PROPERTIES (
            "type"="lance", "lance.catalog.type"="filesystem", "warehouse"="s3://warehouse/lance",
            "s3.endpoint"="http://${externalEnvIp}:${minioPort}", "s3.access_key"="admin",
            "s3.secret_key"="password", "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SET enable_file_scanner_v2 = true"
        // Squared L2 is 16 * (row_id - 1)^2. Full IVF_FLAT probing gives an exact oracle.
        for (boolean indexed : [false, true]) {
            def cases = [
                [', "distance_upper_bound"="16"', [1L]],
                [', "distance_lower_bound"="16", "distance_upper_bound"="144"', [2L, 3L]],
                [', "distance_lower_bound"="17", "distance_upper_bound"="64"', []],
                [', "distance_lower_bound"="16", "distance_upper_bound"="144", "offset"="1"', [3L]],
                [', "distance_lower_bound"="16", "distance_upper_bound"="144", "filter"="row_id > 2"', [3L]],
                [', "distance_upper_bound"="0"', []]
            ]
            for (def entry : cases) {
                // Project stored columns too, exercising row-id materialization after range selection.
                def rows = sql "SELECT row_id, label, _distance FROM ${search(indexed, entry[0])} ORDER BY _distance"
                assertEquals(entry[1], rows.collect { (it[0] as Number).longValue() })
                for (def row : rows) {
                    double expected = 16.0 * Math.pow((row[0] as Number).longValue() - 1, 2)
                    assertEquals(expected, (row[2] as Number).doubleValue())
                }
            }
            def lowerOnly = sql "SELECT row_id, _distance FROM ${search(indexed, ', \"distance_lower_bound\"=\"16\"')}"
            assertTrue(lowerOnly.size() > 0 && lowerOnly.size() <= 10)
            assertTrue(lowerOnly.every { (it[1] as Number).doubleValue() >= 16.0 })
            explain {
                sql "SELECT row_id FROM ${search(indexed, ', \"distance_upper_bound\"=\"16\"')}"
                contains "lanceDistanceRange=[-inf, 16.0)"
                contains "lanceVectorIndexStatus=${indexed ? 'USED' : 'DISABLED'}"
            }
        }
        for (String value : ["NaN", "Infinity", "1e100", "invalid"]) {
            test {
                sql "SELECT * FROM ${search(false, ', \"distance_upper_bound\"=\"' + value + '\"')}"
                exception "finite FLOAT"
            }
        }
        test {
            sql "SELECT * FROM ${search(false, ', \"distance_lower_bound\"=\"1\", \"distance_upper_bound\"=\"1.00000001\"')}"
            exception "must be less than"
        }
    } finally {
        sql "DROP CATALOG IF EXISTS `${catalogName}`"
    }
}
