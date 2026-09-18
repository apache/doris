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

suite("test_lance_prepared_vector_search", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("Lance prepared search requires the external MinIO fixtures")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    sql "DROP CATALOG IF EXISTS test_lance_prepared_vector_search"
    sql """CREATE CATALOG test_lance_prepared_vector_search PROPERTIES (
        "type" = "lance",
        "lance.catalog.type" = "filesystem",
        "warehouse" = "s3://warehouse/lance",
        "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.region" = "us-east-1",
        "use_path_style" = "true"
    )"""

    def checkPreparedSearch = { boolean forwarded ->
        def connection = context.getConnection()
        connection.createStatement().withCloseable { control ->
            // PREPARE must run locally; forwarding is enabled only after the statement exists.
            control.execute("SET force_forward_all_queries=false")
            if (forwarded) {
                control.execute("SYNC")
            }
            sql "SET enable_file_scanner_v2 = true"
            sql "SET enable_sql_cache = false"
            sql "SET enable_query_cache = false"
            for (String useIndex : ["false", "true"]) {
                String fixedProperties = """'table'='test_lance_prepared_vector_search.doris.vs_ivf_pq_f32',
                    'column'='embedding', 'metric'='l2', 'use_index'='${useIndex}',
                    'nprobes'='4', 'refine_factor'='10'"""
                def statement = prepareStatement("""SELECT row_id, _distance
                    FROM vector_search(${fixedProperties}, "query_vector"=?, "top_k"=?, "offset"=?, "filter"=?)
                    WHERE row_id > ? ORDER BY _distance, row_id""")
                try {
                    assertTrue(statement instanceof com.mysql.cj.jdbc.ServerPreparedStatement)
                    // Reuse one server statement, including returning to the first vector after a different execution.
                    for (int start : [0, 1023, 0]) {
                        String vector = "[" + (0..<16).collect { it + start }.join(",") + "]"
                        int topK = start == 0 ? 3 : 2
                        int offset = start == 0 ? 0 : 1
                        String filter = start == 0 ? "row_id > 0" : "row_id > 1000"
                        statement.setString(1, vector)
                        statement.setInt(2, topK)
                        statement.setInt(3, offset)
                        statement.setString(4, filter)
                        statement.setInt(5, 0)
                        def expected = sql("""SELECT row_id, _distance
                            FROM vector_search(${fixedProperties}, "query_vector"="${vector}",
                                "top_k"="${topK}", "offset"="${offset}", "filter"="${filter}")
                            WHERE row_id > 0 ORDER BY _distance, row_id""")
                        assertEquals(topK, expected.size())
                        if (forwarded) {
                            control.execute("SET force_forward_all_queries=true")
                        }
                        try {
                            assertEquals(expected, exec(statement))
                        } finally {
                            if (forwarded) {
                                control.execute("SET force_forward_all_queries=false")
                            }
                        }
                    }
                } finally {
                    statement.close()
                }
            }
        }
    }

    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName)
            + "&emulateUnsupportedPstmts=false"
    connect(context.config.jdbcUser, context.config.jdbcPassword, url) {
        checkPreparedSearch(false)
    }

    def followers = sql_return_maparray("SHOW FRONTENDS").findAll {
        it.IsMaster == "false" && it.Alive == "true"
    }
    if (followers.isEmpty()) {
        logger.info("Skip Lance prepared forwarding coverage: no live non-master FE")
    }
    followers.each { fe ->
        String followerUrl = getServerPrepareJdbcUrl(
                "jdbc:mysql://${fe.Host}:${fe.QueryPort}/", context.dbName, false)
                + "&emulateUnsupportedPstmts=false"
        connect(context.config.jdbcUser, context.config.jdbcPassword, followerUrl) {
            checkPreparedSearch(true)
        }
    }
}
