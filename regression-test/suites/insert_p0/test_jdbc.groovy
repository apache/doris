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

import java.util.Arrays
import java.util.stream.Collectors

suite("test_jdbc") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_insert_p0"
    def tableName = realDb + ".test_jdbc"

    sql "CREATE DATABASE IF NOT EXISTS ${realDb}"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `phone` varchar(50) NULL,
        ) ENGINE=OLAP
        unique KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    // Parse url
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://localhost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://localhost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    String url = String.format("jdbc:mysql://%s:%s/%s?useLocalSessionState=true", sql_ip, sql_port, realDb)
    def batchSize = 5

    def urls = [
        url,
        url + "&rewriteBatchedStatements=true",
        url + "&rewriteBatchedStatements=true&allowMultiQueries=true",
        url + "&rewriteBatchedStatements=true&allowMultiQueries=false"
    ]

    def insert = { jdbc_url ->
        connect(user, password, jdbc_url) {
            logger.info("insert url: {}", jdbc_url)
            def ps = prepareStatement "insert into ${tableName} values(?, ?)"
            for (int i = 0; i < batchSize; i++) {
                String phone = UUID.randomUUID().toString()
                ps.setInt(1, i + 1)
                ps.setString(2, phone)
                logger.info((i + 1) + ", " + phone)
                ps.addBatch()
            }
            int[] results = ps.executeBatch()
            logger.info("insert results: {}", Arrays.stream(results).boxed().map(i -> String.valueOf(i)).collect(Collectors.joining(", ")))
            ps.close()
        }
    }

    def update = { jdbc_url ->
        connect(user, password, jdbc_url) {
            logger.info("update url: {}", jdbc_url)
            def ps = prepareStatement "update ${tableName} set phone = ? where id = ?";
            for (int i = 0; i < batchSize; i++) {
                String phone = UUID.randomUUID().toString()
                ps.setInt(2, i + 1)
                ps.setString(1, phone)
                logger.info((i + 1) + ", " + phone)
                ps.addBatch()
            }
            int[] results = ps.executeBatch()
            logger.info("update results: {}", Arrays.stream(results).boxed().map(i -> String.valueOf(i)).collect(Collectors.joining(", ")))
            ps.close()
        }
    }

    for (final def jdbc_url in urls) {
        insert(jdbc_url)
        update(jdbc_url)
    }
}
