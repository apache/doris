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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_insert_timeout") {
    def tableName = "test_insert_timeout_tbl"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NOT NULL,
            `name` varchar(50) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    try {
        def timeout = sql "SHOW FRONTEND CONFIG like '%max_load_timeout_second%';"

        timeout = Integer.parseInt(timeout[0][1])

        logger.info("${timeout}")

        def invalid_timeout = timeout + 200

        sql " set query_timeout = ${invalid_timeout} "

        test {
            sql " INSERT INTO ${tableName} VALUES (1, 'Alice', 90), (2, 'Bob', 85), (3, 'Charlie', 95) "
            exception "begin transaction failed. errCode = 2, detailMessage = Invalid timeout: ${invalid_timeout}. Timeout should be lower than Config.max_load_timeout_second: ${timeout}"
        }

    } finally {
        sql """
        UNSET VARIABLE query_timeout;
        """
    }
}
