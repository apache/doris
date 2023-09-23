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

suite("insert_group_commit_with_large_data") {
    def table = "insert_group_commit_with_large_data"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    try {
        // create table
        sql """ drop table if exists ${table}; """

        sql """
        CREATE TABLE `${table}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        sql """ set enable_insert_group_commit = true; """

        // insert into 5000 rows
        def insert_sql = """ insert into ${table} values(1, 'a', 10)  """
        for (def i in 2..5000) {
            insert_sql += """, (${i}, 'a', 10) """
        }
        def result = sql """ ${insert_sql} """
        logger.info("insert result: " + result)
        assertEquals(1, result.size())
        assertEquals(1, result[0].size())
        assertEquals(5000, result[0][0])
        getRowCount(5000)

        // data size is large than 4MB, need " set global max_allowed_packet = 5508950 "
        /*def name_value = ""
        for (def i in 0..1024) {
            name_value += 'a'
        }
        insert_sql = """ insert into ${table} values(1, '${name_value}', 10)  """
        for (def i in 2..5000) {
            insert_sql += """, (${i}, '${name_value}', 10) """
        }
        result = sql """ ${insert_sql} """
        logger.info("insert result: " + result)
        assertEquals(1, result.size())
        assertEquals(1, result[0].size())
        assertEquals(5000, result[0][0])
        getRowCount(10000)*/
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
