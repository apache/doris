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

import com.mysql.cj.jdbc.StatementImpl
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_group_commit_interval_ms_property") {

    def dbName = "regression_test_insert_p0"
    def tableName = "test_group_commit_interval_ms_property_tbl"
    def table = dbName + "." + tableName

    def group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
        }
        // assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(serverInfo.contains("'label':'group_commit_"))
        return serverInfo
    }



    for (item in ["legacy", "nereids"]) {
        try {
            test_table = table + "_" + item;
            sql """ drop table if exists ${test_table} force; """
            sql """
            CREATE table ${test_table} (
                k bigint,  
                v bigint
                )  
                UNIQUE KEY(k)  
                DISTRIBUTED BY HASH (k) BUCKETS 8
                PROPERTIES(  
                "replication_num" = "1",
                "group_commit_interval_ms"="10000"
                );
            """

            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {

            sql """ set group_commit = async_mode; """

            if (item == "nereids") {
                sql """ set enable_nereids_planner=true; """
                sql """ set enable_fallback_to_original_planner=false; """
            } else {
                sql """ set enable_nereids_planner = false; """
            }

            def res1 = sql """show create table ${test_table}"""
            assertTrue(res1.toString().contains("\"group_commit_interval_ms\" = \"10000\""))

            def msg1 = group_commit_insert """insert into ${test_table} values(1,1); """, 1

            Thread.sleep(2000);

            def msg2 = group_commit_insert """insert into ${test_table} values(2,2) """, 1

            assertEquals(msg1.substring(msg1.indexOf("group_commit")+11, msg1.indexOf("group_commit")+43), msg2.substring(msg2.indexOf("group_commit")+11, msg2.indexOf("group_commit")+43));

            sql "ALTER table ${test_table} SET (\"group_commit_interval_ms\"=\"1000\"); "

            def res2 = sql """show create table ${test_table}"""
            assertTrue(res2.toString().contains("\"group_commit_interval_ms\" = \"1000\""))

            def msg3 = group_commit_insert """insert into ${test_table} values(3,3); """, 1

            Thread.sleep(8000);

            def msg4 = group_commit_insert """insert into ${test_table} values(4,4); """, 1

            assertNotEquals(msg3.substring(msg3.indexOf("group_commit")+11, msg3.indexOf("group_commit")+43), msg4.substring(msg4.indexOf("group_commit")+11, msg4.indexOf("group_commit")+43));

            }
        } finally {
                // try_sql("DROP TABLE ${table}")
        }
    }
}
