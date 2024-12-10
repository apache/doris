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

import org.junit.Assert;

suite("test_http_permissions_check_auth","p0,auth") {
    String suiteName = "test_http_permissions_check_auth"
    String tableName = "${suiteName}_table"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """drop table if exists `${tableName}`"""
    sql """
        CREATE TABLE `${tableName}` (
          `k1` int,
          `k2` int
        ) ENGINE=OLAP
        DISTRIBUTED BY random BUCKETS auto
        PROPERTIES ('replication_num' = '1') ;
        """
    sql """insert into ${tableName} values(1,1)"""
    sql """set session_context = 'trace_id:mmn9';"""
    sql """select * from ${tableName};"""

    def get_queryid_by_traceid = { check_func ->
        httpTest {
            basicAuthorization "${user}","${pwd}"
            endpoint "${context.config.feHttpAddress}"
            uri "/rest/v2/manager/query/trace_id/mmn9"
            op "get"
            check check_func
        }
    }

    get_queryid_by_traceid.call() {
        respCode, body ->
            log.info("body:${body}")
            assertTrue("${body}".contains("Bad Request"))
    }

    sql """grant 'admin' to ${user}"""

    get_queryid_by_traceid.call() {
        respCode, body ->
            log.info("body:${body}")
            assertTrue("${body}".contains("success"))
    }

    sql """drop table if exists `${tableName}`"""
    try_sql("DROP USER ${user}")
}
