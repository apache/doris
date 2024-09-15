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

suite("test_http_meta_tables_auth","p0,auth,nonConcurrent") {
    String suiteName = "test_http_meta_tables_auth"
    String dbName = context.config.getDbNameByFile(context.file)
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
    try {
            sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "true"); """
            def getTables = { check_func ->
                httpTest {
                    basicAuthorization "${user}","${pwd}"
                    endpoint "${context.config.feHttpAddress}"
                    uri "/rest/v2/api/meta/namespaces/default_cluster/databases/${dbName}/tables"
                    op "get"
                    check check_func
                }
            }

            getTables.call() {
                respCode, body ->
                    log.info("body:${body}")
                    assertFalse("${body}".contains("${tableName}"))
            }

            sql """grant select_priv on ${dbName}.${tableName} to ${user}"""

            getTables.call() {
                respCode, body ->
                    log.info("body:${body}")
                    assertTrue("${body}".contains("${tableName}"))
            }

            sql """drop table if exists `${tableName}`"""
            try_sql("DROP USER ${user}")
    } finally {
         sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "false"); """
    }


}
