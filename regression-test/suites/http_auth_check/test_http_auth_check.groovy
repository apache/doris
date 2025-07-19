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

suite("test_http_auth_check", "p0,auth,nonConcurrent") {
    String suiteName = "test_http_auth_check"
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
    sql """insert into ${tableName} values(1,1)"""
    
    try {
        // Test the isHttpAuthEnabled() helper method
        def authEnabled = isHttpAuthEnabled()
        logger.info("HTTP auth enabled: ${authEnabled}")
        
        // Test enabling HTTP auth
        sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "true"); """
        
        // Verify config is enabled
        def authEnabledAfter = isHttpAuthEnabled()
        logger.info("HTTP auth enabled after setting: ${authEnabledAfter}")
        assertTrue(authEnabledAfter, "HTTP auth should be enabled")
        
        // Test health endpoint with authentication
        def healthCheck = { check_func ->
            httpTest {
                basicAuthorization "${context.config.feHttpUser}","${context.config.feHttpPassword}"
                endpoint "${context.config.feHttpAddress}"
                uri "/api/health"
                op "get"
                check check_func
            }
        }
        
        healthCheck.call() {
            respCode, body ->
                logger.info("Health check response: code=${respCode}, body=${body}")
                assertTrue(respCode == 200, "Health check should succeed with authentication")
        }
        
        // Test httpGet helper method
        try {
            def healthUrl = "http://${context.config.feHttpAddress}/api/health"
            def result = httpGet(healthUrl, false, true)
            logger.info("httpGet result: ${result}")
            assertNotNull(result, "httpGet should return a result")
        } catch (Exception e) {
            logger.warn("httpGet test failed: ${e.message}")
        }
        
        // Test database metadata with authentication
        def getDatabases = { check_func ->
            httpTest {
                basicAuthorization "${context.config.feHttpUser}","${context.config.feHttpPassword}"
                endpoint "${context.config.feHttpAddress}"
                uri "/rest/v2/api/meta/namespaces/default_cluster/databases"
                op "get"
                check check_func
            }
        }
        
        getDatabases.call() {
            respCode, body ->
                logger.info("Database metadata response: code=${respCode}")
                assertTrue(respCode == 200, "Database metadata should succeed with authentication")
        }
        
        // Test table schema API with authentication
        def getTableSchema = { check_func ->
            httpTest {
                basicAuthorization "${context.config.feHttpUser}","${context.config.feHttpPassword}"
                endpoint "${context.config.feHttpAddress}"
                uri "/api/${dbName}/${tableName}/_schema"
                op "get"
                check check_func
            }
        }
        
        getTableSchema.call() {
            respCode, body ->
                logger.info("Table schema response: code=${respCode}")
                assertTrue(respCode == 200, "Table schema should succeed with authentication")
        }
        
        sql """drop table if exists `${tableName}`"""
        try_sql("DROP USER ${user}")
        
    } finally {
        sql """ ADMIN SET ALL FRONTENDS CONFIG ("enable_all_http_auth" = "false"); """
    }
}