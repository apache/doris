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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_check_file_cache_consistency", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.beConfigs.add("enable_file_cache=true")
    options.beConfigs.add('file_cache_path=[{"path":"/data/doris_cloud/file_cache","total_size":104857600,"query_limit":104857600}]')
    
    docker(options) {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
        
        def beId = backendId_to_backendIP.keySet()[0]
        def beIp = backendId_to_backendIP.get(beId)
        def beHttpPort = backendId_to_backendHttpPort.get(beId)
        def socket = "${beIp}:${beHttpPort}"
        
        // wait for be start
        Thread.sleep(5000)
        
        sql """
            CREATE DATABASE IF NOT EXISTS test_file_cache_db
        """
        
        sql """
            CREATE TABLE IF NOT EXISTS test_file_cache_db.test_table (
                id INT,
                name STRING,
                value DOUBLE
            ) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql """
            INSERT INTO test_file_cache_db.test_table VALUES
            (1, 'test1', 1.1),
            (2, 'test2', 2.2),
            (3, 'test3', 3.3),
            (4, 'test4', 4.4),
            (5, 'test5', 5.5)
        """
        
        // query to trigger cache
        sql """
            SELECT * FROM test_file_cache_db.test_table WHERE id > 0
        """
        
        sql """
            SELECT COUNT(*) FROM test_file_cache_db.test_table
        """
        
        sql """
            SELECT AVG(value) FROM test_file_cache_db.test_table WHERE id > 2
        """
        
        // wait for cache to be created
        Thread.sleep(5000)
        
        // Test 1: list_base_paths operation
        httpTest {
            endpoint ""
            uri "${socket}/api/file_cache?op=list_base_paths"
            op "post"
            body ""
            check {respCode, body ->
                assertEquals(respCode, 200)
                println("List base paths response: ${body}")
                
                assertNotNull(body, "Response body should not be null")
                assertNotEquals(body, "null", "Response body should not be 'null'")
                
                def respJson = parseJson(body)
                assertTrue(respJson instanceof List, "Response should be a JSON array")
                assertEquals(1, respJson.size(), "Should return exactly one base path")
                
                def basePath = respJson[0]
                assertEquals("/data/doris_cloud/file_cache", basePath, "Base path should match configured path")
                
                println("Verified base path: ${basePath}")
            }
        }

        // Test 2: check_consistency operation
        httpTest {
            endpoint ""
            uri "${socket}/api/file_cache?op=check_consistency&base_path=/data/doris_cloud/file_cache"
            op "post"
            body ""
            check {respCode, body ->
                assertEquals(respCode, 200)
                
                if (body == null || body == "null") {
                    println("No inconsistencies found in file cache - this is expected for a clean cache")
                } else {
                    // should't be any inconsistency
                    try {
                        def respJson = parseJson(body)
                        
                        if (respJson instanceof List) {
                            println("Found ${respJson.size()} inconsistencies in file cache")
                            respJson.each { inconsistency ->
                                assertTrue(inconsistency instanceof String, "Each inconsistency should be a string")
                                assertTrue(inconsistency.contains("Hash:") || 
                                         inconsistency.contains("Inconsistency Reason") ||
                                         inconsistency.contains("File cache info") ||
                                         inconsistency.contains("File cahce info"), 
                                         "Inconsistency should contain expected format")
                            }
                        } else {
                            fail("If not null, response should be a JSON array")
                        }
                    } catch (Exception e) {
                        throw e
                    }
                }
            }
        }
    }
} 