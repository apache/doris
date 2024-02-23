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
import groovy.json.JsonBuilder

suite("regression_test_variant_column_limit", "nonConcurrent"){
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }
    def table_name = "var_column_limit"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    try {
        def jsonBuilder = new JsonBuilder()
        def root = jsonBuilder {
            // Generate 2049 fields
            (1..2049).each { fieldNumber ->
                "field$fieldNumber" fieldNumber
            }
        }

        String jsonString = jsonBuilder.toPrettyString()
        sql """insert into ${table_name} values (1, '$jsonString')"""
    } catch(Exception ex) {
        logger.info("""INSERT INTO ${table_name} failed: """ + ex)
        assertTrue(ex.toString().contains("Reached max column"));
    } finally {
    }
    sql """insert into ${table_name} values (1, '{"a" : 1, "b" : 2, "c" : 3}')"""

}