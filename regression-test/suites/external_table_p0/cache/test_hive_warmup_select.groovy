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

suite("test_hive_warmup_select", "p0,external,hive,external_docker,external_docker_hive,nonConcurrent") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    def test_basic_warmup = {
        // Enable file cache for warm up functionality
        sql "set enable_file_cache=true"
        sql "set disable_file_cache=false"
        
        sql "WARM UP SELECT * FROM lineitem"

        sql "WARM UP SELECT l_orderkey, l_discount FROM lineitem"
        
        sql "WARM UP SELECT l_orderkey, l_discount FROM lineitem WHERE l_quantity > 10"
    }

    def test_warmup_negative_cases = {
        // Enable file cache for warm up functionality
        sql "set enable_file_cache=true"
        sql "set disable_file_cache=false"
        
        // These should fail as warm up select doesn't support these operations
        try {
            sql "WARM UP SELECT * FROM lineitem LIMIT 5"
            assert false : "Expected ParseException for LIMIT clause"
        } catch (Exception e) {
            // Expected to fail
            println "LIMIT clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT l_shipmode, COUNT(*) FROM lineitem GROUP BY l_shipmode"
            assert false : "Expected ParseException for GROUP BY clause"
        } catch (Exception e) {
            // Expected to fail
            println "GROUP BY clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT * FROM lineitem t1 JOIN lineitem t2 ON t1.l_orderkey = t2.l_orderkey"
            assert false : "Expected ParseException for JOIN clause"
        } catch (Exception e) {
            // Expected to fail
            println "JOIN clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT * FROM lineitem UNION SELECT * FROM lineitem"
            assert false : "Expected ParseException for UNION clause"
        } catch (Exception e) {
            // Expected to fail
            println "UNION clause correctly rejected for WARM UP SELECT"
        }
    }

    def test_warmup_permission = { String catalog_name ->
        // Test that warm up select only requires SELECT privilege, not LOAD privilege
        def user1 = 'test_hive_warmup_user1'
        def pwd = '123456'
        def tokens = context.config.jdbcUrl.split('/')
        def url = tokens[0] + "//" + tokens[2] + "/?defaultCatalog=${catalog_name}"

        // Clean up
        sql """DROP USER IF EXISTS ${user1}"""

        try {
            // Create user with only SELECT privilege on external catalog
            sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
            sql """GRANT SELECT_PRIV ON ${catalog_name}.tpch1_parquet.lineitem TO ${user1}"""

            // Test: user with only SELECT privilege should be able to run WARM UP SELECT
            connect(user1, "${pwd}", url) {
                sql "set enable_file_cache=true"
                sql "set disable_file_cache=false"

                // This should succeed - only SELECT privilege is needed
                sql "WARM UP SELECT * FROM ${catalog_name}.tpch1_parquet.lineitem"
                
                sql "WARM UP SELECT l_orderkey, l_discount FROM ${catalog_name}.tpch1_parquet.lineitem WHERE l_quantity > 10"
                
                // Verify regular SELECT also works
                def result = sql "SELECT COUNT(*) FROM ${catalog_name}.tpch1_parquet.lineitem"
                assert result.size() > 0
            }

            // Test: user without SELECT privilege should fail
            sql """REVOKE SELECT_PRIV ON ${catalog_name}.tpch1_parquet.lineitem FROM ${user1}"""
            
            connect(user1, "${pwd}", url) {
                sql "set enable_file_cache=true"
                sql "set disable_file_cache=false"
                test {
                    sql "WARM UP SELECT * FROM ${catalog_name}.tpch1_parquet.lineitem"
                    exception "denied"
                }
            }

            // Test: user with LOAD privilege but no SELECT privilege should also fail
            sql """GRANT LOAD_PRIV ON ${catalog_name}.tpch1_parquet.lineitem TO ${user1}"""
            
            connect(user1, "${pwd}", url) {
                sql "set enable_file_cache=true"
                sql "set disable_file_cache=false"
                test {
                    sql "WARM UP SELECT * FROM ${catalog_name}.tpch1_parquet.lineitem"
                    exception "denied"
                }
            }

        } finally {
            // Clean up
            sql """DROP USER IF EXISTS ${user1}"""
        }
    }

    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_${hivePrefix}_warmup_select"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        sql """switch ${catalog_name}"""
        sql """use `tpch1_parquet`"""

        test_basic_warmup()
        test_warmup_negative_cases()
        test_warmup_permission(catalog_name)

        sql """drop catalog if exists ${catalog_name}"""
    }
}

