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

suite("test_hive_warmup_select", "p0,external,hive,external_docker,external_docker_hive") {
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

    for (String hivePrefix : ["hive2", "hive3"]) {
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

        sql """drop catalog if exists ${catalog_name}"""
    }
}

