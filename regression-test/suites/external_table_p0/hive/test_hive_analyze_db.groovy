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

 suite("test_hive_analyze_db", "p0,external,hive,external_docker,external_docker_hive") {

     def verify_column_stats_result = { column, result, count, ndv, nulls, size, avg_size, min, max ->
         def found = false;
         for (int i = 0; i < result.size(); i++) {
             if (result[i][0] == column) {
                 found = true;
                 assertEquals(count, result[i][2])
                 assertEquals(ndv, result[i][3])
                 assertEquals(nulls, result[i][4])
                 assertEquals(size, result[i][5])
                 assertEquals(avg_size, result[i][6])
                 assertEquals(min, result[i][7])
                 assertEquals(max, result[i][8])
             }
         }
         assertTrue(found)
     }

     String enabled = context.config.otherConfigs.get("enableHiveTest")
     if (enabled == null || !enabled.equalsIgnoreCase("true")) {
         logger.info("diable Hive test.")
         return;
     }

     for (String hivePrefix : ["hive2", "hive3"]) {
         String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
         String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
         String catalog_name = "${hivePrefix}_test_hive_partition_column_analyze"
         sql """drop catalog if exists ${catalog_name};"""
         sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
         logger.info("catalog " + catalog_name + " created")
         sql """switch ${catalog_name};"""
         logger.info("switched to catalog " + catalog_name)
         sql """use statistics;"""
         sql """set query_timeout=300"""
         sql """analyze database statistics with sync"""
         def result = sql """show column stats statistics"""
         assertEquals(result.size(), 17)

         verify_column_stats_result("lo_orderpriority", result, "100.0", "5.0", "0.0", "880.0", "8.8", "'1-URGENT'", "'5-LOW'")
         verify_column_stats_result("lo_custkey", result, "100.0", "26.0", "0.0", "400.0", "4.0", "67423", "2735521")
         verify_column_stats_result("lo_partkey", result, "100.0", "100.0", "0.0", "400.0", "4.0", "2250", "989601")
         verify_column_stats_result("lo_revenue", result, "100.0", "100.0", "0.0", "400.0", "4.0", "101171", "8703450")
         verify_column_stats_result("lo_commitdate", result, "100.0", "95.0", "0.0", "400.0", "4.0", "19920515", "19981016")
         verify_column_stats_result("lo_quantity", result, "100.0", "46.0", "0.0", "400.0", "4.0", "1", "50")
         verify_column_stats_result("lo_orderkey", result, "100.0", "26.0", "0.0", "400.0", "4.0", "1", "98")
         verify_column_stats_result("lo_suppkey", result, "100.0", "100.0", "0.0", "400.0", "4.0", "4167", "195845")
         verify_column_stats_result("lo_supplycost", result, "100.0", "100.0", "0.0", "400.0", "4.0", "58023", "121374")
         verify_column_stats_result("lo_shipmode", result, "100.0", "7.0", "0.0", "421.0", "4.21", "'AIR'", "'TRUCK'")
         verify_column_stats_result("lo_orderdate", result, "100.0", "26.0", "0.0", "400.0", "4.0", "19920221", "19980721")
         verify_column_stats_result("lo_linenumber", result, "100.0", "7.0", "0.0", "400.0", "4.0", "1", "7")
         verify_column_stats_result("lo_shippriority", result, "100.0", "1.0", "0.0", "400.0", "4.0", "0", "0")
         verify_column_stats_result("lo_ordtotalprice", result, "100.0", "26.0", "0.0", "400.0", "4.0", "3428256", "36771805")
         verify_column_stats_result("lo_extendedprice", result, "100.0", "100.0", "0.0", "400.0", "4.0", "104300", "9066094")
         verify_column_stats_result("lo_tax", result, "100.0", "9.0", "0.0", "400.0", "4.0", "0", "8")
         verify_column_stats_result("lo_discount", result, "100.0", "11.0", "0.0", "400.0", "4.0", "0", "10")
     }
 }

