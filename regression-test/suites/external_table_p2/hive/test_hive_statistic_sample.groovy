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

suite("test_hive_statistic_sample", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic_sample"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        sql """analyze table ${catalog_name}.tpch_1000_parquet.region with sample rows 10 with sync"""
        sql """analyze table ${catalog_name}.tpch_1000_parquet.supplier with sample percent 10 with sync"""
        sql """use ${catalog_name}.tpch_1000_parquet"""
        def result = sql """show column stats region (r_regionkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_regionkey")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "20.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats region (r_name)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_name")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "34.0")
        assertEquals(result[0][6], "6.8")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats region (r_comment)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_comment")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "330.0")
        assertEquals(result[0][6], "66.0")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_suppkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_suppkey")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "9998799.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "3.9995194E7")
        assertEquals(result[0][6], "3.9999997999759773")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_name)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_name")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "9998799.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "1.79978374E8")
        assertEquals(result[0][6], "17.999999199903908")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_address)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_address")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "9998799.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "2.50070604E8")
        assertEquals(result[0][6], "25.010064108699456")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_nationkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_nationkey")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "25.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "3.9995194E7")
        assertEquals(result[0][6], "3.9999997999759773")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_phone)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_phone")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "9996537.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "1.49981978E8")
        assertEquals(result[0][6], "14.99999929991592")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_acctbal)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_acctbal")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "1054512.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "7.9990388E7")
        assertEquals(result[0][6], "7.999999599951955")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        result = sql """show column stats supplier (s_comment)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "s_comment")
        assertEquals(result[0][2], "9998799.0")
        assertEquals(result[0][3], "9630165.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "6.24883849E8")
        assertEquals(result[0][6], "62.49589065646784")
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        sql """drop catalog ${catalog_name}"""
    }
}

