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

        sql """analyze table ${catalog_name}.tpch_1000_parquet.region with sample percent 10 with sync"""
        sql """analyze table ${catalog_name}.tpch_1000_parquet.supplier with sample percent 10 with sync"""
        sql """use ${catalog_name}.tpch_1000_parquet"""
        def result = sql """show column stats region (r_regionkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "r_regionkey")
        assertTrue(result[0][1] == "5.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "20.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats region (r_name)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "r_name")
        assertTrue(result[0][1] == "5.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "34.0")
        assertTrue(result[0][5] == "6.8")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats region (r_comment)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "r_comment")
        assertTrue(result[0][1] == "5.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "330.0")
        assertTrue(result[0][5] == "66.0")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_suppkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_suppkey")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "9970222.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "3.9995194E7")
        assertTrue(result[0][5] == "3.9999997999759773")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_name)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_name")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "1.004004E7")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "1.79978374E8")
        assertTrue(result[0][5] == "17.999999199903908")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_address)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_address")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "9998862.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "2.50070604E8")
        assertTrue(result[0][5] == "25.010064108699456")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_nationkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_nationkey")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "25.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "3.9995194E7")
        assertTrue(result[0][5] == "3.9999997999759773")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_phone)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_phone")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "9928006.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "1.49981978E8")
        assertTrue(result[0][5] == "14.99999929991592")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_acctbal)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_acctbal")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "4766937.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "7.9990388E7")
        assertTrue(result[0][5] == "7.999999599951955")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        result = sql """show column stats supplier (s_comment)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "s_comment")
        assertTrue(result[0][1] == "9998799.0")
        assertTrue(result[0][2] == "9931298.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "6.24883849E8")
        assertTrue(result[0][5] == "62.49589065646784")
        assertTrue(result[0][6] == "N/A")
        assertTrue(result[0][7] == "N/A")

        sql """drop catalog ${catalog_name}"""
    }
}

