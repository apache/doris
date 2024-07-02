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

suite("test_hive_statistics_from_hms", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistics_from_hms"
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
        sql """use tpch1_parquet;"""
        // Load cache
        sql """show column cached stats lineitem"""
        Thread.sleep(3000)
        // Get result
        def result = sql """show column cached stats lineitem (l_returnflag)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_returnflag")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "2.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "6001215.0")
        assertTrue(result[0][6] == "1.0")

        result = sql """show column cached stats lineitem (l_receiptdate)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_receiptdate")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "2535.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_tax)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_tax")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "8.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "4.800972E7")
        assertTrue(result[0][6] == "8.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_shipmode)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_shipmode")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "7.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.5717007E7")
        assertTrue(result[0][6] == "4.285300060071169")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_suppkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_suppkey")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "6.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_shipdate)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_shipdate")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "2535.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_commitdate)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_commitdate")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "2427.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_partkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_partkey")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "13152.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_orderkey)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_orderkey")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "1000998.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_quantity)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_quantity")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "31.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "4.800972E7")
        assertTrue(result[0][6] == "8.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_linestatus)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_linestatus")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "2.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "6001215.0")
        assertTrue(result[0][6] == "1.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_comment)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_comment")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "3834237.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "1.5899739E8")
        assertTrue(result[0][6] == "26.494199924515286")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_extendedprice)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_extendedprice")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "1000998.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "4.800972E7")
        assertTrue(result[0][6] == "8.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_linenumber)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_linenumber")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "261329.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "2.400486E7")
        assertTrue(result[0][6] == "4.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_discount)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_discount")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "15.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "4.800972E7")
        assertTrue(result[0][6] == "8.0")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        result = sql """show column cached stats lineitem (l_shipinstruct)"""
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "l_shipinstruct")
        assertTrue(result[0][2] == "6001215.0")
        assertTrue(result[0][3] == "4.0")
        assertTrue(result[0][4] == "0.0")
        assertTrue(result[0][5] == "7.2006178E7")
        assertTrue(result[0][6] == "11.998599950176756")
        assertTrue(result[0][7] == "N/A")
        assertTrue(result[0][8] == "N/A")

        for (int i = 0; i < 10; i++) {
            result = sql """show table stats lineitem"""
            logger.info("show table stats result: " + result)
            assertTrue(result.size() == 1)
            if (result[0][2] == "0") {
                Thread.sleep(1000)
                continue;
            }
            assertTrue(result[0][2] == "6001215")
            break;
        }

        sql """drop catalog ${catalog_name}"""
    }
}

