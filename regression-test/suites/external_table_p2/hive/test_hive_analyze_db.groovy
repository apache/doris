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

suite("test_hive_analyze_db", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_analyze_db"
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

        assertEquals(result[0][0], "lo_orderpriority")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "880.0")
        assertEquals(result[0][6], "8.8")
        assertEquals(result[0][7], "'1-URGENT'")
        assertEquals(result[0][8], "'5-LOW'")
        
        assertEquals(result[1][0], "lo_custkey")
        assertEquals(result[1][2], "100.0")
        assertEquals(result[1][3], "26.0")
        assertEquals(result[1][4], "0.0")
        assertEquals(result[1][5], "400.0")
        assertEquals(result[1][6], "4.0")
        assertEquals(result[1][7], "67423")
        assertEquals(result[1][8], "2735521")

        assertEquals(result[2][0], "lo_partkey")
        assertEquals(result[2][2], "100.0")
        assertEquals(result[2][3], "100.0")
        assertEquals(result[2][4], "0.0")
        assertEquals(result[2][5], "400.0")
        assertEquals(result[2][6], "4.0")
        assertEquals(result[2][7], "2250")
        assertEquals(result[2][8], "989601")

        assertEquals(result[3][0], "lo_revenue")
        assertEquals(result[3][2], "100.0")
        assertEquals(result[3][3], "100.0")
        assertEquals(result[3][4], "0.0")
        assertEquals(result[3][5], "400.0")
        assertEquals(result[3][6], "4.0")
        assertEquals(result[3][7], "101171")
        assertEquals(result[3][8], "8703450")

        assertEquals(result[4][0], "lo_commitdate")
        assertEquals(result[4][2], "100.0")
        assertEquals(result[4][3], "95.0")
        assertEquals(result[4][4], "0.0")
        assertEquals(result[4][5], "400.0")
        assertEquals(result[4][6], "4.0")
        assertEquals(result[4][7], "19920515")
        assertEquals(result[4][8], "19981016")
        
        assertEquals(result[5][0], "lo_quantity")
        assertEquals(result[5][2], "100.0")
        assertEquals(result[5][3], "46.0")
        assertEquals(result[5][4], "0.0")
        assertEquals(result[5][5], "400.0")
        assertEquals(result[5][6], "4.0")
        assertEquals(result[5][7], "1")
        assertEquals(result[5][8], "50")

        assertEquals(result[6][0], "lo_orderkey")
        assertEquals(result[6][2], "100.0")
        assertEquals(result[6][3], "26.0")
        assertEquals(result[6][4], "0.0")
        assertEquals(result[6][5], "400.0")
        assertEquals(result[6][6], "4.0")
        assertEquals(result[6][7], "1")
        assertEquals(result[6][8], "98")

        assertEquals(result[7][0], "lo_suppkey")
        assertEquals(result[7][2], "100.0")
        assertEquals(result[7][3], "100.0")
        assertEquals(result[7][4], "0.0")
        assertEquals(result[7][5], "400.0")
        assertEquals(result[7][6], "4.0")
        assertEquals(result[7][7], "4167")
        assertEquals(result[7][8], "195845")

        assertEquals(result[8][0], "lo_supplycost")
        assertEquals(result[8][2], "100.0")
        assertEquals(result[8][3], "100.0")
        assertEquals(result[8][4], "0.0")
        assertEquals(result[8][5], "400.0")
        assertEquals(result[8][6], "4.0")
        assertEquals(result[8][7], "58023")
        assertEquals(result[8][8], "121374")

        assertEquals(result[9][0], "lo_shipmode")
        assertEquals(result[9][2], "100.0")
        assertEquals(result[9][3], "7.0")
        assertEquals(result[9][4], "0.0")
        assertEquals(result[9][5], "421.0")
        assertEquals(result[9][6], "4.21")
        assertEquals(result[9][7], "'AIR'")
        assertEquals(result[9][8], "'TRUCK'")

        assertEquals(result[10][0], "lo_orderdate")
        assertEquals(result[10][2], "100.0")
        assertEquals(result[10][3], "26.0")
        assertEquals(result[10][4], "0.0")
        assertEquals(result[10][5], "400.0")
        assertEquals(result[10][6], "4.0")
        assertEquals(result[10][7], "19920221")
        assertEquals(result[10][8], "19980721")

        assertEquals(result[11][0], "lo_linenumber")
        assertEquals(result[11][2], "100.0")
        assertEquals(result[11][3], "7.0")
        assertEquals(result[11][4], "0.0")
        assertEquals(result[11][5], "400.0")
        assertEquals(result[11][6], "4.0")
        assertEquals(result[11][7], "1")
        assertEquals(result[11][8], "7")

        assertEquals(result[12][0], "lo_shippriority")
        assertEquals(result[12][2], "100.0")
        assertEquals(result[12][3], "1.0")
        assertEquals(result[12][4], "0.0")
        assertEquals(result[12][5], "400.0")
        assertEquals(result[12][6], "4.0")
        assertEquals(result[12][7], "0")
        assertEquals(result[12][8], "0")

        assertEquals(result[13][0], "lo_ordtotalprice")
        assertEquals(result[13][2], "100.0")
        assertEquals(result[13][3], "26.0")
        assertEquals(result[13][4], "0.0")
        assertEquals(result[13][5], "400.0")
        assertEquals(result[13][6], "4.0")
        assertEquals(result[13][7], "3428256")
        assertEquals(result[13][8], "36771805")

        assertEquals(result[14][0], "lo_extendedprice")
        assertEquals(result[14][2], "100.0")
        assertEquals(result[14][3], "100.0")
        assertEquals(result[14][4], "0.0")
        assertEquals(result[14][5], "400.0")
        assertEquals(result[14][6], "4.0")
        assertEquals(result[14][7], "104300")
        assertEquals(result[14][8], "9066094")

        assertEquals(result[15][0], "lo_tax")
        assertEquals(result[15][2], "100.0")
        assertEquals(result[15][3], "9.0")
        assertEquals(result[15][4], "0.0")
        assertEquals(result[15][5], "400.0")
        assertEquals(result[15][6], "4.0")
        assertEquals(result[15][7], "0")
        assertEquals(result[15][8], "8")

        assertEquals(result[16][0], "lo_discount")
        assertEquals(result[16][2], "100.0")
        assertEquals(result[16][3], "11.0")
        assertEquals(result[16][4], "0.0")
        assertEquals(result[16][5], "400.0")
        assertEquals(result[16][6], "4.0")
        assertEquals(result[16][7], "0")
        assertEquals(result[16][8], "10")
    }
}

