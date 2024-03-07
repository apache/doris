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

suite("test_hive_statistic_cache", "p2,external,hive,external_remote,external_remote_hive") {

    def wait_row_count_reported = { table, expected ->
        for (int i = 0; i < 10; i++) {
            result = sql """show table stats ${table}"""
            logger.info("show table stats result: " + result)
            assertTrue(result.size() == 1)
            if (result[0][2] == "0") {
                Thread.sleep(1000)
                continue;
            }
            assertEquals(expected, result[0][2])
            break;
        }
    }

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic_cache"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        sql """use ${catalog_name}.tpch_1000_parquet"""
        sql """desc customer""";
        sql """desc lineitem""";
        sql """desc region""";
        sql """desc nation""";
        sql """desc orders""";
        sql """desc part""";
        sql """desc partsupp""";
        sql """desc supplier""";

        wait_row_count_reported("customer", "150000000")
        wait_row_count_reported("lineitem", "5999989709")
        wait_row_count_reported("region", "5")
        wait_row_count_reported("nation", "25")
        wait_row_count_reported("orders", "1500000000")
        wait_row_count_reported("part", "200000000")
        wait_row_count_reported("partsupp", "800000000")
        wait_row_count_reported("supplier", "10000000")

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use statistics;"""
        sql """set query_timeout=300;"""
        sql """analyze table `stats` with sync;"""
        sql """select count(*) from stats"""
        Thread.sleep(5000);
        result = sql """show column cached stats `stats` (lo_orderkey)"""
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "98")

        result = sql """show column cached stats `stats` (lo_linenumber)"""
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "7.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "7")

        result = sql """show column cached stats `stats` (lo_custkey)"""
        assertEquals(result[0][0], "lo_custkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "67423")
        assertEquals(result[0][8], "2735521")

        result = sql """show column cached stats `stats` (lo_partkey)"""
        assertEquals(result[0][0], "lo_partkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "100.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "2250")
        assertEquals(result[0][8], "989601")

        result = sql """show column cached stats `stats` (lo_suppkey)"""
        assertEquals(result[0][0], "lo_suppkey")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "100.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "4167")
        assertEquals(result[0][8], "195845")

        result = sql """show column cached stats `stats` (lo_orderdate)"""
        assertEquals(result[0][0], "lo_orderdate")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "19920221")
        assertEquals(result[0][8], "19980721")

        result = sql """show column cached stats `stats` (lo_orderpriority)"""
        assertEquals(result[0][0], "lo_orderpriority")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "880.0")
        assertEquals(result[0][6], "8.0")
        assertEquals(result[0][7], "'1-URGENT'")
        assertEquals(result[0][8], "'5-LOW'")

        result = sql """show column cached stats `stats` (lo_shippriority)"""
        assertEquals(result[0][0], "lo_shippriority")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "1.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "0")
        assertEquals(result[0][8], "0")

        result = sql """show column cached stats `stats` (lo_extendedprice)"""
        assertEquals(result[0][0], "lo_extendedprice")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "100.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "104300")
        assertEquals(result[0][8], "9066094")

        result = sql """show column cached stats `stats` (lo_ordtotalprice)"""
        assertEquals(result[0][0], "lo_ordtotalprice")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "26.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "3428256")
        assertEquals(result[0][8], "36771805")

        result = sql """show column cached stats `stats` (lo_discount)"""
        assertEquals(result[0][0], "lo_discount")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "11.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "0")
        assertEquals(result[0][8], "10")

        result = sql """show column cached stats `stats` (lo_revenue)"""
        assertEquals(result[0][0], "lo_revenue")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "100.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "101171")
        assertEquals(result[0][8], "8703450")

        result = sql """show column cached stats `stats` (lo_supplycost)"""
        assertEquals(result[0][0], "lo_supplycost")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "100.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "58023")
        assertEquals(result[0][8], "121374")

        result = sql """show column cached stats `stats` (lo_tax)"""
        assertEquals(result[0][0], "lo_tax")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "9.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "0")
        assertEquals(result[0][8], "8")

        result = sql """show column cached stats `stats` (lo_commitdate)"""
        assertEquals(result[0][0], "lo_commitdate")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "95.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "19920515")
        assertEquals(result[0][8], "19981016")

        result = sql """show column cached stats `stats` (lo_shipmode)"""
        assertEquals(result[0][0], "lo_shipmode")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "7.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "421.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "'AIR'")
        assertEquals(result[0][8], "'TRUCK'")

        result = sql """show column cached stats `stats` (lo_quantity)"""
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "46.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "400.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "1")
        assertEquals(result[0][8], "50")
    }
}

