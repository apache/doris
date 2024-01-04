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

suite("test_hive_statistic_cache", "p2") {
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
        Thread.sleep(1000);
        def result = sql """show table cached stats customer"""
        assertEquals(result[0][2], "150000000")

        result = sql """show table cached stats lineitem"""
        assertEquals(result[0][2], "5999989709")

        result = sql """show table cached stats region"""
        assertEquals(result[0][2], "5")

        result = sql """show table cached stats nation"""
        assertEquals(result[0][2], "25")

        result = sql """show table cached stats orders"""
        assertEquals(result[0][2], "1500000000")

        result = sql """show table cached stats part"""
        assertEquals(result[0][2], "200000000")

        result = sql """show table cached stats partsupp"""
        assertEquals(result[0][2], "800000000")

        result = sql """show table cached stats supplier"""
        assertEquals(result[0][2], "10000000")

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
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "98")

        result = sql """show column cached stats `stats` (lo_linenumber)"""
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "7")

        result = sql """show column cached stats `stats` (lo_custkey)"""
        assertEquals(result[0][0], "lo_custkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "67423")
        assertEquals(result[0][7], "2735521")

        result = sql """show column cached stats `stats` (lo_partkey)"""
        assertEquals(result[0][0], "lo_partkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "2250")
        assertEquals(result[0][7], "989601")

        result = sql """show column cached stats `stats` (lo_suppkey)"""
        assertEquals(result[0][0], "lo_suppkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "4167")
        assertEquals(result[0][7], "195845")

        result = sql """show column cached stats `stats` (lo_orderdate)"""
        assertEquals(result[0][0], "lo_orderdate")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "19920221")
        assertEquals(result[0][7], "19980721")

        result = sql """show column cached stats `stats` (lo_orderpriority)"""
        assertEquals(result[0][0], "lo_orderpriority")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "880.0")
        assertEquals(result[0][5], "8.8")
        assertEquals(result[0][6], "'1-URGENT'")
        assertEquals(result[0][7], "'5-LOW'")

        result = sql """show column cached stats `stats` (lo_shippriority)"""
        assertEquals(result[0][0], "lo_shippriority")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "1.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "0")

        result = sql """show column cached stats `stats` (lo_extendedprice)"""
        assertEquals(result[0][0], "lo_extendedprice")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "104300")
        assertEquals(result[0][7], "9066094")

        result = sql """show column cached stats `stats` (lo_ordtotalprice)"""
        assertEquals(result[0][0], "lo_ordtotalprice")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "3428256")
        assertEquals(result[0][7], "36771805")

        result = sql """show column cached stats `stats` (lo_discount)"""
        assertEquals(result[0][0], "lo_discount")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "11.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "10")

        result = sql """show column cached stats `stats` (lo_revenue)"""
        assertEquals(result[0][0], "lo_revenue")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "101171")
        assertEquals(result[0][7], "8703450")

        result = sql """show column cached stats `stats` (lo_supplycost)"""
        assertEquals(result[0][0], "lo_supplycost")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "58023")
        assertEquals(result[0][7], "121374")

        result = sql """show column cached stats `stats` (lo_tax)"""
        assertEquals(result[0][0], "lo_tax")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "9.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "8")

        result = sql """show column cached stats `stats` (lo_commitdate)"""
        assertEquals(result[0][0], "lo_commitdate")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "95.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "19920515")
        assertEquals(result[0][7], "19981016")

        result = sql """show column cached stats `stats` (lo_shipmode)"""
        assertEquals(result[0][0], "lo_shipmode")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "421.0")
        assertEquals(result[0][5], "4.21")
        assertEquals(result[0][6], "'AIR'")
        assertEquals(result[0][7], "'TRUCK'")

        result = sql """show column cached stats `stats` (lo_quantity)"""
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "46.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "50")
    }
}

