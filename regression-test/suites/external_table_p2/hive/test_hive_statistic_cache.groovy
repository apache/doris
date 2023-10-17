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
        assertTrue(result[0][2] == "150000000")

        result = sql """show table cached stats lineitem"""
        assertTrue(result[0][2] == "5999989709")

        result = sql """show table cached stats region"""
        assertTrue(result[0][2] == "5")

        result = sql """show table cached stats nation"""
        assertTrue(result[0][2] == "25")

        result = sql """show table cached stats orders"""
        assertTrue(result[0][2] == "1500000000")

        result = sql """show table cached stats part"""
        assertTrue(result[0][2] == "200000000")

        result = sql """show table cached stats partsupp"""
        assertTrue(result[0][2] == "800000000")

        result = sql """show table cached stats supplier"""
        assertTrue(result[0][2] == "10000000")

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use statistics;"""
        sql """set query_timeout=300;"""
        sql """analyze table `stats` with sync;"""
        sql """select count(*) from stats"""
        Thread.sleep(5000);
        result = sql """show column cached stats `stats` (lo_orderkey)"""
        assertTrue(result[0][0] == "lo_orderkey")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "26.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "98")

        result = sql """show column cached stats `stats` (lo_linenumber)"""
        assertTrue(result[0][0] == "lo_linenumber")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "7.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "7")

        result = sql """show column cached stats `stats` (lo_custkey)"""
        assertTrue(result[0][0] == "lo_custkey")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "26.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "67423")
        assertTrue(result[0][7] == "2735521")

        result = sql """show column cached stats `stats` (lo_partkey)"""
        assertTrue(result[0][0] == "lo_partkey")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "2250")
        assertTrue(result[0][7] == "989601")

        result = sql """show column cached stats `stats` (lo_suppkey)"""
        assertTrue(result[0][0] == "lo_suppkey")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "4167")
        assertTrue(result[0][7] == "195845")

        result = sql """show column cached stats `stats` (lo_orderdate)"""
        assertTrue(result[0][0] == "lo_orderdate")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "26.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "19920221")
        assertTrue(result[0][7] == "19980721")

        result = sql """show column cached stats `stats` (lo_orderpriority)"""
        assertTrue(result[0][0] == "lo_orderpriority")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "5.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "888.8000000000001")
        assertTrue(result[0][5] == "8.8")
        assertTrue(result[0][6] == "'1-URGENT'")
        assertTrue(result[0][7] == "'5-LOW'")

        result = sql """show column cached stats `stats` (lo_shippriority)"""
        assertTrue(result[0][0] == "lo_shippriority")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "1.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "0")
        assertTrue(result[0][7] == "0")

        result = sql """show column cached stats `stats` (lo_extendedprice)"""
        assertTrue(result[0][0] == "lo_extendedprice")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "104300")
        assertTrue(result[0][7] == "9066094")

        result = sql """show column cached stats `stats` (lo_ordtotalprice)"""
        assertTrue(result[0][0] == "lo_ordtotalprice")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "26.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "3428256")
        assertTrue(result[0][7] == "36771805")

        result = sql """show column cached stats `stats` (lo_discount)"""
        assertTrue(result[0][0] == "lo_discount")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "11.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "0")
        assertTrue(result[0][7] == "10")

        result = sql """show column cached stats `stats` (lo_revenue)"""
        assertTrue(result[0][0] == "lo_revenue")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "101171")
        assertTrue(result[0][7] == "8703450")

        result = sql """show column cached stats `stats` (lo_supplycost)"""
        assertTrue(result[0][0] == "lo_supplycost")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "100.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "58023")
        assertTrue(result[0][7] == "121374")

        result = sql """show column cached stats `stats` (lo_tax)"""
        assertTrue(result[0][0] == "lo_tax")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "9.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "0")
        assertTrue(result[0][7] == "8")

        result = sql """show column cached stats `stats` (lo_commitdate)"""
        assertTrue(result[0][0] == "lo_commitdate")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "95.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "19920515")
        assertTrue(result[0][7] == "19981016")

        result = sql """show column cached stats `stats` (lo_shipmode)"""
        assertTrue(result[0][0] == "lo_shipmode")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "7.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "425.21")
        assertTrue(result[0][5] == "4.21")
        assertTrue(result[0][6] == "'AIR'")
        assertTrue(result[0][7] == "'TRUCK'")

        result = sql """show column cached stats `stats` (lo_quantity)"""
        assertTrue(result[0][0] == "lo_quantity")
        assertTrue(result[0][1] == "100.0")
        assertTrue(result[0][2] == "46.0")
        assertTrue(result[0][3] == "0.0")
        assertTrue(result[0][4] == "404.0")
        assertTrue(result[0][5] == "4.0")
        assertTrue(result[0][6] == "1")
        assertTrue(result[0][7] == "50")
    }
}

