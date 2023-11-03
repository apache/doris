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

suite("test_hive_statistic", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_statistic"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")

        // Test analyze table without init.
        sql """analyze table ${catalog_name}.tpch_1000_parquet.region with sync"""

        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use statistics;"""
        sql """analyze table `statistics` with sync"""
        def result = sql """show column stats `statistics` (lo_quantity)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_quantity")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "46.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "50")

        result = sql """show column stats `statistics` (lo_orderkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "98")

        result = sql """show column stats `statistics` (lo_linenumber)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_linenumber")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "1")
        assertEquals(result[0][7], "7")

        result = sql """show column stats `statistics` (lo_custkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_custkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "67423")
        assertEquals(result[0][7], "2735521")

        result = sql """show column stats `statistics` (lo_partkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_partkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "2250")
        assertEquals(result[0][7], "989601")

        result = sql """show column stats `statistics` (lo_suppkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_suppkey")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "4167")
        assertEquals(result[0][7], "195845")

        result = sql """show column stats `statistics` (lo_orderdate)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderdate")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "19920221")
        assertEquals(result[0][7], "19980721")

        result = sql """show column stats `statistics` (lo_orderpriority)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_orderpriority")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "880.0")
        assertEquals(result[0][5], "8.8")
        assertEquals(result[0][6], "'1-URGENT'")
        assertEquals(result[0][7], "'5-LOW'")

        result = sql """show column stats `statistics` (lo_shippriority)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_shippriority")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "1.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "0")

        result = sql """show column stats `statistics` (lo_extendedprice)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_extendedprice")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "104300")
        assertEquals(result[0][7], "9066094")

        result = sql """show column stats `statistics` (lo_ordtotalprice)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_ordtotalprice")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "26.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "3428256")
        assertEquals(result[0][7], "36771805")

        result = sql """show column stats `statistics` (lo_discount)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_discount")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "11.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "10")

        result = sql """show column stats `statistics` (lo_revenue)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_revenue")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "101171")
        assertEquals(result[0][7], "8703450")

        result = sql """show column stats `statistics` (lo_supplycost)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_supplycost")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "100.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "58023")
        assertEquals(result[0][7], "121374")

        result = sql """show column stats `statistics` (lo_tax)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_tax")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "9.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "0")
        assertEquals(result[0][7], "8")

        result = sql """show column stats `statistics` (lo_commitdate)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_commitdate")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "95.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "400.0")
        assertEquals(result[0][5], "4.0")
        assertEquals(result[0][6], "19920515")
        assertEquals(result[0][7], "19981016")

        result = sql """show column stats `statistics` (lo_shipmode)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_shipmode")
        assertEquals(result[0][1], "100.0")
        assertEquals(result[0][2], "7.0")
        assertEquals(result[0][3], "0.0")
        assertEquals(result[0][4], "421.0")
        assertEquals(result[0][5], "4.21")
        assertEquals(result[0][6], "'AIR'")
        assertEquals(result[0][7], "'TRUCK'")

        sql """ALTER TABLE statistics MODIFY COLUMN lo_shipmode SET STATS ('row_count'='6001215')"""
        result = sql "show column stats `statistics` (lo_shipmode)"
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "lo_shipmode")
        assertEquals(result[0][1], "6001215.0")

        sql """drop stats statistics"""
        result = sql """show column stats statistics"""
        assertEquals(result.size(), 0)

        sql """analyze database `statistics` with sync"""
        result = sql """show table stats statistics"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][2], "100")

        result = sql """show table cached stats statistics"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][2], "100")

        sql """drop stats statistics"""
        result = sql """show column cached stats statistics"""
        assertEquals(result.size(), 0)

        sql """use multi_catalog"""
        sql """analyze table logs1_parquet (log_time) with sync"""
        def ctlId
        def dbId
        def tblId
        result = sql """show catalogs"""

        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == catalog_name) {
                ctlId = result[i][0]
            }
        }
        result = sql """show proc '/catalogs/$ctlId'"""
        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == 'multi_catalog') {
                dbId = result[i][0]
            }
        }
        result = sql """show proc '/catalogs/$ctlId/$dbId'"""
        for (int i = 0; i < result.size(); i++) {
            if (result[i][1] == 'logs1_parquet') {
                tblId = result[i][0]
            }
        }

        result = sql """select * from internal.__internal_schema.column_statistics where id = '${tblId}--1-log_time'"""
        assertEquals(result.size(), 1)
        def id = result[0][0]
        def catalog_id = result[0][1]
        def db_id = result[0][2]
        def tbl_id = result[0][3]
        def idx_id = result[0][4]
        def col_id = result[0][5]
        def count = result[0][7]
        def ndv = result[0][8]
        def null_count = result[0][9]
        def data_size_in_bytes = result[0][12]
        def update_time = result[0][13]

        sql """insert into internal.__internal_schema.column_statistics values ('$id', '$catalog_id', '$db_id', '$tbl_id', '$idx_id', '$col_id', NULL, $count, $ndv, $null_count, '', '', '$data_size_in_bytes', '$update_time')"""

        result = sql """show column stats logs1_parquet (log_time)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][6], "N/A")
        assertEquals(result[0][7], "N/A")
        sql """drop catalog ${catalog_name}"""
    }
}

