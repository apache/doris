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

suite("test_hive_statistic", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = hivePrefix + "_test_hive_statistic"
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

        // TODO will be supported in future
        // Test analyze table without init.
        // sql """analyze table ${catalog_name}.tpch_1000_parquet.region with sync"""

        // logger.info("switched to catalog " + catalog_name)
        // sql """use statistics;"""
        // sql """analyze table `statistics` with sync"""
        // def result = sql """show column stats `statistics` (lo_quantity)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_quantity")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "46.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "1")
        // assertEquals(result[0][8], "50")

        // result = sql """show column stats `statistics` (lo_orderkey)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_orderkey")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "26.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "1")
        // assertEquals(result[0][8], "98")

        // result = sql """show column stats `statistics` (lo_linenumber)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_linenumber")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "7.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "1")
        // assertEquals(result[0][8], "7")

        // result = sql """show column stats `statistics` (lo_custkey)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_custkey")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "26.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "67423")
        // assertEquals(result[0][8], "2735521")

        // result = sql """show column stats `statistics` (lo_partkey)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_partkey")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "100.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "2250")
        // assertEquals(result[0][8], "989601")

        // result = sql """show column stats `statistics` (lo_suppkey)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_suppkey")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "100.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "4167")
        // assertEquals(result[0][8], "195845")

        // result = sql """show column stats `statistics` (lo_orderdate)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_orderdate")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "26.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "19920221")
        // assertEquals(result[0][8], "19980721")

        // result = sql """show column stats `statistics` (lo_orderpriority)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_orderpriority")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "5.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "880.0")
        // assertEquals(result[0][6], "8.8")
        // assertEquals(result[0][7], "'1-URGENT'")
        // assertEquals(result[0][8], "'5-LOW'")

        // result = sql """show column stats `statistics` (lo_shippriority)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_shippriority")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "1.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "0")
        // assertEquals(result[0][8], "0")

        // result = sql """show column stats `statistics` (lo_extendedprice)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_extendedprice")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "100.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "104300")
        // assertEquals(result[0][8], "9066094")

        // result = sql """show column stats `statistics` (lo_ordtotalprice)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_ordtotalprice")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "26.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "3428256")
        // assertEquals(result[0][8], "36771805")

        // result = sql """show column stats `statistics` (lo_discount)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_discount")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "11.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "0")
        // assertEquals(result[0][8], "10")

        // result = sql """show column stats `statistics` (lo_revenue)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_revenue")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "100.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "101171")
        // assertEquals(result[0][8], "8703450")

        // result = sql """show column stats `statistics` (lo_supplycost)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_supplycost")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "100.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "58023")
        // assertEquals(result[0][8], "121374")

        // result = sql """show column stats `statistics` (lo_tax)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_tax")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "9.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "0")
        // assertEquals(result[0][8], "8")

        // result = sql """show column stats `statistics` (lo_commitdate)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_commitdate")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "95.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "400.0")
        // assertEquals(result[0][6], "4.0")
        // assertEquals(result[0][7], "19920515")
        // assertEquals(result[0][8], "19981016")

        // result = sql """show column stats `statistics` (lo_shipmode)"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_shipmode")
        // assertEquals(result[0][2], "100.0")
        // assertEquals(result[0][3], "7.0")
        // assertEquals(result[0][4], "0.0")
        // assertEquals(result[0][5], "421.0")
        // assertEquals(result[0][6], "4.21")
        // assertEquals(result[0][7], "'AIR'")
        // assertEquals(result[0][8], "'TRUCK'")

        // sql """ALTER TABLE statistics MODIFY COLUMN lo_shipmode SET STATS ('row_count'='6001215')"""
        // result = sql "show column stats `statistics` (lo_shipmode)"
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][0], "lo_shipmode")
        // assertEquals(result[0][2], "6001215.0")

        // sql """drop stats statistics"""
        // result = sql """show column stats statistics"""
        // assertEquals(result.size(), 0)

        // sql """analyze database `statistics` with sync"""
        // result = sql """show table stats statistics"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][2], "100")

        // result = sql """show table cached stats statistics"""
        // assertEquals(result.size(), 1)
        // assertEquals(result[0][2], "100")

        // sql """drop stats statistics"""
        // result = sql """show column cached stats statistics"""
        // assertEquals(result.size(), 0)

        sql """use multi_catalog"""
        sql """analyze table logs1_parquet (log_time) with sync"""
        def ctlId
        def dbId
        def tblId
        def result = sql """show catalogs"""

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
        assertEquals(result[0][7], "N/A")
        assertEquals(result[0][8], "N/A")

        sql """use tpch1_parquet;"""
        sql """drop stats region"""
        sql """alter table region modify column r_comment set stats ('row_count'='5.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='330.0', 'min_value'='ges. thinly even pinto beans ca', 'max_value'='uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl');"""
        sql """alter table region modify column r_name set stats ('row_count'='5.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='34.0', 'min_value'='AFRICA', 'max_value'='MIDDLE EAST');"""
        sql """alter table region modify column r_regionkey set stats ('row_count'='5.0', 'ndv'='5.0', 'num_nulls'='0.0', 'data_size'='20.0', 'min_value'='0', 'max_value'='4');"""
        result = sql """show column stats region(r_regionkey)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_regionkey")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "20.0")
        assertEquals(result[0][6], "4.0")
        assertEquals(result[0][7], "0")
        assertEquals(result[0][8], "4")

        result = sql """show column stats region(r_comment)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_comment")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "330.0")
        assertEquals(result[0][6], "66.0")
        assertEquals(result[0][7], "\'ges. thinly even pinto beans ca\'")
        assertEquals(result[0][8], "\'uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl\'")

        result = sql """show column stats region(r_name)"""
        assertEquals(result.size(), 1)
        assertEquals(result[0][0], "r_name")
        assertEquals(result[0][2], "5.0")
        assertEquals(result[0][3], "5.0")
        assertEquals(result[0][4], "0.0")
        assertEquals(result[0][5], "34.0")
        assertEquals(result[0][6], "6.8")
        assertEquals(result[0][7], "\'AFRICA\'")
        assertEquals(result[0][8], "\'MIDDLE EAST\'")

        sql """drop catalog ${catalog_name}"""
    }
}

