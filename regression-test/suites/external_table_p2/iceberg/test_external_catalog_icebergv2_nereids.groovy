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

suite("test_external_catalog_icebergv2_nereids", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String hms_catalog_name = "test_external_hms_catalog_iceberg_nereids"
        String iceberg_catalog_name = "test_external_iceberg_catalog_nereids"

        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            create catalog if not exists ${hms_catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """drop catalog if exists ${iceberg_catalog_name};"""
        sql """
            create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        sql """set enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""

        sql """switch ${hms_catalog_name};"""
        // test parquet format format
        def q01 = {
            qt_q01 """ select count(1) as c from customer_small """
            qt_q02 """ select c_custkey from customer_small group by c_custkey order by c_custkey limit 4 """
            qt_q03 """ select count(1) from orders_small """
            qt_q04 """ select count(1) from customer_small where c_name = 'Customer#000000005' or c_name = 'Customer#000000006' """
            qt_q05 """ select * from customer_small order by c_custkey limit 3 """
            qt_q06 """ select o_orderkey from orders_small where o_orderkey > 652566 order by o_orderkey limit 3 """
            qt_q07 """ select o_totalprice from orders_small where o_custkey < 3357 order by o_custkey limit 3 """
            qt_q08 """ select count(1) as c from customer """
        }
        // test time travel stmt
        def q02 = {
            qt_q09 """ select c_custkey from customer for time as of '2022-12-27 10:21:36' order by c_custkey limit 3 """
            qt_q10 """ select c_custkey from customer for time as of '2022-12-28 10:21:36' order by c_custkey desc limit 3 """
            qt_q11 """ select c_custkey from customer for version as of 906874575350293177 order by c_custkey limit 3 """
            qt_q12 """ select c_custkey from customer for version as of 6352416983354893547 order by c_custkey desc limit 3 """
        }
        // in predicate
        def q03 = {
            qt_q13 """ select c_custkey from customer_small where c_custkey in (1, 2, 4, 7) order by c_custkey """
            qt_q14 """ select c_name from customer_small where c_name in ('Customer#000000004', 'Customer#000000007') order by c_custkey """
        }

        // test for 'FOR TIME AS OF' and 'FOR VERSION AS OF'
        def q04 = {
            qt_q15 """ select count(*) from ${hms_catalog_name}.tpch_1000_icebergv2.customer_small FOR TIME AS OF '2022-12-22 02:29:30' """
            qt_q16 """ select count(*) from ${hms_catalog_name}.tpch_1000_icebergv2.customer_small FOR VERSION AS OF 6113938156088124425 """
            qt_q17 """ select count(*) from ${iceberg_catalog_name}.tpch_1000_icebergv2.customer_small FOR TIME AS OF '2022-12-22 02:29:30' """
            qt_q18 """ select count(*) from ${iceberg_catalog_name}.tpch_1000_icebergv2.customer_small FOR VERSION AS OF 6113938156088124425 """
        }
        
        sql """ use `tpch_1000_icebergv2`; """
        q01()
        q02()
        q03()
        q04()
    }
}
