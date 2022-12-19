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

suite("test_external_catalog_icebergv2", "p2") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
            String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
            String catalog_name = "test_external_catalog_iceberg"

            sql """admin set frontend config ("enable_multi_catalog" = "true")"""
            sql """drop catalog if exists ${catalog_name};"""
            sql """
                create catalog if not exists ${catalog_name} properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
                );
            """

            sql """switch ${catalog_name};"""
            // test parquet format format
            def q01 = {
                qt_q01 """ select count(1) as c from customer;"""
                qt_q02 """ select * from test1 order by col_1;"""
                qt_q03 """ select count(1) from nation """
                qt_q04 """ select count(1) from orders """
                qt_q05 """ select p_name from part where p_partkey = 4438130 order by p_name limit 1; """
                qt_q06 """ select ps_supplycost from partsupp where ps_partkey = 199588198 and ps_suppkey = 9588199 and ps_availqty = 2949 """
                qt_q07 """ select * from region order by r_regionkey limit 3 """
                qt_q08 """ select s_address from supplier where s_suppkey = 2823947 limit 3"""
            }
            sql """ use `tpch_1000_icebergv2`; """
            q01()
        } finally {
            // sql """admin set frontend config ("enable_multi_catalog" = "false")"""
        }
    }
}
