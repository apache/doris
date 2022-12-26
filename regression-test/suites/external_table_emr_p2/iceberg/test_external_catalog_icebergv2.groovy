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
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_external_catalog_iceberg"

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
            qt_q01 """ select count(1) as c from customer_small;"""
            qt_q02 """ select c_custkey from customer_small group by c_custkey limit 4;"""
            qt_q03 """ select count(1) from orders """
            qt_q04 """ select count(1) from customer_small where c_name = 'Customer#0063356' order by c_custkey limit 1; """
            qt_q05 """ select * from customer_small order by c_custkey limit 3 """
            qt_q06 """ select o_orderkey from orders where o_orderkey > 652566 limit 3"""
            qt_q07 """ select o_orderkey from orders where o_custkey < 3357 limit 3"""
            qt_q08 """ select count(1) as c from customer;"""
        }
        sql """ use `tpch_1000_icebergv2`; """
        q01()
    }
}
