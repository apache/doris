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

suite("test_parquet_bloom_filter", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (!"true".equalsIgnoreCase(enabled)) {
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "test_parquet_bloom_filter"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use multi_catalog """ 
	
       	order_qt_sql1 """ select * from parquet_bloom_filter; """
        order_qt_sql2 """ select * from parquet_bloom_filter where tinyint_col = 5; """ 
        order_qt_sql3 """ select * from parquet_bloom_filter where smallint_col = 500; """ 
        order_qt_sql4 """ select * from parquet_bloom_filter where int_col = 7; """ 
        order_qt_sql5 """ select * from parquet_bloom_filter where bigint_col = 30000; """ 
        order_qt_sql6 """ select * from parquet_bloom_filter where float_col = cast(51.421917 as float); """ 
        order_qt_sql7 """ select * from parquet_bloom_filter where double_col = 941.7540673462295;""" 
        order_qt_sql8 """ select * from parquet_bloom_filter where decimal_col = 389.53; """ 
        order_qt_sql9 """ select * from parquet_bloom_filter where name = 'User_4'; """ 
        order_qt_sql10 """ select * from parquet_bloom_filter where is_active = false; """ 
        order_qt_sql11 """ select * from parquet_bloom_filter where created_date = '2024-01-03'; """ 
        order_qt_sql12 """ select * from parquet_bloom_filter where last_login = '2025-11-18 14:18:13.927333'; """ 
        order_qt_sql13 """ select * from parquet_bloom_filter where numeric_array[2] = 2.2; """ 
        order_qt_sql14 """ select * from parquet_bloom_filter where struct_element(address_info, "city") = 'city_7';"""
        order_qt_sql15 """ select * from parquet_bloom_filter where metrics['score'] = '33.546909893506374'; """

        sql """drop catalog ${catalog_name};"""
    }
}
