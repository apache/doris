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

suite("iceberg_equality_delete", "p2,external,iceberg,external_remote,external_remote_iceberg") {

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {

        String catalog_name = "test_external_iceberg_equality_delete"
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHdfsPort = context.config.otherConfigs.get("extHdfsPort")
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hadoop',
                'warehouse' = 'hdfs://${extHiveHmsHost}:${extHdfsPort}/usr/hive/warehouse/hadoop_catalog'
            );
        """

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """ use multi_catalog;""" 

        // one delete column
        qt_one_delete_column """select * from customer_flink_one order by c_custkey"""
        qt_one_delete_column_orc """select * from customer_flink_one_orc order by c_custkey"""
        qt_count1 """select count(*) from  customer_flink_one"""
        qt_count1_orc """select count(*) from  customer_flink_one_orc"""
        qt_max1 """select max(c_comment) from customer_flink_one"""
        qt_max1_orc """select max(c_comment) from customer_flink_one_orc"""
        // three delete columns
        qt_one_delete_column """select * from customer_flink_three order by c_custkey"""
        qt_one_delete_column_orc """select * from customer_flink_three_orc order by c_custkey"""
        qt_count3 """select count(*) from  customer_flink_three"""
        qt_count3_orc """select count(*) from  customer_flink_three_orc"""
        qt_max3 """select max(c_comment) from customer_flink_three"""
        qt_max3_orc """select max(c_comment) from customer_flink_three_orc"""

        sql """drop catalog ${catalog_name}"""
    }
}
