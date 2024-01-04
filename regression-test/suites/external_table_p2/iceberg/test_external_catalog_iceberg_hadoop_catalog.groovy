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

suite("test_external_catalog_iceberg_hadoop_catalog", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String iceberg_catalog_name = "test_external_iceberg_catalog_hadoop"
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHdfsPort = context.config.otherConfigs.get("extHdfsPort")
        sql """drop catalog if exists ${iceberg_catalog_name};"""
        sql """
            create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hadoop',
                'warehouse' = 'hdfs://${extHiveHmsHost}:${extHdfsPort}/usr/hive/warehouse/hadoop_catalog'
            );
        """

        sql """switch ${iceberg_catalog_name};"""
        def q01 = {
            qt_q01 """ select count(*) from iceberg_hadoop_catalog """
            qt_q02 """ select c_custkey from iceberg_hadoop_catalog group by c_custkey order by c_custkey limit 7 """
            qt_q03 """ select * from iceberg_hadoop_catalog order by c_custkey limit 3 """
        }

        def q02 = {
            qt_q04 """ select * from multi_partition2 order by val """
            qt_q05 """ select count(*) from table_with_append_file where MAN_ID is not null """
        }

        sql """ use `multi_catalog`; """
        q01()
        q02()
    }
}
