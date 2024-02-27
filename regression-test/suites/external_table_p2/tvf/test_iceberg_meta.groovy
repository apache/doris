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

suite("test_iceberg_meta", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String iceberg_catalog_name = "test_iceberg_meta_tvf"
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHdfsPort = context.config.otherConfigs.get("extHdfsPort")
        String db = "multi_catalog"
        sql """drop catalog if exists ${iceberg_catalog_name};"""
        sql """
            create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hadoop',
                'warehouse' = 'hdfs://${extHiveHmsHost}:${extHdfsPort}/usr/hive/warehouse/hadoop_catalog'
            );
        """

        sql """switch ${iceberg_catalog_name};"""
        sql """ use `${db}`; """

        order_qt_q01 """ select count(*) from iceberg_hadoop_catalog """
        order_qt_q02 """ select c_custkey from iceberg_hadoop_catalog group by c_custkey order by c_custkey limit 7 """

        order_qt_tvf_1 """ select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                            "table" = "${iceberg_catalog_name}.${db}.multi_partition",
                            "query_type" = "snapshots");
                        """

        order_qt_tvf_2 """ select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                            "table" = "${iceberg_catalog_name}.${db}.multi_partition",
                            "query_type" = "snapshots")
                            where snapshot_id = 7235593032487457798;
                        """
    }
}