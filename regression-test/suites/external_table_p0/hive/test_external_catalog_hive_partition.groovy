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

suite("test_external_catalog_hive_partition", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }
    for (String hivePrefix : ["hive2", "hive3"]) {
        String extHiveHmsHost = context.config.otherConfigs.get("externalEnvIp")
        String extHiveHmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_external_catalog_hive_partition"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """switch ${catalog_name};"""
        // test parquet format
        def q01_parquet = {
            qt_q01 """ select * from multi_catalog.parquet_partitioned_one_column order by t_float """
            qt_q02 """ select * from multi_catalog.parquet_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from multi_catalog.parquet_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from multi_catalog.parquet_partitioned_columns order by t_float """
            qt_q05 """ select * from multi_catalog.parquet_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from multi_catalog.parquet_partitioned_columns where t_int is not null order by t_float """
            //qt_q07 """ select  o_orderyear, o_orderkey, o_custkey from multi_catalog.orders_par_parquet where o_custkey=1820677 order by o_orderkey """
        }
        // test orc format
        def q01_orc = {
            qt_q01 """ select * from multi_catalog.orc_partitioned_one_column order by t_float """
            qt_q02 """ select * from multi_catalog.orc_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from multi_catalog.orc_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from multi_catalog.orc_partitioned_columns order by t_float """
            qt_q05 """ select * from multi_catalog.orc_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from multi_catalog.orc_partitioned_columns where t_int is not null order by t_float """
            //qt_q07 """ select  o_orderyear, o_orderkey, o_custkey from multi_catalog.orders_par_orc where o_custkey=1820677 order by o_orderkey """
        }
        // test text format
        def q01_text = {
            qt_q01 """ select * from multi_catalog.text_partitioned_one_column order by t_float """
            qt_q02 """ select * from multi_catalog.text_partitioned_one_column where t_int is null order by t_float """
            qt_q03 """ select * from multi_catalog.text_partitioned_one_column where t_int is not null order by t_float """
            qt_q04 """ select * from multi_catalog.text_partitioned_columns order by t_float """
            qt_q05 """ select * from multi_catalog.text_partitioned_columns where t_int is null order by t_float """
            qt_q06 """ select * from multi_catalog.text_partitioned_columns where t_int is not null order by t_float """
        }
        sql """ use `multi_catalog`; """
        q01_parquet()
        q01_orc()
        q01_text()
    }
}

