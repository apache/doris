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


suite("test_hive_opt_fill_partition", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix  ="hive3";
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
    
        String catalog_name = "test_hive_opt_fill_partition"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hadoop.username' = 'hadoop',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
        );
        """

        sql """ switch ${catalog_name} """
        sql """ use `default` """
        
        qt_parquet_1 """ select * from parquet_partition_multi_row_group limit 5; """
        qt_parquet_2 """ select count(col1) from parquet_partition_multi_row_group ; """
        qt_parquet_3 """ select count(col2) from parquet_partition_multi_row_group ; """
        qt_parquet_4 """ select count(col3) from parquet_partition_multi_row_group ; """
        qt_parquet_5 """ select count(partition_col1) from parquet_partition_multi_row_group ; """
        qt_parquet_6 """ select count(partition_col1) from parquet_partition_multi_row_group ; """
        qt_parquet_7 """ select col1,count(*) from parquet_partition_multi_row_group group by col1;  """
        qt_parquet_8 """ select partition_col1,count(*) from parquet_partition_multi_row_group group by partition_col1; """
        qt_parquet_9 """ select partition_col2,count(*) from parquet_partition_multi_row_group group by partition_col2; """
        qt_parquet_10 """ select * from parquet_partition_multi_row_group  where col1 = 'word' limit 5; """
        qt_parquet_11 """ select count(*) from parquet_partition_multi_row_group  where col2 != 100; """
        qt_parquet_12 """ select count(*) from parquet_partition_multi_row_group  where partition_col1 = 'hello' limit 5; """
        qt_parquet_13 """ select count(*) from parquet_partition_multi_row_group  where partition_col2 = 1 limit 5; """
        qt_parquet_14 """ select count(*) from parquet_partition_multi_row_group  where partition_col2 != 1 ; """


        qt_orc_1 """ select * from orc_partition_multi_stripe limit 5; """
        qt_orc_2 """ select count(col1) from orc_partition_multi_stripe ; """
        qt_orc_3 """ select count(col2) from orc_partition_multi_stripe ; """
        qt_orc_4 """ select count(col3) from orc_partition_multi_stripe ; """
        qt_orc_5 """ select count(partition_col1) from orc_partition_multi_stripe ; """
        qt_orc_6 """ select count(partition_col1) from orc_partition_multi_stripe ; """
        qt_orc_7 """ select col1,count(*) from orc_partition_multi_stripe group by col1;  """
        qt_orc_8 """ select partition_col1,count(*) from orc_partition_multi_stripe group by partition_col1; """
        qt_orc_9 """ select partition_col2,count(*) from orc_partition_multi_stripe group by partition_col2; """
        qt_orc_10 """ select * from orc_partition_multi_stripe  where col1 = 'word' limit 5; """
        qt_orc_11 """ select count(*) from orc_partition_multi_stripe  where col2 != 100; """
        qt_orc_12 """ select count(*) from orc_partition_multi_stripe  where partition_col1 = 'hello' limit 5; """
        qt_orc_13 """ select count(*) from orc_partition_multi_stripe  where partition_col2 = 1 limit 5; """
        qt_orc_14 """ select count(*) from orc_partition_multi_stripe  where partition_col2 != 1 ; """

    }
}