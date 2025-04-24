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

suite("test_hive_get_schema_from_table", "external_docker,hive,external_docker_hive,p0,external") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    // test get scheam from table
    for (String hivePrefix : ["hive2", "hive3"]) {
       String catalog_name = "test_${hivePrefix}_get_schema"
       String ex_db_name = "`default`"
       String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
       String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
       String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

       sql """drop catalog if exists ${catalog_name} """

       sql """CREATE CATALOG ${catalog_name} PROPERTIES (
           'type'='hms',
           'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
           'hadoop.username' = 'hive',
           'get_schema_from_table' = 'true'
       );"""

       sql """switch ${catalog_name}"""

       def res_dbs_log = sql "show databases;"
       for (int i = 0; i < res_dbs_log.size(); i++) {
           def tbs = sql "show tables from  `${res_dbs_log[i][0]}`"
           log.info("database = ${res_dbs_log[i][0]} => tables = " + tbs.toString())
       }

       order_qt_schema_1 """select * from ${catalog_name}.${ex_db_name}.parquet_partition_table order by l_orderkey limit 1;"""
       order_qt_schema_2 """select * from ${catalog_name}.${ex_db_name}.parquet_delta_binary_packed order by int_value limit 1;"""
       order_qt_schema_3 """select * from ${catalog_name}.${ex_db_name}.parquet_alltypes_tiny_pages  order by id desc  limit 5;"""
       order_qt_schema_4 """select * from ${catalog_name}.${ex_db_name}.orc_all_types_partition order by bigint_col desc limit 3;"""
       order_qt_schema_5 """select * from ${catalog_name}.${ex_db_name}.csv_partition_table order by k1 limit 1;"""
       order_qt_schema_6 """select * from ${catalog_name}.${ex_db_name}.csv_all_types limit 1;"""
       order_qt_schema_7 """select * except(t_varchar_max_length) from ${catalog_name}.${ex_db_name}.text_all_types limit 1;"""

       //sql """drop catalog if exists ${catalog_name} """
    
    }
}

