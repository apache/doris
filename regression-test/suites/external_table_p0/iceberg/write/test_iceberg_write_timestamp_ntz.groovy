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

suite("test_iceberg_write_timestamp_ntz", "p0,external,iceberg,external_docker,external_docker_iceberg") { 
    
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String iceberg_catalog_name = "test_iceberg_write_timestamp_ntz_${hivePrefix}"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${iceberg_catalog_name}"""
            sql """create catalog if not exists ${iceberg_catalog_name} properties (
                'type'='iceberg',
                'iceberg.catalog.type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'warehouse' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );"""

           

            sql """set enable_fallback_to_original_planner=false;"""          
            sql """switch ${iceberg_catalog_name};"""
            sql """use test_db;"""


            sql """INSERT INTO t_ntz_doris VALUES ('2025-02-07 20:12:00');"""
            sql """INSERT INTO t_tz_doris VALUES ('2025-02-07 20:12:00');"""

         
            explain {
                sql("select * from t_ntz_doris")
                contains "col='2025-02-07 20:12:00'"
            }


            explain {
                sql("select * from t_tz_doris")
                contains "col='2025-02-07 20:12:00'"
            } 

          
           sql """drop catalog if exists ${iceberg_catalog_name}"""
  

        } finally {

        }
    }
}
