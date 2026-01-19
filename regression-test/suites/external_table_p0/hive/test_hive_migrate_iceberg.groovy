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

suite("test_hive_migrate_iceberg", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        try {
            String catalog_name = "${hivePrefix}_test_hive_migrate_iceberg"
            sql """drop catalog if exists ${catalog_name}"""

            sql """create catalog if not exists ${catalog_name} properties (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'hms',
                'hive.metastore.uris' = "thrift://127.0.0.1:9383"
            );"""
            sql """use `${catalog_name}`.`multi_catalog`"""

            qt_q1 """ select * from hive_parquet_migrate_iceberg order by new_id;"""
            qt_q2 """ select * from hive_parquet_migrate_iceberg order by new_id limit 1; """
            qt_q3 """ select * from hive_parquet_migrate_iceberg order by new_id limit 5; """
            qt_q4 """ select dt,metrics,user_info,new_id from hive_parquet_migrate_iceberg order by new_id;"""



            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
