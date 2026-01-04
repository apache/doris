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

suite("test_mixed_format_partitions", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String catalog_name = "test_${hivePrefix}_mixed_format"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}'
            );"""

            // Prepare Hive table with mixed formats
            String tableName = "mixed_format_table_${hivePrefix}"
            hive_docker """ DROP TABLE IF EXISTS ${tableName} """
            hive_docker """ 
                CREATE TABLE ${tableName} (
                    id INT,
                    name STRING
                ) 
                PARTITIONED BY (dt STRING) 
                STORED AS ORC 
            """

            // Add ORC partition (default format)
            hive_docker """ ALTER TABLE ${tableName} ADD PARTITION (dt='2023-01-01') """
            hive_docker """ INSERT INTO TABLE ${tableName} PARTITION (dt='2023-01-01') VALUES (1, 'orc_row') """

            // Add Parquet partition (explicit format)
            hive_docker """ ALTER TABLE ${tableName} ADD PARTITION (dt='2023-01-02') """
            hive_docker """ ALTER TABLE ${tableName} PARTITION (dt='2023-01-02') SET FILEFORMAT PARQUET """
            hive_docker """ INSERT INTO TABLE ${tableName} PARTITION (dt='2023-01-02') VALUES (2, 'parquet_row') """

            sql """refresh catalog ${catalog_name}"""
            sql """use ${catalog_name}.default"""

            // Query and verify
            // Should return both rows if the fix works.
            // If the fix is broken, it might fail to read the parquet partition (expecting ORC) or vice versa.
            order_qt_mixed_format """ SELECT id, name, dt FROM ${tableName} ORDER BY id """

            // Clean up
            hive_docker """ DROP TABLE IF EXISTS ${tableName} """
            sql """drop catalog if exists ${catalog_name}"""

        } catch (Exception e) {
            logger.error("Error in test_mixed_format_partitions", e)
            throw e
        }
    }
}
