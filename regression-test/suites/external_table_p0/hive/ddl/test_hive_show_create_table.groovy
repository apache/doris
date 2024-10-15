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

suite("test_hive_show_create_table", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hms_port = context.config.otherConfigs.get("hive2HmsPort")
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String catalog_name = "test_hive_show_create_table"
        String table_name = "table_with_pars";

        sql """drop catalog if exists ${catalog_name};"""

        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """use `default`;"""

        def serde = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        def serdeFormat = "'serialization.format' = '|'"
        def filedDelim = "'field.delim' = '|'"
        def inputFormat = "'org.apache.hadoop.mapred.TextInputFormat'"
        def outputFormat = "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        def create_tbl_res = sql """ show create table table_with_pars """
        logger.info("${create_tbl_res}")
        assertTrue(create_tbl_res.toString().containsIgnoreCase("${serde}"))
        assertTrue(create_tbl_res.toString().containsIgnoreCase("${serdeFormat}"))
        assertTrue(create_tbl_res.toString().containsIgnoreCase("${filedDelim}"))
        assertTrue(create_tbl_res.toString().containsIgnoreCase("${inputFormat}"))
        assertTrue(create_tbl_res.toString().containsIgnoreCase("${outputFormat}"))
    }
}
