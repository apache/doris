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

suite("test_hive_ddl_text_format", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hms_port = context.config.otherConfigs.get("hive3HmsPort")
        String hdfs_port = context.config.otherConfigs.get("hive3HdfsPort")
        String catalog_name = "test_hive_ddl_text_format"
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

        sql """ drop table if exists tb_text """
        sql """
        create table tb_text (
            id int,
            `name` string
        ) PROPERTIES (
            'compression'='gzip',
            'file_format'='text',
            'field.delim'='\t',
            'line.delim'='\n',
            'collection.delim'=';',
            'mapkey.delim'=':',
            'serialization.null.format'='\\N'
        );
        """

        String serde = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
        String input_format = "'org.apache.hadoop.mapred.TextInputFormat'"
        String output_format = "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
        String doris_fileformat = "'doris.file_format'='text'"
        String filed_delim = "'field.delim'"
        String line_delim = "'line.delim'"
        String mapkey_delim = "'mapkey.delim'"

        def create_tbl_res = sql """ show create table tb_text """
        String res = create_tbl_res.toString()
        logger.info("${res}")
        assertTrue(res.containsIgnoreCase("${serde}"))
        assertTrue(res.containsIgnoreCase("${input_format}"))
        assertTrue(res.containsIgnoreCase("${output_format}"))
        assertTrue(res.containsIgnoreCase("${doris_fileformat}"))
        assertTrue(res.containsIgnoreCase("${filed_delim}"))
        assertTrue(res.containsIgnoreCase("${filed_delim}"))
        assertTrue(res.containsIgnoreCase("${line_delim}"))
        assertTrue(res.containsIgnoreCase("${mapkey_delim}"))
    }
}
