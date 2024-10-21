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
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try{
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
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

            sql """ drop table if exists text_table_default_properties """
            sql """
            create table text_table_default_properties (
                id int,
                `name` string,
                tags array<string>,
                attributes map<string, string>
            ) PROPERTIES (
                'file_format'='text'
            );
            """
            sql """
            INSERT INTO text_table_default_properties VALUES
                (1, 'Alice', array('tag1', 'tag2'), map('key1', 'value1', 'key2', 'value2')),
                (2, 'Bob', array('tagA', 'tagB'), map('keyA', 'valueA', 'keyB', 'valueB')),
                (3, 'Charlie', NULL, map('keyC', 'valueC', 'keyD', 'valueD'));
            """
            order_qt_default_properties """ select * from text_table_default_properties """

            order_qt_hive_docker_default_properties""" select * from text_table_default_properties """

            sql """ drop table if exists text_table_standard_properties """
            // Escape characters need to be considered in groovy scripts
            sql """
            create table text_table_standard_properties (
                id int,
                `name` string,
                tags array<string>,
                attributes map<string, string>
            ) PROPERTIES (
                'compression'='plain',
                'file_format'='text',
                'field.delim'='\\1',
                'line.delim'='\\n',
                'collection.delim'='\\2',
                'mapkey.delim'='\\3',
                'escape.delim'= '\\\\',
                'serialization.null.format'='\\\\N'
            );
            """
            sql """
            INSERT INTO text_table_standard_properties VALUES
                (1, 'Alice', array('tag1', 'tag2'), map('key1', 'value1', 'key2', 'value2')),
                (2, 'Bob', array('tagA', 'tagB'), map('keyA', 'valueA', 'keyB', 'valueB')),
                (3, 'Charlie', NULL, map('keyC', 'valueC', 'keyD', 'valueD'));
            """
            order_qt_standard_properties """ select * from text_table_standard_properties """
            order_qt_hive_docker_standard_properties """ select * from text_table_standard_properties order by id; """

            sql """ drop table if exists text_table_different_properties """
            sql """
            create table text_table_different_properties (
                id int,
                `name` string,
                tags array<string>,
                attributes map<string, string>
            ) PROPERTIES (
                'compression'='gzip',
                'file_format'='text',
                'field.delim'='A',
                'line.delim'='\\4',
                'collection.delim'=',',
                'mapkey.delim'=':',
                'escape.delim'='|',
                'serialization.null.format'='null'
            );
            """
            sql """
            INSERT INTO text_table_different_properties VALUES
                (1, 'Alice', array('tag1', 'tag2'), map('key1', 'value1', 'key2', 'value2')),
                (2, 'Bob', array('tagA', 'tagB'), map('keyA', 'valueA', 'keyB', 'valueB')),
                (3, 'Charlie', NULL, map('keyC', 'valueC', 'keyD', 'valueD'));
            """
            order_qt_different_properties """ select * from text_table_different_properties """
            order_qt_hive_docker_different_properties """ select * from text_table_different_properties order by id; """

            String serde = "'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
            String input_format = "'org.apache.hadoop.mapred.TextInputFormat'"
            String output_format = "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'"
            String doris_fileformat = "'doris.file_format'='text'"
            String filed_delim = "'field.delim'"
            String line_delim = "'line.delim'"
            String mapkey_delim = "'mapkey.delim'"
            String collection_delim = "'collection.delim'"
            String escape_delim = "'escape.delim'"
            String serialization_null_format = "'serialization.null.format'"

            def create_tbl_res = sql """ show create table text_table_standard_properties """
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
            assertTrue(res.containsIgnoreCase("${collection_delim}"))
            assertTrue(res.containsIgnoreCase("${escape_delim}"))
            assertTrue(res.containsIgnoreCase("${serialization_null_format}"))
        } finally {
        }
    }
}
