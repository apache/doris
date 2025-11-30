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


suite("test_hive_varbinary_type","p0,external,tvf,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2","hive3"]) {
    
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name_no_mapping = "${hivePrefix}_test_varbinary_no_mapping"
        String catalog_name_with_mapping = "${hivePrefix}_test_varbinary_with_mapping"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def hdfsUserName = "doris"
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"

        sql """drop catalog if exists ${catalog_name_no_mapping}"""
        sql """create catalog if not exists ${catalog_name_no_mapping} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        
        sql """drop catalog if exists ${catalog_name_with_mapping}"""
        sql """create catalog if not exists ${catalog_name_with_mapping} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
            "enable.mapping.varbinary"="true"
        );"""

        // no mapping
        sql """ switch ${catalog_name_no_mapping}"""
        sql """ use `test_varbinary` """
        qt_select1 """ select * from test_hive_binary_orc order by id; """
        qt_select2 """ select * from test_hive_binary_parquet order by id; """
        qt_select3 """ select * from test_hive_uuid_fixed_orc order by id; """
        qt_select4 """ select * from test_hive_uuid_fixed_parquet order by id; """
        qt_select5 """ select * from test_hive_binary_edge_cases order by id; """

        // write orc
        qt_select6 """ insert into test_hive_binary_orc_write_no_mapping select * from test_hive_binary_orc; """
        qt_select7 """ insert into test_hive_binary_orc_write_no_mapping values(6,X"ABAB",X"ABAB"); """
        qt_select8 """ insert into test_hive_binary_orc_write_no_mapping values(NULL,NULL,NULL); """
        qt_select9 """ select * from test_hive_binary_orc_write_no_mapping order by id; """

        // write parquet
        qt_select10 """ insert into test_hive_binary_parquet_write_no_mapping select * from test_hive_binary_parquet; """
        qt_select11 """ insert into test_hive_binary_parquet_write_no_mapping values(6,X"ABAB",X"ABAB"); """
        qt_select12 """ insert into test_hive_binary_parquet_write_no_mapping values(NULL,NULL,NULL); """
        qt_select13 """ select * from test_hive_binary_parquet_write_no_mapping order by id; """

        // with mapping
        sql """ switch ${catalog_name_with_mapping} """
        sql """ use `test_varbinary` """
        qt_select14 """ select * from test_hive_binary_orc order by id; """
        qt_select15 """ select * from test_hive_binary_parquet order by id; """
        qt_select16 """ select * from test_hive_uuid_fixed_orc order by id; """
        qt_select17 """ select * from test_hive_uuid_fixed_parquet order by id; """
        qt_select18 """ select * from test_hive_binary_edge_cases order by id; """

        // write orc
        qt_select19 """ insert into test_hive_binary_orc_write_with_mapping select * from test_hive_binary_orc; """
        qt_select20 """ insert into test_hive_binary_orc_write_with_mapping values(6,X"ABAB",X"ABAB"); """
        qt_select21 """ insert into test_hive_binary_orc_write_with_mapping values(NULL,NULL,NULL); """
        qt_select22 """ select * from test_hive_binary_orc_write_with_mapping order by id; """

        // write parquet
        qt_select23 """ insert into test_hive_binary_parquet_write_with_mapping select * from test_hive_binary_parquet; """
        qt_select24 """ insert into test_hive_binary_parquet_write_with_mapping values(6,X"ABAB",X"ABAB"); """
        qt_select25 """ insert into test_hive_binary_parquet_write_with_mapping values(NULL,NULL,NULL); """
        qt_select26 """ select * from test_hive_binary_parquet_write_with_mapping order by id; """
    }

}