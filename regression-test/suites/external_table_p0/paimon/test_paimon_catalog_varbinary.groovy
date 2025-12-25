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

suite("test_paimon_catalog_varbinary", "p0,external,doris,external_docker,external_docker_doris,new_catalog_property") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String catalog_name_no_mapping = "test_paimon_catalog_varbinary_no_mapping"
        String catalog_name_with_mapping = "test_paimon_catalog_varbinary_with_mapping"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name_no_mapping}"""
        sql """create catalog if not exists ${catalog_name_no_mapping} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""

        sql """drop catalog if exists ${catalog_name_with_mapping}"""
        sql """create catalog if not exists ${catalog_name_with_mapping} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "enable.mapping.varbinary"="true",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""
        // no mapping
        sql """use `${catalog_name_no_mapping}`.`db1`"""
        sql """ set force_jni_scanner=true; """
        qt_varbinary_1 """ select * from binary_demo3 order by id; """
        explain {
            sql "select * except(binary_data),length(binary_data) from binary_size_test order by test_id;"
            contains "paimonNativeReadSplits=0/1"
        }
        qt_varbinary_2 """ select * except(binary_data),length(binary_data) from binary_size_test order by test_id; """
        
        sql """ set force_jni_scanner=false; """
        explain {
            sql "select * except(binary_data),length(binary_data) from binary_size_test order by test_id;"
            contains "paimonNativeReadSplits=1/1"
        }
        qt_varbinary_3 """ select * except(binary_data),length(binary_data) from binary_size_test order by test_id; """
        
        // with mapping
        sql """use `${catalog_name_with_mapping}`.`db1`"""
        sql """ set force_jni_scanner=true; """
        qt_varbinary_4 """ select * from binary_demo3 order by id; """
        explain {
            sql "select * except(binary_data),length(binary_data) from binary_size_test order by test_id;"
            contains "paimonNativeReadSplits=0/1"
        }
        qt_varbinary_5 """ select * except(binary_data),length(binary_data) from binary_size_test order by test_id; """
        
        sql """ set force_jni_scanner=false; """
        explain {
            sql "select * except(binary_data),length(binary_data) from binary_size_test order by test_id;"
            contains "paimonNativeReadSplits=1/1"
        }
        qt_varbinary_6 """ select * except(binary_data),length(binary_data) from binary_size_test order by test_id; """
        

        // no mapping
        qt_varbinary_7 """ 
            select * from hdfs(
            "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1/db1.db/binary_demo3/bucket-0/data-01367323-fe57-4cf2-8d63-658136eef42a-0.parquet",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:${hdfs_port}",
            "hadoop.username" = "doris",
            "format" = "parquet");
        """

        // with mapping
        qt_varbinary_8 """ 
            select * from hdfs(
            "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1/db1.db/binary_demo3/bucket-0/data-01367323-fe57-4cf2-8d63-658136eef42a-0.parquet",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:${hdfs_port}",
            "hadoop.username" = "doris",
            "enable_mapping_varbinary" = "true",
            "format" = "parquet");
        """
    }
}



