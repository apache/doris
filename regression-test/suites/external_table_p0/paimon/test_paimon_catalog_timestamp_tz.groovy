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

suite("test_paimon_catalog_timestamp_tz", "p0,external,doris,external_docker,external_docker_doris,new_catalog_property") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String catalog_name_with_mapping = "test_paimon_timestamp_tz_with_mapping"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name_with_mapping}"""
        sql """create catalog if not exists ${catalog_name_with_mapping} properties (
                "type" = "paimon",
                "paimon.catalog.type"="filesystem",
                "enable.mapping.varbinary"="true",
                "enable.mapping.timestamp_tz"="true",
                "warehouse" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1"
            );"""
        sql """set time_zone = 'Asia/Shanghai';"""
        // with mapping
        sql """use `${catalog_name_with_mapping}`.`db1`"""
        sql """ set force_jni_scanner=true; """
        explain {
            sql "select * from t_ltz order by id;"
            contains "paimonNativeReadSplits=0/1"
        }
        order_qt_desc_1 """ desc t_ltz; """
        qt_jni_1 """ select * from t_ltz order by id; """
        qt_jni_1_cast """ select id, cast(ts_ltz as string) from t_ltz order by id; """
        sql """ set force_jni_scanner=false; """
        explain {
            sql "select * from t_ltz order by id;"
            contains "paimonNativeReadSplits=2/2"
        }
        order_qt_desc_2 """ desc t_ltz; """
        qt_native_1 """ select * from t_ltz order by id; """
        qt_native_1_cast """ select id, cast(ts_ltz as string) from t_ltz order by id; """

        // with mapping
        qt_mapping_tz """ 
            select * from hdfs(
            "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1/db1.db/t_ltz/bucket-0/*.parquet",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:${hdfs_port}",
            "hadoop.username" = "doris",
            "enable_mapping_varbinary" = "true",
            "enable_mapping_timestamp_tz" = "true",
            "format" = "parquet") order by id;
        """

        order_qt_mapping_tz_desc """ 
            desc function hdfs(
            "uri" = "hdfs://${externalEnvIp}:${hdfs_port}/user/doris/paimon1/db1.db/t_ltz/bucket-0/*.parquet",
            "fs.defaultFS" = "hdfs://${externalEnvIp}:${hdfs_port}",
            "hadoop.username" = "doris",
            "enable_mapping_varbinary" = "true",
            "enable_mapping_timestamp_tz" = "true",
            "format" = "parquet");
        """
    }
}



