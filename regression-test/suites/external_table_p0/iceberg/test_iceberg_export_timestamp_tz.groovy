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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_iceberg_export_timestamp_tz", "external,hive,external_docker") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2"]) {
        setHivePrefix(hivePrefix)
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        // It's okay to use random `hdfsUser`, but can not be empty.
        def hdfsUserName = "doris"
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
        def outfile_path = "/user/doris/tmp_data"
        def uri = "${defaultFS}" + "${outfile_path}/exp_"

        def outfile_to_HDFS = {format,export_table_name, enable_int96_timestamps ->
            // select ... into outfile ...
            def uuid = UUID.randomUUID().toString()
            outfile_path = "/user/doris/tmp_data/${uuid}"
            uri = "${defaultFS}" + "${outfile_path}/exp_"

            def res = sql """
                SELECT * FROM ${export_table_name} t ORDER BY id
                INTO OUTFILE "${uri}"
                FORMAT AS ${format}
                PROPERTIES (
                    "fs.defaultFS"="${defaultFS}",
                    "hadoop.username" = "${hdfsUserName}",
                    "enable_int96_timestamps"="${enable_int96_timestamps}"
                );
            """
            logger.info("outfile success path: " + res[0][3]);
            return res[0][3]
        }

        try {
            String catalog_name_with_export = "test_iceberg_timestamp_tz_with_mapping_export"
            String db_name = "test_timestamp_tz"
            String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
            String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
            sql """set time_zone = 'Asia/Shanghai';"""
            
            sql """drop catalog if exists ${catalog_name_with_export}"""
            sql """
            CREATE CATALOG ${catalog_name_with_export} PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'uri' = 'http://${externalEnvIp}:${rest_port}',
                "s3.access_key" = "admin",
                "s3.secret_key" = "password",
                "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
                "s3.region" = "us-east-1",
                "s3.path.style.access" = "true",
                "s3.connection.ssl.enabled" = "false",
                "enable.mapping.varbinary"="true",
                "enable.mapping.timestamp_tz"="true"
            );"""

            sql """switch ${catalog_name_with_export}"""
            sql """use ${db_name}"""
            order_qt_select_desc_orc """ desc test_ice_timestamp_tz_orc; """
            order_qt_select_desc_parquet """ desc test_ice_timestamp_tz_parquet; """

            def format = "parquet"
            def export_table_name = "test_ice_timestamp_tz_parquet"

            def outfile_url0 = outfile_to_HDFS(format, export_table_name, "true")
            order_qt_select_tvf0 """ select * from HDFS(
                        "uri" = "${outfile_url0}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf0_desc """ desc function HDFS(
                        "uri" = "${outfile_url0}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """

            def outfile_url0_false = outfile_to_HDFS(format, export_table_name, "false")
            order_qt_select_tvf0_false """ select * from HDFS(
                        "uri" = "${outfile_url0}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf0_desc_false """ desc function HDFS(
                        "uri" = "${outfile_url0}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """

            format = "parquet"
            export_table_name = "test_ice_timestamp_tz_orc"
            def outfile_url1 = outfile_to_HDFS(format, export_table_name, "true")
            order_qt_select_tvf1 """ select * from HDFS(
                        "uri" = "${outfile_url1}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf1_desc """ desc function HDFS(
                        "uri" = "${outfile_url1}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """

            def outfile_url1_false = outfile_to_HDFS(format, export_table_name, "false")
            order_qt_select_tvf1_false """ select * from HDFS(
                        "uri" = "${outfile_url1}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf1_desc_false """ desc function HDFS(
                        "uri" = "${outfile_url1}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_timestamp_tz"="true",
                        "enable_mapping_varbinary"="true",
                        "format" = "${format}");
                        """

            format = "orc"
            export_table_name = "test_ice_timestamp_tz_parquet"
            def outfile_url2 = outfile_to_HDFS(format, export_table_name, "true")
            order_qt_select_tvf2 """ select * from HDFS(
                        "uri" = "${outfile_url2}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_varbinary"="true",
                        "enable_mapping_timestamp_tz"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf2_desc """ desc function HDFS(
                        "uri" = "${outfile_url2}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_varbinary"="true",
                        "enable_mapping_timestamp_tz"="true",
                        "format" = "${format}");
                        """


            format = "orc"
            export_table_name = "test_ice_timestamp_tz_orc"
            def outfile_url3 = outfile_to_HDFS(format, export_table_name, "true")
            order_qt_select_tvf3 """ select * from HDFS(
                        "uri" = "${outfile_url3}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_varbinary"="true",
                        "enable_mapping_timestamp_tz"="true",
                        "format" = "${format}");
                        """
            order_qt_select_tvf3_desc """ desc function HDFS(
                        "uri" = "${outfile_url3}.${format}",
                        "hadoop.username" = "${hdfsUserName}",
                        "enable_mapping_varbinary"="true",
                        "enable_mapping_timestamp_tz"="true",
                        "format" = "${format}");
                        """
        } finally {
        }
    }
}
