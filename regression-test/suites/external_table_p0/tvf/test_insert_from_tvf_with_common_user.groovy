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

suite("test_insert_from_tvf_with_common_user", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def export_table_name = "insert_from_tvf_common_user_export_test"
    def load_table_name = "insert_from_tvf_common_user_load_test"
    def outfile_path = "${bucket}/outfile/insert_from_tvf/common_user/exp_"
    def common_user = "insert_from_tvf_common_user"

    def create_table = {table_name ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
            CREATE TABLE `${table_name}` (
                `id` bigint(20) NULL,
                `name` bigint(20) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            );
        """
    }

    def outfile_to_S3 = {
        // select ... into outfile ...
        def res = sql """
            SELECT * FROM ${export_table_name} t
            INTO OUTFILE "s3://${outfile_path}"
            FORMAT AS csv
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """

        return res[0][3]
    }

    // create table to export data
    create_table(export_table_name)
    // insert data
    sql """ insert into ${export_table_name} values (1, 1); """
    sql """ insert into ${export_table_name} values (2, 2); """
    // test base data
    order_qt_select_base """ SELECT * FROM ${export_table_name} ORDER BY id; """
    // outfile to s3
    def outfile_url = outfile_to_S3()

    // create table to load data
    create_table(load_table_name)


    // create user and grant policy
    sql """drop user if exists ${common_user}"""
    sql """create user ${common_user} identified by '12345'"""
    sql """GRANT SELECT_PRIV,LOAD_PRIV,ALTER_PRIV,CREATE_PRIV,DROP_PRIV ON *.*.* TO '${common_user}'@'%';"""

    connect(user = "${common_user}", password = '12345', url = context.config.jdbcUrl) {
        sql """ use regression_test_external_table_p0_tvf """

        sql """ INSERT INTO ${load_table_name}
                SELECT * FROM S3 (
                    "uri" = "http://${s3_endpoint}${outfile_url.substring(4)}0.csv",
                    "ACCESS_KEY"= "${ak}",
                    "SECRET_KEY" = "${sk}",
                    "format" = "csv",
                    "column_separator" = "\t",
                    "region" = "${region}"
                );
            """

        order_qt_select_base """ SELECT * FROM ${load_table_name} t ORDER BY id; """
    }

    sql """drop user ${common_user}"""

}
