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

suite("test_lower_case_mtmv", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_lower_case_mtmv_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    sql """drop database if exists internal.EXTERNAL_LOWER_MTMV; """
    sql """create database if not exists internal.EXTERNAL_LOWER_MTMV;"""
    sql """create table if not exists internal.EXTERNAL_LOWER_MTMV.TABLE_TEST
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.EXTERNAL_LOWER_MTMV.TABLE_TEST values(1, 'lower')"""

    sql """drop catalog if exists test_lower_case_mtmv """

    sql """ CREATE CATALOG `test_lower_case_mtmv` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "EXTERNAL_LOWER_MTMV"
        )"""


    sql """CREATE MATERIALIZED VIEW internal.EXTERNAL_LOWER_MTMV.MTMV_TEST
            REFRESH COMPLETE ON SCHEDULE EVERY 1 minute
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
                'replication_num' = '1'
            )
         AS SELECT * FROM test_lower_case_mtmv.external_lower_mtmv.table_test;"""

    qt_select """select * from internal.EXTERNAL_LOWER_MTMV.MTMV_TEST"""

    sql """drop catalog if exists test_lower_case_mtmv """
    sql """drop database if exists internal.EXTERNAL_LOWER_MTMV """

    try_sql """drop user ${jdbcUser}"""
}
