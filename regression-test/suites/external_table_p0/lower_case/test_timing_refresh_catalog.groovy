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

suite("test_timing_refresh_catalog", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    String mapping = """
    {
        "databases": [
            {"remoteDatabase": "external_timing_refresh_catalog", "mapping": "db"}
        ],
        "tables": [
            {"remoteDatabase": "external_timing_refresh_catalog", "remoteTable": "tbl", "mapping": "table_t"}
        ],
        "columns": [
            {"remoteDatabase": "external_timing_refresh_catalog", "remoteTable": "tbl", "remoteColumn": "id", "mapping": "id_c"}
        ]
    }
    """

    sql """drop database if exists internal.external_timing_refresh_catalog; """
    sql """create database if not exists internal.external_timing_refresh_catalog;"""
    sql """create table if not exists internal.external_timing_refresh_catalog.tbl
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_timing_refresh_catalog.tbl values(1, 'lower')"""

    sql """drop catalog if exists test_timing_refresh_catalog1 """

    sql """ CREATE CATALOG `test_timing_refresh_catalog1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "metadata_refresh_interval_seconds" = "1",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_timing_refresh_catalog"
        )"""

    sql """drop catalog if exists test_timing_refresh_catalog2 """

    sql """ CREATE CATALOG `test_timing_refresh_catalog2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "metadata_refresh_interval_seconds" = "1",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_timing_refresh_catalog"
        )"""

    test {
        def catalogName = "test_timing_refresh_catalog1"
        for (int i = 0; i < 30; i++) {
            sql """
                select * from ${catalogName}.external_timing_refresh_catalog.tbl
            """
            Thread.sleep(1000)
        }
    }

    test {
        def catalogName = "test_timing_refresh_catalog2"
        for (int i = 0; i < 30; i++) {
            sql """
                select * from ${catalogName}.external_timing_refresh_catalog.tbl
            """
            Thread.sleep(1000)
        }
    }

    // with mapping
    sql """drop catalog if exists test_timing_refresh_catalog1 """

    sql """ CREATE CATALOG `test_timing_refresh_catalog1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "metadata_refresh_interval_seconds" = "1",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_timing_refresh_catalog",
            'meta_names_mapping' = '${mapping}'
        )"""

    sql """drop catalog if exists test_timing_refresh_catalog2 """

    sql """ CREATE CATALOG `test_timing_refresh_catalog2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "metadata_refresh_interval_seconds" = "1",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_timing_refresh_catalog",
            'meta_names_mapping' = '${mapping}'
        )"""

    test {
        def catalogName = "test_timing_refresh_catalog1"
        for (int i = 0; i < 30; i++) {
            sql """
                select id_c from ${catalogName}.db.table_t
            """
            Thread.sleep(1000)
        }
    }

    test {
        def catalogName = "test_timing_refresh_catalog2"
        for (int i = 0; i < 30; i++) {
            sql """
                select id_c from ${catalogName}.db.table_t
            """
            Thread.sleep(1000)
        }
    }

    sql """drop catalog if exists test_timing_refresh_catalog1 """
    sql """drop catalog if exists test_timing_refresh_catalog2 """
    sql """drop database if exists internal.external_timing_refresh_catalog """
}