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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_timing_refresh_catalog", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_timing_refresh_catalog_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    def wait_table_sync = { String db ->
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until{
            try {
                def res = sql "show tables from ${db}"
                return res.size() > 0;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    def is_single_fe = {
        def res = sql "show frontends";
        return res.size() == 1;
    }    

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

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

    test {
        def catalogName = "test_timing_refresh_catalog1"
        for (int i = 0; i < 10; i++) {
            sql """
                select * from ${catalogName}.external_timing_refresh_catalog.tbl
            """
            Thread.sleep(1000)
        }
    }

    // when "use_meta_cache" = "false", the meta sync between FEs maybe delay,
    // and it may cause the case unstable, so only run when there is single FE
    if (is_single_fe()) {
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
            def catalogName = "test_timing_refresh_catalog2"
            for (int i = 0; i < 10; i++) {
                sql """
                    select * from ${catalogName}.external_timing_refresh_catalog.tbl
                """
                Thread.sleep(1000)
            }
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

    test {
        def catalogName = "test_timing_refresh_catalog1"
        for (int i = 0; i < 10; i++) {
            sql """
                select id_c from ${catalogName}.db.table_t
            """
            Thread.sleep(1000)
        }
    }

    if (is_single_fe()) {
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
            def catalogName = "test_timing_refresh_catalog2"
            for (int i = 0; i < 10; i++) {
                sql """
                    select id_c from ${catalogName}.db.table_t
                """
                Thread.sleep(1000)
            }
        }
    }

    try_sql """drop user ${jdbcUser}"""
}
