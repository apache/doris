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

suite("test_jdbc_runtime_filter", "p0,external,doris,external_docker,external_docker_doris") {
    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    String catalog_name = "doris_jdbc_catalog_test_runtime_filter";
    String db_name = "regression_test_jdbc_runtime_filter";
    String dorisTable = "doris_tbl";
    String jdbcTable = "jdbc_tbl";

    sql """create database if not exists ${db_name}; """

    sql """use ${db_name}"""
    sql  """ drop table if exists ${db_name}.${jdbcTable} """
    sql  """ drop table if exists ${db_name}.${dorisTable} """
     sql """
          CREATE TABLE ${jdbcTable} (t1 INT) DISTRIBUTED BY HASH (t1) BUCKETS 2 PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${jdbcTable} VALUES (3), (4), (5); """
    sql  """
          CREATE TABLE ${dorisTable} (t2 INT) DISTRIBUTED BY HASH (t2) BUCKETS 2 PROPERTIES("replication_num" = "1");
        """
    sql """ INSERT INTO ${dorisTable} VALUES (1), (2), (3), (4); """

    sql """drop catalog if exists ${catalog_name} """
    sql """ CREATE CATALOG `${catalog_name}` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""

    sql """ set enable_pipeline_x_engine=false; """
    sql """ set runtime_filter_type="IN,MIN_MAX" """
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """
    sql """ set disable_join_reorder=true """
    sql """ set enable_runtime_filter_prune=false """
    explain {
        sql(""" SELECT * FROM ${catalog_name}.${db_name}.${jdbcTable} tb1 JOIN ${dorisTable} tb2 where tb1.t1 = tb2.t2; """)
        contains "runtime filters: RF000[in] <- t2"
        contains "RF001[min_max] <- t2"
        contains "runtime filters: RF000[in] -> t1"
        contains "RF001[min_max] -> t1"
    }

    sql  """ drop table if exists ${db_name}.${dorisTable} """
    sql  """ drop table if exists ${db_name}.${jdbcTable} """
    sql """drop catalog if exists ${catalog_name} """
}