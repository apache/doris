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

suite("test_non_nullable_jdbc_mtmv", "p0,external") {
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    sql "DROP MATERIALIZED VIEW IF EXISTS non_nullable_jdbc_mv"
    sql "DROP TABLE IF EXISTS non_nullable_jdbc_base"
    sql """CREATE TABLE non_nullable_jdbc_base (id INT NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO non_nullable_jdbc_base VALUES (1), (2), (3)"
    String driver = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    String db = context.dbName
    sql "DROP CATALOG IF EXISTS non_nullable_mv_jdbc"
    sql """CREATE CATALOG non_nullable_mv_jdbc PROPERTIES (
        "type" = "jdbc", "user" = "${context.config.jdbcUser}", "password" = "${context.config.jdbcPassword}",
        "jdbc_url" = "${context.config.jdbcUrl}", "driver_url" = "${driver}",
        "driver_class" = "com.mysql.cj.jdbc.Driver"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_jdbc.${db}.non_nullable_jdbc_base",
            "id", "non_nullable_jdbc_mv")
}
