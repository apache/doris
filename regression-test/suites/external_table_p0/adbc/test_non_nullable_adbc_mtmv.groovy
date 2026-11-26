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

suite("test_non_nullable_adbc_mtmv", "p0,external") {
    String driver = context.config.otherConfigs.get("adbcDriverPath")
    if (driver == null || driver.isEmpty()) {
        String thirdparty = System.getenv("DORIS_THIRDPARTY")
        if (thirdparty == null || thirdparty.isEmpty()) {
            thirdparty = new File(context.config.suitePath).parentFile.parent + "/thirdparty"
        }
        driver = thirdparty + "/installed/lib64/libadbc_driver_flightsql.so"
    }
    if (!new File(driver).canRead()) {
        logger.info("Skip non-nullable ADBC MV regression: the Flight SQL driver is not installed")
        return
    }
    sql "SET materialized_view_rewrite_enable_contain_external_table = true"
    sql "DROP MATERIALIZED VIEW IF EXISTS non_nullable_adbc_mv"
    sql "DROP TABLE IF EXISTS non_nullable_adbc_base"
    sql """CREATE TABLE non_nullable_adbc_base (id INT NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO non_nullable_adbc_base VALUES (1), (2), (3)"
    def frontend = sql("SHOW FRONTENDS")[0]
    String db = context.dbName
    sql "DROP CATALOG IF EXISTS non_nullable_mv_adbc"
    sql """CREATE CATALOG non_nullable_mv_adbc PROPERTIES (
        "type" = "adbc", "driver_url" = "${driver}",
        "sql_dialect" = "doris", "uri" = "grpc://${frontend[1]}:${frontend[6]}",
        "user" = "${context.config.jdbcUser}", "password" = "${context.config.jdbcPassword}",
        "partitioned_read" = "required"
    )"""
    checkNonNullableMvRewrite("non_nullable_mv_adbc.${db}.non_nullable_adbc_base",
            "id", "non_nullable_adbc_mv")
}
