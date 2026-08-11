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

import java.sql.Connection
import java.sql.DriverManager

// A JDBC catalog caches the remote column definition. When the remote DDL widens a column, the
// cache keeps reporting the narrow one until it is refreshed, so the planner sees source and
// target as the same type and emits no cast. The value then arrives carrying more than the
// planned column can hold.
//
// For a DATETIMEV2 key column that is not cosmetic: the sub-second digits live in the same 64-bit
// word the storage layer encodes as a key, so two rows the column claims are equal become two
// distinct keys and a unique table stops deduplicating them.
//
// The scanner checks the value against the column it is writing into, not the source schema
// against the cached one. A value that fits is written whatever the source now says, so these
// cases assert on values rather than on declarations.
suite("test_jdbc_stale_schema_precision", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String pg_port = context.config.otherConfigs.get("pg_14_port")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

    String catalog_name = "test_jdbc_stale_schema_precision_catalog"
    String internal_db = "regression_test_jdbc_stale_schema_precision"
    String pg_schema = "stale_precision"
    String pg_url = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?useSSL=false"

    Class.forName("org.postgresql.Driver")

    // The catalog cannot issue DDL against the source, so drive PostgreSQL directly.
    def onPostgres = { List<String> statements ->
        Connection conn = DriverManager.getConnection(pg_url, "postgres", "123456")
        try {
            def stmt = conn.createStatement()
            try {
                statements.each { stmt.execute(it) }
            } finally {
                stmt.close()
            }
        } finally {
            conn.close()
        }
    }

    sql """drop catalog if exists ${catalog_name}"""
    sql """drop database if exists internal.${internal_db}"""
    sql """create database internal.${internal_db}"""

    onPostgres([
        "DROP SCHEMA IF EXISTS ${pg_schema} CASCADE",
        "CREATE SCHEMA ${pg_schema}",
        // Declared narrow to begin with: this is what the catalog will cache.
        """CREATE TABLE ${pg_schema}.orders (
               mchid varchar, orderid varchar, createtime timestamp(0), status int)""",
        "INSERT INTO ${pg_schema}.orders VALUES ('M001','ORD1','2026-06-19 12:00:00',0)",
    ])

    sql """create catalog ${catalog_name} properties(
        "type"="jdbc",
        "user"="postgres",
        "password"="123456",
        "jdbc_url" = "${pg_url}&currentSchema=${pg_schema}",
        "driver_url" = "${driver_url}",
        "driver_class" = "org.postgresql.Driver"
    );"""

    // Populate the catalog's schema cache while the remote column is still narrow.
    qt_cached_datetime_type """desc ${catalog_name}.${pg_schema}.orders"""

    // Widen the remote column and write values only the wider type can hold. The catalog is
    // deliberately not refreshed, so from here on its cache disagrees with the source.
    onPostgres([
        "ALTER TABLE ${pg_schema}.orders ALTER COLUMN createtime TYPE timestamp(6)",
        "DELETE FROM ${pg_schema}.orders",
        """INSERT INTO ${pg_schema}.orders VALUES
               ('M001','ORD1','2026-06-19 12:23:23.486067',1),
               ('M001','ORD1','2026-06-19 12:23:23.000000',2)""",
    ])

    // Still the narrow type -- the cache has not caught up.
    qt_stale_datetime_type """desc ${catalog_name}.${pg_schema}.orders"""

    sql """create table internal.${internal_db}.orders_target (
               `mchid`      varchar(65533) NOT NULL,
               `createtime` datetime       NOT NULL,
               `orderid`    varchar(65533) NOT NULL,
               `status`     int            NULL
           ) ENGINE=OLAP
           UNIQUE KEY(`mchid`, `createtime`, `orderid`)
           DISTRIBUTED BY HASH(`mchid`) BUCKETS 1
           PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");"""

    // The plan was built from the cached, narrower type, so no cast exists and the extra digits
    // would land in a column that cannot hold them -- splitting one logical key into two and
    // breaking dedup on the unique table. The scanner refuses the value instead of writing it.
    test {
        sql """insert into internal.${internal_db}.orders_target
               select mchid, createtime, orderid, status
               from ${catalog_name}.${pg_schema}.orders;"""
        exception "carries more digits than that"
    }

    test {
        sql """select createtime from ${catalog_name}.${pg_schema}.orders"""
        exception "carries more digits than that"
    }

    // A drifted column whose values all still fit is read normally: the check is about what
    // arrives, not about what the cache declares. This is the difference from rejecting on the
    // schema, and it is what keeps a source the catalog maps down -- an Oracle TIMESTAMP(9)
    // planned as DATETIMEV2(6) -- from failing every scan.
    onPostgres([
        "CREATE TABLE ${pg_schema}.whole_seconds (id int, ts timestamp(0))",
        "INSERT INTO ${pg_schema}.whole_seconds VALUES (1, '2026-06-19 12:23:23')",
    ])
    qt_cached_whole_seconds_type """desc ${catalog_name}.${pg_schema}.whole_seconds"""
    onPostgres(["ALTER TABLE ${pg_schema}.whole_seconds ALTER COLUMN ts TYPE timestamp(6)"])
    qt_drifted_but_fits """select ts from ${catalog_name}.${pg_schema}.whole_seconds"""

    // Refreshing reconciles the cache. Every read that failed above has to succeed now and
    // return the value the source actually holds.
    sql """refresh catalog ${catalog_name}"""

    qt_refreshed_datetime_type """desc ${catalog_name}.${pg_schema}.orders"""
    qt_refreshed_datetime_value """select createtime from ${catalog_name}.${pg_schema}.orders
                                  where status = 1"""

    // A source narrower than the plan cannot produce a value the column will not hold, so it is
    // left alone -- only the widening direction can be a problem.
    onPostgres([
        "CREATE TABLE ${pg_schema}.narrowed (id int, ts timestamp(6))",
        "INSERT INTO ${pg_schema}.narrowed VALUES (1, '2026-06-19 12:23:23.486067')",
    ])
    sql """select ts from ${catalog_name}.${pg_schema}.narrowed"""
    onPostgres(["ALTER TABLE ${pg_schema}.narrowed ALTER COLUMN ts TYPE timestamp(0)"])
    qt_narrowed_source """select ts from ${catalog_name}.${pg_schema}.narrowed"""

    // With the cache reconciled the planner can see the source is wider and emits the narrowing
    // cast, so the same insert now succeeds: both source rows round onto one second-granularity
    // key and the unique table keeps one of them. Which one is not pinned -- there is no
    // sequence column and the source query has no ordering -- so assert the key collapsed and
    // carries no sub-second digits, not which row won.
    sql """insert into internal.${internal_db}.orders_target
           select mchid, createtime, orderid, status
           from ${catalog_name}.${pg_schema}.orders;"""
    qt_after_refresh_dedup """select mchid, orderid, createtime,
                                     microsecond(cast(createtime as datetime(6))) as hidden_us
                              from internal.${internal_db}.orders_target"""

    def rows = sql """select count(*) from internal.${internal_db}.orders_target"""
    assertEquals(1, (rows[0][0] as Number).intValue())

    // And a wider destination keeps the microseconds the source actually has -- the reason this
    // layer refuses rather than rounds.
    sql """create table internal.${internal_db}.orders_wide (
               `id` int, `createtime` datetime(6)
           ) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ("replication_num" = "1");"""
    sql """insert into internal.${internal_db}.orders_wide
           select status, createtime from ${catalog_name}.${pg_schema}.orders where status = 1;"""
    qt_wide_destination """select createtime, microsecond(createtime) as us
                           from internal.${internal_db}.orders_wide"""
}
