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

import org.apache.doris.regression.util.JdbcUtils

import java.sql.DriverManager

// HMS gives a Delta table only three things: whether it is Delta, its name and its root path. Schema, files and
// deletion vectors always come from _delta_log, and other suites cover those. This suite varies how Spark registers
// a table, so a path derived from the table name, or a stale name or path after a change, fails here.
//
// Every scenario runs through both catalogs that can reach such a table: the dedicated Delta catalog and the HMS
// gateway catalog, which serves plain Hive tables next to Delta ones. test_iceberg_on_hms_gateway_ddl_parity and
// test_iceberg_hms_case_sensibility are the models for that parity and for the name-case modes.
suite("test_deltalake_hms_registration", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String sparkPort = context.config.otherConfigs.get("deltalakeSparkPort")
    // External tables keep their files after DROP TABLE, so each run needs a root no earlier run has written to.
    String root = "s3a://delta/hms_registration/${System.currentTimeMillis()}"
    Map<String, String> catalogTypes = [
            "dedicated": "'type'='deltalake', 'deltalake.catalog.type'='hms'",
            "gateway": "'type'='hms'",
    ]

    def createCatalog = { String catalog, String typeProperties, String extraProperties ->
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            ${typeProperties}, 'hive.metastore.uris'='thrift://${host}:${hmsPort}',
            'meta.cache.deltalake.latest.ttl-second'='-1', ${extraProperties}
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
        )"""
    }

    Class.forName("org.apache.hive.jdbc.HiveDriver")
    def spark = DriverManager.getConnection("jdbc:hive2://${host}:${sparkPort}/default", "hadoop", "hadoop")
    def runSpark = { String statement -> JdbcUtils.executeToList(spark, statement) }
    try {
        for (String database : ["delta_hms_reg", "delta_hms_reg_plain", "delta_hms_reg_dropped"]) {
            runSpark("drop database if exists ${database} cascade")
            runSpark("create database ${database} location '${root}/${database}'")
        }

        // Managed: no LOCATION, Spark chooses the path. Partitions and a deletion vector prove that data files and
        // the deletion vector both resolve against the path HMS returned.
        runSpark("""create table delta_hms_reg.reg_managed (id bigint, value string, part string) using delta
            partitioned by (part) tblproperties ('delta.enableDeletionVectors'='true')""")
        runSpark("""insert into delta_hms_reg.reg_managed values
            (1, 'one', 'a'), (2, 'two', 'a'), (3, 'three', 'b'), (4, 'four', null)""")
        runSpark("delete from delta_hms_reg.reg_managed where id = 2")
        runSpark("""create table delta_hms_reg.reg_ctas using delta
            as select 1L as id, 'ctas-one' as value union all select 2L, 'ctas-two'""")

        // External: the directory name has nothing to do with the table name.
        runSpark("""create table delta_hms_reg.reg_external (id bigint, value string) using delta
            location '${root}/elsewhere/not_the_table_name'""")
        runSpark("insert into delta_hms_reg.reg_external values (1, 'external-one'), (2, 'external-two')")
        // No columns in the DDL, and a second name for the same path: only _delta_log can supply this schema.
        runSpark("""create table delta_hms_reg.reg_no_columns using delta
            location '${root}/elsewhere/not_the_table_name'""")
        runSpark("""create table delta_hms_reg.reg_space_path (id bigint) using delta
            location '${root}/elsewhere/with space/table'""")
        runSpark("insert into delta_hms_reg.reg_space_path values (1)")
        runSpark("""create table delta_hms_reg.reg_trailing_slash (id bigint) using delta
            location '${root}/elsewhere/trailing_slash/'""")
        runSpark("insert into delta_hms_reg.reg_trailing_slash values (1)")
        // HMS lowercases this name, while _delta_log keeps the case of the columns.
        runSpark("create table delta_hms_reg.REG_Mixed_Case (ID bigint, MixedValue string) using delta")
        runSpark("insert into delta_hms_reg.reg_mixed_case values (1, 'mixed')")

        // These share a database with Delta tables, and must be filtered or served as non-Delta.
        runSpark("""create table delta_hms_reg.reg_plain (id bigint, value string) using parquet
            location '${root}/elsewhere/plain'""")
        runSpark("insert into delta_hms_reg.reg_plain values (1, 'parquet')")
        runSpark("create view delta_hms_reg.reg_view as select id from delta_hms_reg.reg_external")
        runSpark("""create table delta_hms_reg_plain.plain_only (id bigint) using parquet
            location '${root}/elsewhere/plain_only'""")

        // Changed by Spark below, after both catalogs have cached them.
        runSpark("""create table delta_hms_reg.reg_rename_external (id bigint, value string) using delta
            location '${root}/elsewhere/rename_external' tblproperties ('delta.enableDeletionVectors'='true')""")
        runSpark("""create table delta_hms_reg.reg_rename_managed (id bigint, value string) using delta
            tblproperties ('delta.enableDeletionVectors'='true')""")
        for (String table : ["reg_rename_external", "reg_rename_managed"]) {
            runSpark("insert into delta_hms_reg.${table} values (1, 'before'), (2, 'before')")
        }
        runSpark("""create table delta_hms_reg.reg_drop_external (id bigint) using delta
            location '${root}/elsewhere/drop_external'""")
        runSpark("insert into delta_hms_reg.reg_drop_external values (1)")
        for (String table : ["reg_drop_managed", "reg_recreate", "reg_replace"]) {
            runSpark("create table delta_hms_reg.${table} (id bigint) using delta")
            runSpark("insert into delta_hms_reg.${table} values (1)")
        }
        runSpark("create table delta_hms_reg_dropped.reg_in_dropped_db (id bigint) using delta")
        runSpark("insert into delta_hms_reg_dropped.reg_in_dropped_db values (1)")

        for (String label : catalogTypes.keySet()) {
            String typeProperties = catalogTypes[label]
            String catalog = "test_deltalake_hms_reg_${label}"
            createCatalog(catalog, typeProperties, "")

            // Discovery. The gateway also lists non-Delta objects, the dedicated catalog must not.
            "order_qt_${label}_databases"("show databases from ${catalog} like 'delta_hms_reg%'")
            "order_qt_${label}_tables"("show tables from ${catalog}.delta_hms_reg")
            "order_qt_${label}_plain_db_tables"("show tables from ${catalog}.delta_hms_reg_plain")
            sql "switch ${catalog}"
            "order_qt_${label}_info_tables"("""select TABLE_NAME from information_schema.tables
                where TABLE_SCHEMA = 'delta_hms_reg' and TABLE_NAME like 'reg_m%'""")
            "order_qt_${label}_info_columns"("""select COLUMN_NAME, DATA_TYPE from information_schema.columns
                where TABLE_SCHEMA = 'delta_hms_reg' and TABLE_NAME = 'reg_managed'""")
            sql "switch internal"

            // Registration forms.
            for (String table : ["reg_managed", "reg_ctas", "reg_external", "reg_no_columns", "reg_space_path",
                    "reg_trailing_slash", "reg_mixed_case"]) {
                "qt_${label}_${table}_schema"("desc ${catalog}.delta_hms_reg.${table}")
                "order_qt_${label}_${table}"("select * from ${catalog}.delta_hms_reg.${table}")
                "qt_${label}_${table}_count"("select count(*) from ${catalog}.delta_hms_reg.${table}")
            }
            "qt_${label}_external_ddl"("show create table ${catalog}.delta_hms_reg.reg_external")
            "order_qt_${label}_managed_partition"(
                    "select * from ${catalog}.delta_hms_reg.reg_managed where part = 'a'")
            "order_qt_${label}_managed_null_partition"(
                    "select * from ${catalog}.delta_hms_reg.reg_managed where part is null")
            "order_qt_${label}_mixed_case_column"(
                    "select ID, mixedvalue from ${catalog}.delta_hms_reg.reg_mixed_case where MIXEDVALUE = 'mixed'")

            // A non-Delta table is invisible to the dedicated catalog and stays plain Hive through the gateway.
            if (label == "dedicated") {
                test {
                    sql "select * from ${catalog}.delta_hms_reg.reg_plain"
                    exception "Table [reg_plain] does not exist"
                }
            } else {
                "order_qt_${label}_plain"("select * from ${catalog}.delta_hms_reg.reg_plain")
            }

            // Delta is read-only in Doris. These must not fall through to the Hive write path on the gateway, which
            // would rewrite or unregister a table it does not own. INSERT is refused either at sink creation or when
            // the write plan is resolved, with different texts that share only this phrase.
            test {
                sql "insert into ${catalog}.delta_hms_reg.reg_external values (3, 'rejected')"
                exception "does not"
            }
            test {
                sql "truncate table ${catalog}.delta_hms_reg.reg_external"
                exception "TRUNCATE TABLE not supported"
            }
            test {
                sql "alter table ${catalog}.delta_hms_reg.reg_external rename reg_external_rejected"
                exception "RENAME TABLE not supported"
            }
            test {
                sql "drop table ${catalog}.delta_hms_reg.reg_external"
                exception "DROP TABLE not supported"
            }
            "order_qt_${label}_external_after_rejected_writes"("select * from ${catalog}.delta_hms_reg.reg_external")

            // Cache every table that Spark changes below.
            for (String table : ["reg_rename_external", "reg_rename_managed", "reg_drop_external", "reg_drop_managed",
                    "reg_recreate", "reg_replace"]) {
                "order_qt_${label}_${table}_initial"("select * from ${catalog}.delta_hms_reg.${table}")
            }
            "order_qt_${label}_in_dropped_db_initial"(
                    "select * from ${catalog}.delta_hms_reg_dropped.reg_in_dropped_db")
        }

        // Table names are case-insensitive only when the catalog says so. Database names never are in these modes.
        for (String label : catalogTypes.keySet()) {
            String typeProperties = catalogTypes[label]
            for (String mode : ["0", "1", "2"]) {
                String catalog = "test_deltalake_hms_reg_${label}_case_${mode}"
                createCatalog(catalog, typeProperties, "'only_test_lower_case_table_names'='${mode}',")
                "order_qt_${label}_case_${mode}_lower"("select * from ${catalog}.delta_hms_reg.reg_mixed_case")
                "order_qt_${label}_case_${mode}_tables"(
                        "show tables from ${catalog}.delta_hms_reg like '%MIXED_CASE%'")
                if (mode == "0") {
                    test {
                        sql "select * from ${catalog}.delta_hms_reg.REG_MIXED_CASE"
                        exception "Table [REG_MIXED_CASE] does not exist in database [delta_hms_reg]"
                    }
                } else {
                    "order_qt_${label}_case_${mode}_upper"("select * from ${catalog}.delta_hms_reg.REG_MIXED_CASE")
                }
                test {
                    sql "select * from ${catalog}.DELTA_HMS_REG.reg_mixed_case"
                    exception "Database [DELTA_HMS_REG] does not exist"
                }
            }
        }

        // Renames. The external path stays put, so the name stops matching the directory. HMS moves the managed
        // directory and Spark rewrites the SerDe path. New commits prove that the new name reaches the live log.
        for (String table : ["reg_rename_external", "reg_rename_managed"]) {
            runSpark("alter table delta_hms_reg.${table} rename to delta_hms_reg.${table}_new")
            runSpark("delete from delta_hms_reg.${table}_new where id = 1")
            runSpark("insert into delta_hms_reg.${table}_new values (3, 'after')")
        }
        // Dropping external tables keeps the files, so the same path can be registered again under another name.
        runSpark("drop table delta_hms_reg.reg_drop_external")
        runSpark("""create table delta_hms_reg.reg_reattached using delta
            location '${root}/elsewhere/drop_external'""")
        runSpark("drop table delta_hms_reg.reg_drop_managed")
        // Same name, new table. DROP removes the managed files, while REPLACE commits a new schema on the same path.
        runSpark("drop table delta_hms_reg.reg_recreate")
        runSpark("create table delta_hms_reg.reg_recreate (recreated string) using delta")
        runSpark("insert into delta_hms_reg.reg_recreate values ('recreated')")
        runSpark("create or replace table delta_hms_reg.reg_replace (replaced string) using delta")
        runSpark("insert into delta_hms_reg.reg_replace values ('replaced')")
        // One commit through one name must reach the other name of the same path.
        runSpark("insert into delta_hms_reg.reg_no_columns values (3, 'external-three')")
        runSpark("create table delta_hms_reg.reg_added_later (id bigint) using delta")
        runSpark("insert into delta_hms_reg.reg_added_later values (1)")
        runSpark("drop database delta_hms_reg_dropped cascade")

        for (String label : catalogTypes.keySet()) {
            String catalog = "test_deltalake_hms_reg_${label}"
            sql "refresh database ${catalog}.delta_hms_reg"
            "order_qt_${label}_tables_after_changes"("show tables from ${catalog}.delta_hms_reg")

            for (String table : ["reg_rename_external", "reg_rename_managed"]) {
                test {
                    sql "select * from ${catalog}.delta_hms_reg.${table}"
                    exception "Table [${table}] does not exist"
                }
                "qt_${label}_${table}_new_schema"("desc ${catalog}.delta_hms_reg.${table}_new")
                "order_qt_${label}_${table}_new"("select * from ${catalog}.delta_hms_reg.${table}_new")
                "qt_${label}_${table}_new_count"("select count(*) from ${catalog}.delta_hms_reg.${table}_new")
            }
            for (String table : ["reg_drop_external", "reg_drop_managed"]) {
                test {
                    sql "select * from ${catalog}.delta_hms_reg.${table}"
                    exception "Table [${table}] does not exist"
                }
            }
            "qt_${label}_reattached_schema"("desc ${catalog}.delta_hms_reg.reg_reattached")
            "order_qt_${label}_reattached"("select * from ${catalog}.delta_hms_reg.reg_reattached")
            for (String table : ["reg_recreate", "reg_replace"]) {
                "qt_${label}_${table}_schema_after_change"("desc ${catalog}.delta_hms_reg.${table}")
                "order_qt_${label}_${table}_after_change"("select * from ${catalog}.delta_hms_reg.${table}")
            }
            for (String table : ["reg_external", "reg_no_columns"]) {
                "order_qt_${label}_${table}_shared_path"("select * from ${catalog}.delta_hms_reg.${table}")
            }
            "order_qt_${label}_added_later"("select * from ${catalog}.delta_hms_reg.reg_added_later")

            sql "refresh catalog ${catalog}"
            "order_qt_${label}_databases_after_drop"("show databases from ${catalog} like 'delta_hms_reg%'")
            test {
                sql "select * from ${catalog}.delta_hms_reg_dropped.reg_in_dropped_db"
                exception "Database [delta_hms_reg_dropped] does not exist"
            }
        }
    } finally {
        spark.close()
    }
}
