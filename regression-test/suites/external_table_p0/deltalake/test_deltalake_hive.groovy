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

suite("test_deltalake_hive", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String sparkPort = context.config.otherConfigs.get("deltalakeSparkPort")
    sql "drop catalog if exists test_deltalake_hive"
    sql """create catalog test_deltalake_hive properties (
        'type'='hms', 'hive.metastore.uris'='thrift://${host}:${hmsPort}',
        'meta.cache.deltalake.latest.ttl-second'='-1',
        'deltalake.catalog.type'='filesystem',
        's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
    )"""
    sql "switch test_deltalake_hive"
    sql "use delta_m1"
    sql "set time_zone='UTC'"
    qt_mapping_schema "desc mapping_id"
    qt_mapping_ddl "show create table mapping_id"
    for (String table : ["mapping_name", "mapping_id", "ntz", "widening", "dv", "log_compaction"]) {
        "order_qt_${table}"("select * from ${table}")
        "qt_${table}_count"("select count(*) from ${table}")
    }
    order_qt_mapping_filter "select * from mapping_name where partition_renamed='a/b'"
    order_qt_dv_filter "select * from dv where part=1 and id<20"
    order_qt_v2_json "select * from delta_golden.v2_checkpoint_json"
    order_qt_v2_parquet "select * from delta_golden.v2_checkpoint_parquet"
    sql "set enable_file_scanner_v2=false"
    sql "set enable_external_table_batch_mode=true"
    sql "set num_files_in_batch_mode=1"
    sql "set file_split_size=128"
    order_qt_dv_streaming "select * from dv"
    order_qt_mapping_streaming "select * from mapping_id"
    sql "set file_split_size=0"
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    def spark = DriverManager.getConnection("jdbc:hive2://${host}:${sparkPort}/default", "hadoop", "hadoop")
    try {
        JdbcUtils.executeToList(spark, """create or replace table delta_m1.gateway_refresh (id bigint)
            using delta tblproperties ('delta.enableDeletionVectors'='false')""")
        JdbcUtils.executeToList(spark, "insert into delta_m1.gateway_refresh values (1)")
        // The gateway was resolved before Spark registered this table.
        sql "refresh database delta_m1"
        order_qt_initial "select * from gateway_refresh"
        JdbcUtils.executeToList(spark, "insert into delta_m1.gateway_refresh values (2)")
        order_qt_cached "select * from gateway_refresh"
        sql "refresh table gateway_refresh"
        order_qt_table_refresh "select * from gateway_refresh"
        JdbcUtils.executeToList(spark, "alter table delta_m1.gateway_refresh add columns (value string)")
        JdbcUtils.executeToList(spark, "insert into delta_m1.gateway_refresh values (3, 'catalog-refresh')")
        sql "refresh catalog test_deltalake_hive"
        qt_refreshed_schema "desc gateway_refresh"
        order_qt_schema_refresh "select * from gateway_refresh"
        JdbcUtils.executeToList(spark, "insert into delta_m1.gateway_refresh values (4, 'database-refresh')")
        sql "refresh database delta_m1"
        order_qt_database_refresh "select * from gateway_refresh"
        JdbcUtils.executeToList(spark, "drop table if exists delta_m1.gateway_plain")
        JdbcUtils.executeToList(spark, """create table delta_m1.gateway_plain (id bigint, value string)
            using parquet location 's3a://delta/warehouse/delta_m1/gateway_plain'""")
        JdbcUtils.executeToList(spark, "insert overwrite delta_m1.gateway_plain values (1, 'hive'), (2, 'parquet')")
        sql "refresh database delta_m1"
        order_qt_plain_hive "select * from gateway_plain"
    } finally {
        spark.close()
    }
}
