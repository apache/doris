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

suite("test_deltalake_metadata", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableDeltaLakeTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("deltalakeMinioPort")
    String hmsPort = context.config.otherConfigs.get("deltalakeHmsPort")
    String sparkPort = context.config.otherConfigs.get("deltalakeSparkPort")
    String warehouse = context.config.otherConfigs.get("deltalakeWarehouse")
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    def spark = DriverManager.getConnection("jdbc:hive2://${host}:${sparkPort}/default", "hadoop", "hadoop")
    try {
        sql "set num_files_in_batch_mode=1"
        for (String source : ["filesystem", "hms"]) {
            String catalog = "test_deltalake_metadata_${source}"
            String metastore = source == "filesystem" ? "'warehouse'='${warehouse}'"
                    : "'hive.metastore.uris'='thrift://${host}:${hmsPort}'"
            for (String suffix : ["", "_uncached"]) {
                sql "drop catalog if exists ${catalog}${suffix}"
                sql """create catalog ${catalog}${suffix} properties (
                    'type'='deltalake', 'deltalake.catalog.type'='${source}', ${metastore},
                    'meta.cache.deltalake.latest.ttl-second'='${suffix.isEmpty() ? -1 : 0}',
                    's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
                    's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true'
                )"""
            }
            for (boolean streaming : [false, true]) {
                sql "set enable_external_table_batch_mode=${streaming}"
                String label = "${source}_${streaming}"
                for (String table : ["multi_part_checkpoint", "only_checkpoint_files", "v2_checkpoint_json",
                        "v2_checkpoint_parquet", "basic_with_vacuum_protocol_check_feature",
                        "dv_partitioned_with_checkpoint", "dv_with_columnmapping"]) {
                    "order_qt_${label}_${table}"("select * from ${catalog}.delta_golden.${table}")
                    "qt_${label}_${table}_count"("select count(*) from ${catalog}.delta_golden.${table}")
                }
                "order_qt_${label}_compaction"("select * from ${catalog}.delta_m1.log_compaction")
                "order_qt_${label}_pruning"("select * from ${catalog}.delta_m1.skipping where id<5")
                "qt_${label}_count"("select count(*) from ${catalog}.delta_m1.skipping")
                sql "set file_split_size=128"
                "order_qt_${label}_dv_splits"("select * from ${catalog}.delta_m1.dv")
                "order_qt_${label}_mapping_splits"("select * from ${catalog}.delta_m1.mapping_id")
                sql "set file_split_size=0"
            }
            // Fixed, unlimited TTL makes refresh checks independent of machine speed.
            // The fake-clock unit test separately proves that ordinary cache hits never extend a finite TTL.
            JdbcUtils.executeToList(spark, """create or replace table delta_m1.refresh_${source} (id bigint)
                using delta tblproperties ('delta.enableDeletionVectors'='false')""")
            JdbcUtils.executeToList(spark, "insert into delta_m1.refresh_${source} values (1)")
            "order_qt_${source}_initial"("select * from ${catalog}.delta_m1.refresh_${source}")
            JdbcUtils.executeToList(spark, "insert into delta_m1.refresh_${source} values (2)")
            "order_qt_${source}_cached"("select * from ${catalog}.delta_m1.refresh_${source}")
            "order_qt_${source}_uncached"("select * from ${catalog}_uncached.delta_m1.refresh_${source}")
            sql "refresh table ${catalog}.delta_m1.refresh_${source}"
            "order_qt_${source}_table_refresh"("select * from ${catalog}.delta_m1.refresh_${source}")
            JdbcUtils.executeToList(spark, "alter table delta_m1.refresh_${source} add columns (value string)")
            JdbcUtils.executeToList(spark, "insert into delta_m1.refresh_${source} values (3, 'new-schema')")
            "qt_${source}_cached_schema"("desc ${catalog}.delta_m1.refresh_${source}")
            sql "refresh table ${catalog}.delta_m1.refresh_${source}"
            "qt_${source}_refreshed_schema"("desc ${catalog}.delta_m1.refresh_${source}")
            "order_qt_${source}_schema_rows"("select * from ${catalog}.delta_m1.refresh_${source}")
            JdbcUtils.executeToList(spark, "insert into delta_m1.refresh_${source} values (4, 'database-refresh')")
            sql "refresh database ${catalog}.delta_m1"
            "order_qt_${source}_database_refresh"("select * from ${catalog}.delta_m1.refresh_${source}")
            JdbcUtils.executeToList(spark, "insert into delta_m1.refresh_${source} values (5, 'catalog-refresh')")
            sql "refresh catalog ${catalog}"
            "order_qt_${source}_catalog_refresh"("select * from ${catalog}.delta_m1.refresh_${source}")
        }
    } finally {
        spark.close()
    }
}
