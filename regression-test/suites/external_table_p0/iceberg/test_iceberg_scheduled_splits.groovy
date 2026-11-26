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

suite("test_iceberg_scheduled_splits", "p0,external,iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String catalog = context.config.otherConfigs.get("icebergSplitCatalog")
    if (!catalog) {
        catalog = "iceberg_scheduled_splits"
        String host = context.config.otherConfigs.get("externalEnvIp")
        String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type'='iceberg', 'iceberg.catalog.type'='rest', 'uri'='http://${host}:${restPort}',
            's3.access_key'='admin', 's3.secret_key'='password',
            's3.endpoint'='http://${host}:${minioPort}', 's3.region'='us-east-1',
            'meta.cache.iceberg.manifest.enable'='true')"""
    }
    def settings = ["enable_external_table_batch_mode", "num_files_in_batch_mode",
                    "split_batch_size", "split_queue_capacity", "enable_sql_cache"]
    def saved = settings.collectEntries { name -> [(name): sql("show variables like '${name}'")[0][1]] }
    try {
        sql "create database if not exists ${catalog}.iceberg_scheduled_splits"
        sql "use ${catalog}.iceberg_scheduled_splits"
        sql "drop table if exists scheduled_manifest_scan"
        sql "create table scheduled_manifest_scan (id bigint) engine=iceberg"
        // Separate commits produce more manifests than one prefetch window can retain.
        for (int batch = 0; batch < 12; batch++) {
            sql """insert into scheduled_manifest_scan
                select number + ${batch * 16} from numbers('number'='16')"""
        }
        sql "set enable_external_table_batch_mode=true"
        sql "set num_files_in_batch_mode=1"
        sql "set enable_sql_cache=false"
        sql "set split_batch_size=1"
        for (int capacity : [1, 4096]) {
            sql "set split_queue_capacity=${capacity}"
            for (String kind : ["limit", "full", "multi_scan"]) {
                sql "refresh catalog ${catalog}"
                for (String cache : ["cold", "warm"]) {
                    if (kind == "limit") {
                        quickTest("${capacity}_${cache}_limit", """select count(*) from
                            (select id from scheduled_manifest_scan limit 10) limited""", true)
                    } else if (kind == "full") {
                        quickTest("${capacity}_${cache}_full", """select count(*), count(distinct id), sum(id)
                            from scheduled_manifest_scan""", true)
                    } else {
                        // Both scan nodes share the query's task group, including on early stop.
                        quickTest("${capacity}_${cache}_multi_scan", """select count(*), sum(id) from
                            (select id from scheduled_manifest_scan union all
                             select id from scheduled_manifest_scan) combined""", true)
                        quickTest("${capacity}_${cache}_multi_scan_limit", """select count(*) from
                            (select id from scheduled_manifest_scan union all
                             select id from scheduled_manifest_scan limit 10) limited""", true)
                    }
                }
            }
        }
    } finally {
        saved.each { name, value -> sql "set ${name}=${value}" }
    }
}
