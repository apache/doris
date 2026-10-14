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

suite("test_hive_streaming_splits", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }

    // An existing catalog also allows this suite to run against an isolated S3-backed HMS fixture.
    String catalog = context.config.otherConfigs.get("hiveSplitCatalog")
    if (!catalog) {
        catalog = "hive_streaming_splits"
        String host = context.config.otherConfigs.get("externalEnvIp")
        String port = context.config.otherConfigs.get("hive3HmsPort")
        sql "drop catalog if exists ${catalog}"
        sql """create catalog ${catalog} properties (
            'type' = 'hms', 'hive.metastore.uris' = 'thrift://${host}:${port}')"""
    }
    def settings = ["enable_external_table_batch_mode", "num_files_in_batch_mode",
                    "num_partitions_in_batch_mode", "split_batch_size", "split_queue_capacity"]
    def saved = settings.collectEntries { name -> [(name): sql("show variables like '${name}'")[0][1]] }
    try {
        sql "create database if not exists ${catalog}.hive_streaming_splits"
        sql "use ${catalog}.hive_streaming_splits"
        // Truncate while the previous partition metadata still exists, so repeated runs clear its files.
        if (!sql("show tables like 'streaming_unpartitioned'").isEmpty()) {
            sql "truncate table streaming_unpartitioned"
        }
        if (!sql("show tables like 'streaming_partitioned'").isEmpty()) {
            sql "truncate table streaming_partitioned"
        }
        sql "drop table if exists streaming_unpartitioned"
        sql "drop table if exists streaming_partitioned"
        sql """create table streaming_unpartitioned (id bigint, file_id int)
            engine=hive properties ('file_format'='parquet')"""
        sql """create table streaming_partitioned (id bigint, file_id int, p int)
            engine=hive partition by list (p) () properties ('file_format'='parquet')"""
        sql "set enable_external_table_batch_mode=true"
        sql "set num_files_in_batch_mode=1"
        sql "set num_partitions_in_batch_mode=1"
        qt_empty_unpartitioned "select count(*), sum(id) from streaming_unpartitioned"
        qt_empty_partitioned "select count(*), sum(id) from streaming_partitioned"
        // Separate inserts create enough files to cross several small producer batches.
        for (int file = 0; file < 12; file++) {
            sql """insert into streaming_unpartitioned
                select number + ${file * 10}, ${file} from numbers('number'='10')"""
            sql """insert into streaming_partitioned
                select number + ${file * 10}, ${file}, ${file % 3 + 1} from numbers('number'='10')"""
        }
        def checkResults = { String label ->
            quickTest("${label}_unpartitioned_aggregate", """select count(*), sum(id), count(distinct id), sum(file_id)
                from streaming_unpartitioned""", true)
            quickTest("${label}_unpartitioned_files", """select file_id, count(*), count(distinct id),
                sum(id), min(id), max(id) from streaming_unpartitioned group by file_id order by file_id""", true)
            quickTest("${label}_unpartitioned_limit", """select count(*) from
                (select id from streaming_unpartitioned limit 10) limited""", true)
            quickTest("${label}_partitioned_aggregate", """select count(*), sum(id), count(distinct id), sum(file_id), sum(p)
                from streaming_partitioned""", true)
            quickTest("${label}_partitioned_files", """select p, file_id, count(*), count(distinct id),
                sum(id), min(id), max(id) from streaming_partitioned group by p, file_id order by p, file_id""", true)
            quickTest("${label}_partition_range", """select count(*), sum(id), count(distinct id), sum(p)
                from streaming_partitioned where p between 2 and 3""", true)
            quickTest("${label}_single_partition_limit", """select count(*) from
                (select id from streaming_partitioned where p=1 limit 10) limited""", true)
            quickTest("${label}_empty_partitions", """select count(*), sum(id)
                from streaming_partitioned where p>3""", true)
        }
        sql "set enable_external_table_batch_mode=false"
        sql "set num_partitions_in_batch_mode=0"
        explain {
            sql "select * from streaming_partitioned limit 10"
            notContains "(approximate)inputSplitNum"
        }
        checkResults("synchronous")
        sql "set enable_external_table_batch_mode=true"
        sql "set num_partitions_in_batch_mode=1"
        for (def limits : [[3, 1], [7, 2], [128, 32]]) {
            sql "set split_batch_size=${limits[0]}"
            sql "set split_queue_capacity=${limits[1]}"
            explain {
                sql "select * from streaming_unpartitioned limit 10"
                contains "(approximate)inputSplitNum"
            }
            explain {
                sql "select * from streaming_partitioned where p=1 limit 10"
                contains "(approximate)inputSplitNum"
            }
            checkResults("streaming_${limits[0]}_${limits[1]}")
        }
        test {
            sql "set split_batch_size=0"
            exception "Split batch size and queue capacity must be positive"
        }
        test {
            sql "set split_queue_capacity=-1"
            exception "Split batch size and queue capacity must be positive"
        }
    } finally {
        saved.each { name, value -> sql "set ${name}=${value}" }
    }
}
