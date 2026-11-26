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

suite("test_two_level_file_splits", "p0,external") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableHiveTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    sql "drop catalog if exists test_two_level_file_splits"
    sql """create catalog test_two_level_file_splits properties (
        "type"="hms", "hadoop.username"="hadoop",
        "hive.metastore.uris"="thrift://${host}:${hmsPort}"
    )"""
    sql "switch test_two_level_file_splits"
    sql "use `default`"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "set file_split_size=1048576"
    sql "set file_split_size_on_fe=0"
    sql "set file_split_size_on_be=0"
    sql "set max_file_scanners_concurrency=1"
    sql "set enable_condition_cache=false"
    def queries = [
        "select k, count(*) from fact_big where k is not null group by k order by k",
        "select count(*) from fact_big",
        "select a.k, count(*) from fact_big a join dim_small b on a.k=b.k where b.c1=5 group by a.k order by a.k",
        "select col1, col2, col3 from orc_tiny_stripes where col1%100=0 order by col1,col2,col3",
        "select count(*) from orc_tiny_stripes"
    ]
    // The single-scanner run cannot publish physical children. Compare the same data and SQL
    // against parallel refinement, including absent/nonpositive and positive coalescing targets.
    def reference = queries.collect { query -> sql(query) }
    sql "set max_file_scanners_concurrency=16"
    sql "set enable_condition_cache=true"
    for (int batchThreshold : [0, Integer.MAX_VALUE]) {
        sql "set num_files_in_batch_mode=${batchThreshold}"
        for (List sizes : [[0, 0], [536870912, -1], [536870912, 1048576], [536870912, 67108864]]) {
            sql "set file_split_size_on_fe=${sizes[0]}"
            sql "set file_split_size_on_be=${sizes[1]}"
            // The second pass exercises the condition-cache lifecycle after sibling completion.
            for (int pass = 0; pass < 2; pass++) {
                queries.eachWithIndex { query, index ->
                    assertEquals(reference[index], sql(query))
                }
            }
        }
    }
}
