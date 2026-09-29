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

// Bucketed hash aggregation merges the per-instance states of different buckets
// concurrently in several source instances. Merges that allocate memory (collect_set
// on strings, DISTINCT on strings) must not share an arena across those instances.
suite("collect_set_bucketed_agg_merge") {
    // Bucketed agg needs a real single-BE cluster (the P0 CI runs one BE):
    // be_number_for_test can only disable it, so the BUCKETED AGGREGATE
    // assertions below fail on a multi-BE cluster.
    sql "set enable_bucketed_hash_agg=true"
    sql "set be_number_for_test=1"
    sql "set agg_phase=1"
    sql "set parallel_pipeline_task_num=8"
    sql "set bucketed_agg_min_input_rows=0"
    // Bucketed agg is disabled while spill is enabled, so turn off fuzzy spill.
    sql "set enable_spill=false"
    sql "set enable_force_spill=false"
    sql "set bucketed_agg_max_group_keys=0"
    sql "set bucketed_agg_high_card_threshold=1.0"

    sql "DROP TABLE IF EXISTS collect_set_bucketed_agg_merge_t"
    sql """
        CREATE TABLE collect_set_bucketed_agg_merge_t (
            id INT NOT NULL,
            k INT NOT NULL,
            s VARCHAR(128) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 16
        PROPERTIES ('replication_num' = '1')
    """
    // every group key has rows in every tablet, so every sink instance holds
    // states for keys spread over all buckets
    sql """
        INSERT INTO collect_set_bucketed_agg_merge_t
        SELECT number, number % 3000,
               concat('value_', number % 13, '_', repeat('x', number % 37))
        FROM numbers("number" = "300000")
    """

    String query = """
        SELECT k, collect_set(s) cs, count(DISTINCT s) cnt
        FROM collect_set_bucketed_agg_merge_t GROUP BY k
    """
    explain {
        sql query
        contains("BUCKETED AGGREGATE")
    }

    for (int i = 0; i < 5; i++) {
        order_qt_bucketed """
            SELECT count(*), sum(size(cs)), sum(cnt),
                   sum(cast(murmur_hash3_64(concat(k, ':', array_join(array_sort(cs), ','))) AS LARGEINT))
            FROM (${query}) q
        """
    }

    // control: the same query without bucketed hash aggregation
    sql "set enable_bucketed_hash_agg=false"
    order_qt_no_bucketed """
        SELECT count(*), sum(size(cs)), sum(cnt),
               sum(cast(murmur_hash3_64(concat(k, ':', array_join(array_sort(cs), ','))) AS LARGEINT))
        FROM (${query}) q
    """
}
