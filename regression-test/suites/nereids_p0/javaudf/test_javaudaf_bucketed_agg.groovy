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

// Bucketed hash aggregation merges the live states of different sink instances
// without serializing them, which Java UDAFs cannot handle. A Java UDAF must
// therefore keep the regular aggregation plan even when bucketed agg applies.
suite("test_javaudaf_bucketed_agg") {
    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    // Bucketed agg needs a real single-BE cluster (the P0 CI runs one BE):
    // be_number_for_test can only disable it, so the BUCKETED AGGREGATE
    // assertions below fail on a multi-BE cluster.
    sql "set be_number_for_test=1"
    sql "set enable_bucketed_hash_agg=true"
    sql "set bucketed_agg_min_input_rows=0"
    // Bucketed agg is disabled while spill is enabled, so turn off fuzzy spill.
    sql "set enable_spill=false"
    sql "set enable_force_spill=false"
    sql "set bucketed_agg_max_group_keys=0"
    sql "set bucketed_agg_high_card_threshold=1.0"
    sql "set parallel_pipeline_task_num=2"

    sql "DROP TABLE IF EXISTS test_javaudaf_bucketed_agg_tbl"
    sql """
        CREATE TABLE test_javaudaf_bucketed_agg_tbl (
            id INT NOT NULL,
            k INT NOT NULL,
            v INT NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    // The same group key spreads over all tablets, so several sink instances
    // build a state for it and the source side has to merge them.
    sql """
        INSERT INTO test_javaudaf_bucketed_agg_tbl
        SELECT number, number % 3, number FROM numbers("number" = "100")
    """

    sql "DROP FUNCTION IF EXISTS test_javaudaf_bucketed_agg_sum(int)"
    sql """ CREATE AGGREGATE FUNCTION test_javaudaf_bucketed_agg_sum(int) RETURNS BigInt PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.MySumInt",
        "always_nullable"="false",
        "type"="JAVA_UDF"
    ); """

    // A builtin aggregate on the same shape still uses bucketed agg.
    explain {
        sql "SELECT k, sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k"
        contains("BUCKETED AGGREGATE")
    }
    explain {
        sql "SELECT k, test_javaudaf_bucketed_agg_sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k"
        notContains("BUCKETED AGGREGATE")
    }
    explain {
        sql """SELECT k, sum(v), test_javaudaf_bucketed_agg_sum(v)
            FROM test_javaudaf_bucketed_agg_tbl GROUP BY k"""
        notContains("BUCKETED AGGREGATE")
    }

    order_qt_udaf """
        SELECT k, test_javaudaf_bucketed_agg_sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k
    """
    order_qt_udaf_with_builtin """
        SELECT k, sum(v), test_javaudaf_bucketed_agg_sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k
    """

    // Force the one-phase plan so that only the translator fusion gate can keep
    // the Java UDAF away from bucketed agg.
    sql "set agg_phase=1"
    explain {
        sql "SELECT k, test_javaudaf_bucketed_agg_sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k"
        notContains("BUCKETED AGGREGATE")
    }
    order_qt_udaf_one_phase """
        SELECT k, test_javaudaf_bucketed_agg_sum(v) FROM test_javaudaf_bucketed_agg_tbl GROUP BY k
    """
}
