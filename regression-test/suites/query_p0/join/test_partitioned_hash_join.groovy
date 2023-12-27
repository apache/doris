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

suite("test_partitioned_hash_join", "query,p0,arrow_flight_sql") {
    sql "drop table if exists test_partitioned_hash_join_l"
    sql "drop table if exists test_partitioned_hash_join_r"
    sql """ create table test_partitioned_hash_join_l (
        kl1 int, vl1 int
    ) distributed by hash(vl1) properties("replication_num"="1");
    """
    sql """ create table test_partitioned_hash_join_r (
        kr1 int, vr1 int
    ) distributed by hash(vr1) properties("replication_num"="1");
    """
    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table "test_partitioned_hash_join_r"

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','
        set 'timeout', '72000'
        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """test_partitioned_hash_join_r.csv"""
        time 3000 // limit inflight 3s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }
    sql """ set enable_profile = 1; """
    sql "insert into test_partitioned_hash_join_l values (100, 1100), (0, 10), (1, 110), (255, 2550)";

    sql """ sync """

    qt_partitioned_hash_join1 """
        select
            /*+SET_VAR(disable_join_reorder=true,experimental_enable_pipeline_engine=false, parallel_fragment_exec_instance_num=1, partitioned_hash_join_rows_threshold = 1)*/
            kl1
        from
            test_partitioned_hash_join_l
        where
            kl1 in (
                select
                    kr1
                from
                    test_partitioned_hash_join_r
                order by
                    kr1
            ) order by kl1;
    """
}