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
suite("test_select_stddev_variance_window") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tableName = "stddev_variance_window"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `k1` tinyint(4) NULL COMMENT "",
            `k2` smallint(6) NULL COMMENT "",
            `k3` int(11) NULL COMMENT "",
            `k4` bigint(20) NULL COMMENT "",
            `k5` decimal(9, 3) NULL COMMENT "",
            `k6` char(5) NULL COMMENT "",
            `k10` date NULL COMMENT "",
            `k11` datetime NULL COMMENT "",
            `k12` datev2 NULL COMMENT "",
            `k13` datetimev2 NULL COMMENT "",
            `k14` datetimev2(3) NULL COMMENT "",
            `k15` datetimev2(6) NULL COMMENT "",
            `k7` varchar(20) NULL COMMENT "",
            `k8` double NULL COMMENT "",
            `k9` float NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k12`, `k13`, `k14`, `k15`, `k7`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    streamLoad {
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '\t'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'test_stddev_variance_window.csv'

        time 10000 // limit inflight 10s

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
    sql "sync"

    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between current row and current row) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1) from  ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between current row and current row) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1) from  ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between current row and current row) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1) from  ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between current row and current row) from   ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1) from  ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_pop(k2) over (partition by k6 order by k1) from ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, stddev_samp(k2) over (partition by k6 order by k1) from ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_pop(k2) over (partition by k6 order by k1) from ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, variance_samp(k2) over (partition by k6 order by k1) from ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile(k2,0.8) over (partition by k6 order by k1) from ${tableName} order by k1;"

    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1 rows between 3 preceding and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1 rows between 3 preceding and 1 preceding) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1 rows between 3 preceding and 1 following) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1 rows between current row and current row) from  ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1 rows between current row and unbounded following) from ${tableName} order by k1;"
    // Nereids does't support window function
    // qt_select_default  "select k1, percentile_approx(k2,0.5,4096) over (partition by k6 order by k1) from ${tableName} order by k1;"
}





