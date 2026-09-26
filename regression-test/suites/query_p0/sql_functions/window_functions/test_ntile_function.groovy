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
suite("test_ntile_function") {
    def tableName = "test_ntile_function"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS `${tableName}` (
            `k1` tinyint(4) NOT NULL COMMENT "",
            `k2` smallint(6) NOT NULL COMMENT "",
            `k3` smallint(6) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
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
        file 'test_ntile_function.csv'

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

    qt_select "select k1, k2, k3, ntile(3) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    qt_select "select k1, k2, k3, ntile(5) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    qt_select "select k2, k1, k3, ntile(3) over (order by k2,k3,k1) as ntile from ${tableName} order by k2, k3, k1, ntile;"
    qt_select "select k3, k2, k1, ntile(3) over (partition by k3 order by k3,k2,k1) as ntile from ${tableName} order by k3, k2, k1, ntile;"

    // the bucket may be a constant expression that folds to a positive integer
    qt_select_const_expr "select k1, k2, k3, ntile(1 + 1) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    qt_select_const_expr "select k1, k2, k3, ntile(cast(5 - 2 as bigint)) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    qt_select_const_expr "select k1, k2, k3, ntile(abs(-3)) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"

    test {
        sql "select k1, k2, k3, ntile(0) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "positive"
    }

    test {
        sql "select k1, k2, k3, ntile(k1) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket of NTILE must be a constant value"
    }

    test {
        sql "select k1, k2, k3, ntile(1 - 1) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }

    test {
        sql "select k1, k2, k3, ntile(1 - 2) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }

    test {
        sql "select k1, k2, k3, ntile(cast('abc' as int)) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }

    test {
        sql "select k1, k2, k3, ntile(170141183460469231731687303715884105727) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket of NTILE must be an integer within the range of BIGINT, but got LARGEINT"
    }

    // when constant folding is skipped, a literal bucket still works while an unfolded constant expression
    // is rejected by the planner instead of reaching the backend
    sql "set debug_skip_fold_constant=true"
    qt_select_skip_fold "select k1, k2, k3, ntile(2) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    test {
        sql "select k1, k2, k3, ntile(1 + 1) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }
    test {
        sql "select k1, k2, k3, ntile(cast('3' as int)) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }
    sql "set debug_skip_fold_constant=false"

    // FE can not evaluate `%` or crc32, but BE folds them when enable_fold_constant_by_be is set
    sql "set enable_fold_constant_by_be=true"
    qt_select_fold_by_be "select k1, k2, k3, ntile(5 % 3) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    qt_select_fold_by_be "select k1, k2, k3, ntile(crc32('a') % 3 + 3) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    test {
        sql "select k1, k2, k3, ntile(3 % 3) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }
    sql "set enable_fold_constant_by_be=false"
    test {
        sql "select k1, k2, k3, ntile(5 % 3) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }

    // the bucket is still checked when translating the plan if the rewrites that fold and check it are disabled
    sql "set disable_nereids_rules='REWRITE_PROJECT_EXPRESSION,REWRITE_WINDOW_EXPRESSION'"
    qt_select_no_window_rewrite "select k1, k2, k3, ntile(2) over (partition by k1 order by k2,k3) as ntile from ${tableName} order by k1, k2, k3, ntile;"
    test {
        sql "select k1, k2, k3, ntile(1 + 1) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket parameter of NTILE must be a constant positive integer"
    }
    sql "set disable_nereids_rules=''"

    test {
        sql "select k1, k2, k3, ntile(cast(3 as largeint)) over (partition by k1 order by k2) as ntile from ${tableName} order by k1, k2, k3 desc;"
        exception "The bucket of NTILE must be an integer within the range of BIGINT, but got LARGEINT"
    }
}




