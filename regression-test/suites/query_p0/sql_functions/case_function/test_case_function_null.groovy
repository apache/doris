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

suite("test_case_function_null", "query,p0,arrow_flight_sql") {
    sql """ drop table if exists case_null0 """
    sql """ create table case_null0 (
                `c0` decimalv3(17, 1) NULL,
                `c1` boolean NOT NULL,
                `c2` date NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`c0`, `c1`, `c2`)
            DISTRIBUTED BY RANDOM BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
    """

    sql """ drop table if exists `case_null1` """
    sql """
        CREATE TABLE `case_null1` (
          `c0` varchar(7) NOT NULL DEFAULT ""
        ) ENGINE=OLAP
        AGGREGATE KEY(`c0`)
        DISTRIBUTED BY RANDOM BUCKETS 24
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ drop table if exists `case_null2` """
    sql """
        CREATE TABLE `case_null2` (
          `c0` tinyint(4) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`c0`)
        DISTRIBUTED BY HASH(`c0`) BUCKETS 20
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    def tables = ["case_null0", "case_null1", "case_null2"]

    for (String tableName in tables) {
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '\t'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${tableName}.csv"""

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
            }
        }
    }

    qt_sql_case0 """
    SELECT * FROM (
        SELECT
            case_null0.c2 c2,
            case_null0.c1 c1
        FROM
            case_null0,
            case_null1
        WHERE
            CASE
                (
                    CAST(TIMESTAMP '1970-10-27 14:44:27' AS DATE) BETWEEN DATE '1970-08-05'
                    AND DATE '1970-07-12'
                )
                WHEN CAST(NULL AS BOOLEAN) THEN (NULL IN (NULL))
                WHEN (('쬊o') !=('Sf^M7mir')) THEN (
                    ('0.24865') LIKE (
                        CASE
                            false
                            WHEN case_null0.c1 THEN case_null1.c0
                            WHEN case_null0.c1 THEN case_null1.c0
                        END
                    )
                )
            END
        GROUP BY
            case_null0.c2,
            case_null0.c1,
            case_null1.c0
        UNION
        SELECT
            case_null0.c2 c2,
            case_null0.c1 c1
        FROM
            case_null0,
            case_null1
        WHERE
            (
                NOT CASE
                    (
                        CAST(TIMESTAMP '1970-10-27 14:44:27' AS DATE) BETWEEN DATE '1970-08-05'
                        AND DATE '1970-07-12'
                    )
                    WHEN CAST(NULL AS BOOLEAN) THEN (NULL IN (NULL))
                    WHEN (('쬊o') !=('Sf^M7mir')) THEN (
                        ('0.24865') LIKE (
                            CASE
                                false
                                WHEN case_null0.c1 THEN case_null1.c0
                                WHEN case_null0.c1 THEN case_null1.c0
                            END
                        )
                    )
                END
            )
        GROUP BY
            case_null0.c2,
            case_null0.c1,
            case_null1.c0
        UNION
        SELECT
            case_null0.c2 c2,
            case_null0.c1 c1
        FROM
            case_null0,
            case_null1
        WHERE
            (
                (
                    CASE
                        (
                            CAST(TIMESTAMP '1970-10-27 14:44:27' AS DATE) BETWEEN DATE '1970-08-05'
                            AND DATE '1970-07-12'
                        )
                        WHEN CAST(NULL AS BOOLEAN) THEN (NULL IN (NULL))
                        WHEN (('쬊o') !=('Sf^M7mir')) THEN (
                            ('0.24865') like (
                                CASE
                                    false
                                    WHEN case_null0.c1 THEN case_null1.c0
                                    WHEN case_null0.c1 THEN case_null1.c0
                                END
                            )
                        )
                    END
                ) IS NULL
            )
        GROUP BY
            case_null0.c2,
            case_null0.c1,
            case_null1.c0
        ) a
        order BY
            c2,
            c1;
    """
    // There is a behavior change. The 0.4cast boolean used to be 0 in the past, but now it has changed to 1.
    // Therefore, we need to update the case accordingly.
    qt_sql_case1 """
        SELECT SUM(
            CASE (((NULL BETWEEN NULL AND NULL)) and (CAST(0.0 AS BOOLEAN)))
            WHEN ((CAST('-1530390546' AS VARCHAR)) LIKE ('-1678299490'))
            THEN (- (+ case_null2.c0))
            WHEN CASE (NULL IN (NULL))
            WHEN false THEN (case_null2.c0 NOT BETWEEN case_null2.c0 AND 1916517711)
            END THEN ((((case_null2.c0)*(1309461808)))/((- -267268292)))
            END)
        FROM case_null2;
    """
    // There is a behavior change. The 0.4cast boolean used to be 0 in the past, but now it has changed to 1.
    // Therefore, we need to update the case accordingly.
    qt_sql_case2 """
        SELECT SUM(CASE (((NULL BETWEEN NULL AND NULL)) and (CAST(0.0 AS BOOLEAN)))
            WHEN ((CAST('-1530390546' AS VARCHAR)) LIKE ('-1678299490'))
            THEN (- (+ case_null2.c0))
            END)
        FROM case_null2;
    """

    sql "SET experimental_enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // There is a behavior change. The 0.4cast boolean used to be 0 in the past, but now it has changed to 1.
    // Therefore, we need to update the case accordingly.
    qt_sql_case1 """
        SELECT SUM(
            CASE (((NULL BETWEEN NULL AND NULL)) and (CAST(0.0 AS BOOLEAN)))
            WHEN ((CAST('-1530390546' AS VARCHAR)) LIKE ('-1678299490'))
            THEN (- (+ case_null2.c0))
            WHEN CASE (NULL IN (NULL))
            WHEN false THEN (case_null2.c0 NOT BETWEEN case_null2.c0 AND 1916517711)
            END THEN ((((case_null2.c0)*(1309461808)))/((- -267268292)))
            END)
        FROM case_null2;
    """

    // There is a behavior change. The 0.4cast boolean used to be 0 in the past, but now it has changed to 1.
    // Therefore, we need to update the case accordingly.
    qt_sql_case2 """
        SELECT SUM(CASE (((NULL BETWEEN NULL AND NULL)) and (CAST(0.0 AS BOOLEAN)))
            WHEN ((CAST('-1530390546' AS VARCHAR)) LIKE ('-1678299490'))
            THEN (- (+ case_null2.c0))
            END)
        FROM case_null2;
    """


    qt_sql_case3 """SELECT COUNT(CASE (NOT (NOT true))  WHEN (((- 47960023)) IS NOT NULL) THEN NULL ELSE NULL END) from case_null1;"""

}
