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

suite("test_typeof") {
    sql "DROP TABLE IF EXISTS test_typeof"
    sql """
        CREATE TABLE test_typeof (
            id INT NOT NULL,
            i INT,
            v VARCHAR(10),
            c CHAR(4),
            d DECIMAL(12,3),
            a ARRAY<INT>,
            m MAP<STRING,INT>,
            s STRUCT<x:INT,y:VARCHAR(10)>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_typeof VALUES
            (1, 7, 'cat', 'cat', 1.25, [1, NULL], MAP('k', 1), {1, 'cat'}),
            (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    """

    def originalSkipFold = sql("SHOW VARIABLES LIKE 'debug_skip_fold_constant'")[0][1]
    def originalFoldByBe = sql("SHOW VARIABLES LIKE 'enable_fold_constant_by_be'")[0][1]
    try {
        [false, true].each { skipFold ->
            [false, true].each { foldByBe ->
                sql "SET debug_skip_fold_constant = ${skipFold}"
                sql "SET enable_fold_constant_by_be = ${foldByBe}"

                qt_literals """
                SELECT typeof(NULL), typeof(''), typeof('cat'),
                    typeof(CAST(NULL AS BOOLEAN)), typeof(CAST(NULL AS TINYINT)),
                    typeof(CAST(NULL AS SMALLINT)), typeof(CAST(NULL AS INT)),
                    typeof(CAST(NULL AS BIGINT)), typeof(CAST(NULL AS LARGEINT)),
                    typeof(CAST(NULL AS FLOAT)), typeof(CAST(NULL AS DOUBLE)),
                    typeof(CAST(NULL AS VARCHAR(10))), typeof(CAST(NULL AS CHAR(4))),
                    typeof(CAST(NULL AS STRING)), typeof(X'00'),
                    typeof(CAST(NULL AS DECIMAL(5,1))), typeof(CAST(NULL AS DECIMAL(12,3))),
                    typeof(CAST(NULL AS DECIMAL(38,10))), typeof(CAST(NULL AS DATEV2)),
                    typeof(CAST(NULL AS DATETIME(6))), typeof(CAST(NULL AS TIMESTAMP_NS)),
                    typeof(CAST(NULL AS TIMESTAMPTZ(3))),
                    typeof(CAST(NULL AS IPV4)), typeof(CAST(NULL AS IPV6)),
                    typeof(CAST(NULL AS JSON)), typeof(CAST(NULL AS VARIANT)),
                    typeof(TO_BITMAP(1)), typeof(HLL_HASH('cat'))
                """

                qt_nested """
                SELECT typeof(CAST(NULL AS ARRAY<ARRAY<INT>>)),
                    typeof(CAST(NULL AS MAP<STRING,ARRAY<DECIMAL(12,3)>>)),
                    typeof(CAST(NULL AS STRUCT<x:INT,y:VARCHAR(10)>)),
                    typeof(CAST(NULL AS ARRAY<STRUCT<x:INT,y:MAP<STRING,INT>>>)),
                    typeof(typeof(v)), typeof(CAST(v AS CHAR(4)))
                FROM test_typeof WHERE id = 1
                """

                order_qt_columns """
                SELECT id, typeof(i), typeof(v), typeof(c), typeof(d),
                    typeof(a), typeof(m), typeof(s), typeof(i + CAST(1 AS INT)),
                    typeof(i) IS NULL
                FROM test_typeof ORDER BY id
                """

                order_qt_aggregate_cardinality """
                SELECT 'nonempty', typeof(sum(i)) FROM test_typeof
                UNION ALL
                SELECT 'empty', typeof(sum(i)) FROM test_typeof WHERE id < 0
                ORDER BY 1
                """

                order_qt_empty """
                SELECT typeof(v) FROM test_typeof WHERE id < 0 ORDER BY id
                """
            }
        }
    } finally {
        sql "SET debug_skip_fold_constant = ${originalSkipFold}"
        sql "SET enable_fold_constant_by_be = ${originalFoldByBe}"
    }

    test {
        sql "SELECT typeof()"
        exception "arity"
    }
    test {
        sql "SELECT typeof(1, 2)"
        exception "arity"
    }
    test {
        sql "SELECT id FROM test_typeof WHERE typeof(sum(i)) = 'bigint'"
        exception "LOGICAL_FILTER can not contains AggregateFunction expression"
    }
    test {
        sql "SELECT id FROM test_typeof WHERE typeof(row_number() OVER ()) = 'bigint'"
        exception "LOGICAL_FILTER can not contains WindowExpression expression"
    }
    test {
        sql "SELECT typeof(CAST(TRUE AS DATE))"
        exception "cannot cast BOOLEAN to DATEV2"
    }
}
