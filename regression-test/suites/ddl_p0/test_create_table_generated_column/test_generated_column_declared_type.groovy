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

suite("test_generated_column_declared_type") {
    sql "DROP TABLE IF EXISTS test_generated_boolean_chain"
    sql """
        CREATE TABLE test_generated_boolean_chain (
            a INT,
            c BOOLEAN GENERATED ALWAYS AS (a),
            d INT GENERATED ALWAYS AS (c + 1),
            e INT GENERATED ALWAYS AS (d + 1)
        )
        DUPLICATE KEY(a)
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO test_generated_boolean_chain(a) VALUES (0), (2), (-2), (NULL)"
    order_qt_boolean_values """
        SELECT a, c, d, e, c + 1 AS expected_d, d + 1 AS expected_e
        FROM test_generated_boolean_chain
    """
    sql """
        INSERT INTO test_generated_boolean_chain(a)
        SELECT a + 10 FROM test_generated_boolean_chain WHERE a IS NOT NULL
    """
    order_qt_boolean_select """
        SELECT a, c, d, e, c + 1 AS expected_d, d + 1 AS expected_e
        FROM test_generated_boolean_chain
    """

    sql "DROP TABLE IF EXISTS test_generated_numeric_chain"
    sql """
        CREATE TABLE test_generated_numeric_chain (
            id INT,
            x DOUBLE,
            c INT GENERATED ALWAYS AS (x),
            d DOUBLE GENERATED ALWAYS AS (c + 0.25),
            e INT GENERATED ALWAYS AS (d),
            f DOUBLE GENERATED ALWAYS AS (e + 0.5)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_generated_numeric_chain(id, x)
        VALUES (1, 1.7), (2, -1.7), (3, 0), (4, NULL)
    """
    order_qt_numeric_values """
        SELECT *, c + 0.25 AS expected_d, CAST(d AS INT) AS expected_e, e + 0.5 AS expected_f
        FROM test_generated_numeric_chain
    """
    sql """
        INSERT OVERWRITE TABLE test_generated_numeric_chain(id, x)
        SELECT id, x FROM test_generated_numeric_chain
    """
    order_qt_numeric_overwrite """
        SELECT *, c + 0.25 AS expected_d, CAST(d AS INT) AS expected_e, e + 0.5 AS expected_f
        FROM test_generated_numeric_chain
    """
    streamLoad {
        table 'test_generated_numeric_chain'
        set 'column_separator', ','
        set 'columns', 'id, raw, x = raw / 3.0'
        file 'gen_col_data.csv'
        time 10000
    }
    sql "sync"
    order_qt_numeric_stream_load """
        SELECT *, c + 0.25 AS expected_d, CAST(d AS INT) AS expected_e, e + 0.5 AS expected_f
        FROM test_generated_numeric_chain
    """
}
