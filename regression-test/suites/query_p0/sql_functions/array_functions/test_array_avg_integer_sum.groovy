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

suite("test_array_avg_integer_sum") {
    // array_avg adds up integers in the same type as avg(), so the large values cancel out
    // and the average of the sum 1 is 1/3.
    order_qt_const """
        SELECT array_avg(array(9223372036854775807, 1, -9223372036854775807)),
               array_avg(array(170141183460469231731687303715884105727, 1,
                               -170141183460469231731687303715884105727))
    """

    sql """ DROP TABLE IF EXISTS test_array_avg_integer_sum """
    sql """
        CREATE TABLE test_array_avg_integer_sum (
            id INT,
            b ARRAY<BIGINT>,
            l ARRAY<LARGEINT>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_array_avg_integer_sum VALUES
            (1, array(9223372036854775807, 1, -9223372036854775807),
                array(170141183460469231731687303715884105727, 1,
                      -170141183460469231731687303715884105727)),
            (2, array(9223372036854775807, NULL, 1, -9223372036854775807),
                array(170141183460469231731687303715884105727, NULL, 1,
                      -170141183460469231731687303715884105727)),
            (3, array(1, 2, 3), array(1, 2, 4)),
            (4, array(), array()),
            (5, NULL, NULL)
    """
    order_qt_column """
        SELECT id, array_avg(b), array_avg(l) FROM test_array_avg_integer_sum
    """

    // avg() over the same elements gives the same result.
    order_qt_same_as_avg_bigint """
        SELECT id, avg(x) FROM test_array_avg_integer_sum LATERAL VIEW explode(b) t AS x GROUP BY id
    """
    order_qt_same_as_avg_largeint """
        SELECT id, avg(x) FROM test_array_avg_integer_sum LATERAL VIEW explode(l) t AS x GROUP BY id
    """
}
