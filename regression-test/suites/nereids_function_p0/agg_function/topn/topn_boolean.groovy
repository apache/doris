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

suite("topn_boolean") {
    order_qt_literals """
        SELECT topn_weighted(TRUE, 1, 2), topn_weighted(FALSE, 1, 2, 50),
               topn_array(TRUE, 2), topn_array(FALSE, 2, 50)
    """
    order_qt_null_literals """
        SELECT topn_weighted(CAST(NULL AS BOOLEAN), 1, 2),
               topn_weighted(CAST(NULL AS BOOLEAN), 1, 2, 50),
               topn_array(CAST(NULL AS BOOLEAN), 2),
               topn_array(CAST(NULL AS BOOLEAN), 2, 50)
    """

    sql "DROP TABLE IF EXISTS test_topn_boolean"
    sql """
        CREATE TABLE test_topn_boolean (
            id INT,
            flag BOOLEAN,
            weight BIGINT
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_topn_boolean VALUES
        (1, FALSE, 1), (2, FALSE, 1), (3, TRUE, 5),
        (4, NULL, 100), (5, TRUE, NULL)
    """

    order_qt_weighted """
        SELECT topn_weighted(flag, weight, 1), topn_weighted(flag, weight, 1, 50),
               topn_weighted(flag, weight, 2), topn_weighted(flag, weight, 2, 50),
               topn_weighted(flag, weight, 3), topn_weighted(flag, weight, 3, 50)
        FROM test_topn_boolean
    """
    order_qt_array """
        SELECT topn_array(flag, 1), topn_array(flag, 1, 50),
               topn_array(flag, 2), topn_array(flag, 2, 50),
               topn_array(flag, 3), topn_array(flag, 3, 50)
        FROM test_topn_boolean WHERE weight IS NOT NULL
    """
    order_qt_grouped """
        SELECT flag, topn_weighted(flag, weight, 2), topn_weighted(flag, weight, 2, 50),
               topn_array(flag, 2), topn_array(flag, 2, 50)
        FROM test_topn_boolean GROUP BY flag
    """
    order_qt_all_null """
        SELECT topn_weighted(flag, weight, 2), topn_weighted(flag, weight, 2, 50),
               topn_array(flag, 2), topn_array(flag, 2, 50)
        FROM test_topn_boolean WHERE flag IS NULL
    """
    order_qt_empty """
        SELECT topn_weighted(flag, weight, 2), topn_weighted(flag, weight, 2, 50),
               topn_array(flag, 2), topn_array(flag, 2, 50)
        FROM test_topn_boolean WHERE id < 0
    """
    order_qt_ties """
        SELECT topn_weighted(flag, 1, 2), topn_weighted(flag, 1, 2, 50),
               topn_array(flag, 2), topn_array(flag, 2, 50)
        FROM test_topn_boolean WHERE id IN (1, 3)
    """
}
