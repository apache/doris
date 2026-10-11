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

suite("topn_array_timev2") {
    order_qt_reproduction """
        SELECT topn_array(v, 2), topn_array(v, 2, 100) FROM (
            SELECT CAST('01:00:00' AS TIME) v
            UNION ALL SELECT CAST('02:00:00' AS TIME)
        ) t
    """

    sql "DROP TABLE IF EXISTS test_topn_array_timev2"
    sql """
        CREATE TABLE test_topn_array_timev2 (
            id INT,
            g INT,
            value_string STRING
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_topn_array_timev2 VALUES
            (1, 1, '01:00:00.123456'), (2, 1, '01:00:00.123456'),
            (3, 1, '02:00:00.654321'), (4, 2, '-12:34:56.123456'),
            (5, 2, '-12:34:56.123456'), (6, 2, '27:00:00.654321'),
            (7, 2, '838:59:59'), (8, 2, '-838:59:59'), (9, 1, NULL), (10, 3, NULL)
    """

    [0, 3, 6].each { scale ->
        "order_qt_timev2_${scale}" """
            SELECT topn_array(v, 1), topn_array(v, 1, 100),
                   topn_array(v, 2), topn_array(v, 2, 100),
                   topn_array(v, 10), topn_array(v, 10, 100),
                   topn_array(v, 2)[1]
            FROM (SELECT CAST(value_string AS TIME(${scale})) v FROM test_topn_array_timev2) t
        """
        "order_qt_grouped_${scale}" """
            SELECT g, topn_array(v, 2), topn_array(v, 2, 100)
            FROM (SELECT g, CAST(value_string AS TIME(${scale})) v FROM test_topn_array_timev2) t
            GROUP BY g
        """
        "order_qt_distinct_${scale}" """
            SELECT topn_array(DISTINCT v, 10), topn_array(DISTINCT v, 10, 100)
            FROM (SELECT CAST(value_string AS TIME(${scale})) v FROM test_topn_array_timev2) t
        """
        "order_qt_state_${scale}" """
            SELECT topn_array_merge(topn_array_state(CAST(value_string AS TIME(${scale})), 2)),
                   topn_array_merge(topn_array_state(CAST(value_string AS TIME(${scale})), 2, 100))
            FROM test_topn_array_timev2
        """
        "order_qt_null_${scale}" """
            SELECT topn_array(CAST(NULL AS TIME(${scale})), 2),
                   topn_array(CAST(NULL AS TIME(${scale})), 2, 100)
        """
        "order_qt_empty_${scale}" """
            SELECT topn_array(v, 2), topn_array(v, 2, 100)
            FROM (SELECT CAST(value_string AS TIME(${scale})) v
                  FROM test_topn_array_timev2 WHERE id < 0) t
        """
    }
}
