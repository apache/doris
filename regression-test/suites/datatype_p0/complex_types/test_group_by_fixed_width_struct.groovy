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

suite("test_group_by_fixed_width_struct") {
    sql "DROP TABLE IF EXISTS test_group_by_fixed_width_struct"

    sql """
        CREATE TABLE test_group_by_fixed_width_struct (
            id INT,
            grp INT,
            st_tiny STRUCT<v:TINYINT>,
            st_int STRUCT<v:INT>,
            st_fixed STRUCT<a:INT,b:BIGINT> NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO test_group_by_fixed_width_struct VALUES
            (1, 1, NAMED_STRUCT('v', 1), NAMED_STRUCT('v', 10), NAMED_STRUCT('a', 1, 'b', 10)),
            (2, 1, NAMED_STRUCT('v', 1), NAMED_STRUCT('v', 10), NAMED_STRUCT('a', 1, 'b', 10)),
            (3, 1, NAMED_STRUCT('v', 2), NAMED_STRUCT('v', 20), NAMED_STRUCT('a', 2, 'b', 20)),
            (4, 2, NAMED_STRUCT('v', 1), NAMED_STRUCT('v', 10), NAMED_STRUCT('a', NULL, 'b', 30)),
            (5, 2, NAMED_STRUCT('v', 1), NAMED_STRUCT('v', 10), NAMED_STRUCT('a', NULL, 'b', 30))
    """

    order_qt_group_by_tiny_struct """
        SELECT grp, st_tiny, COUNT(*)
        FROM test_group_by_fixed_width_struct
        GROUP BY grp, st_tiny
        ORDER BY grp, st_tiny
    """

    order_qt_group_by_int_struct """
        SELECT grp, st_int, COUNT(*)
        FROM test_group_by_fixed_width_struct
        GROUP BY grp, st_int
        ORDER BY grp, st_int
    """

    order_qt_group_by_fixed_struct """
        SELECT grp, st_fixed, COUNT(*)
        FROM test_group_by_fixed_width_struct
        GROUP BY grp, st_fixed
        ORDER BY grp, st_fixed
    """

    order_qt_distinct_fixed_struct """
        SELECT grp, SIZE(array_agg(DISTINCT st_fixed))
        FROM test_group_by_fixed_width_struct
        GROUP BY grp
        ORDER BY grp
    """

    order_qt_array_agg_distinct_struct """
        SELECT grp, SIZE(array_agg(DISTINCT st_int))
        FROM test_group_by_fixed_width_struct
        GROUP BY grp
        ORDER BY grp
    """

    order_qt_collect_list_distinct_struct """
        SELECT grp, SIZE(collect_list(DISTINCT st_int))
        FROM test_group_by_fixed_width_struct
        GROUP BY grp
        ORDER BY grp
    """

    order_qt_group_array_distinct_struct """
        SELECT grp, SIZE(group_array(DISTINCT st_int))
        FROM test_group_by_fixed_width_struct
        GROUP BY grp
        ORDER BY grp
    """
}
