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

suite("convert_inner_join_to_semi_join") {

    sql "DROP TABLE IF EXISTS t1"
    sql "DROP TABLE IF EXISTS t2"

    sql """
        CREATE TABLE t1 (
            id1 int null,
            v1 int null
        )
        DUPLICATE KEY(id1)
        DISTRIBUTED BY HASH(id1) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE t2 (
            id2 int null,
            v2 int null
        )
        DUPLICATE KEY(id2)
        DISTRIBUTED BY HASH(id2) BUCKETS 3
        PROPERTIES ("replication_num" = "1")
    """

    sql "insert into t1 values (1, 10), (2, 20), (3, 30), (null, 40), (null, 50)"
    sql "insert into t2 values (1, 100), (null, 200), (null, 300), (5, 500)"

    // select distinct t1.id1 from t1 join t2 on t1.id1 = t2.id2
    // -> select distinct t1.id1 from t1 left semi join t2 on t1.id1 = t2.id2.
    // Rows with id1 = 2 and id1 = 3 have no match (2 and 3 are absent from t2), so only
    // id1 = 1 and the NULL keys (matched by the `<=>` join against the two NULL rows of
    // t2) survive.
    order_qt_distinct_inner_join """
        select distinct t1.id1 from t1 join t2 on t1.id1 <=> t2.id2 order by t1.id1
        """

    // Same DISTINCT query with plain `=`: NULL keys never match, so the result drops the
    // NULL row. This pins the difference between the two join predicates.
    order_qt_distinct_plain_equal """
        select distinct t1.id1 from t1 join t2 on t1.id1 = t2.id2 order by t1.id1
        """
}
