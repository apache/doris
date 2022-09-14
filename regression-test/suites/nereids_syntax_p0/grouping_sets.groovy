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

suite("test_nereids_grouping_sets") {

    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"

    sql "DROP TABLE IF EXISTS groupingSetsTable"

    sql """
        CREATE TABLE `groupingSetsTable` (
        `k1` bigint(20) NULL,
        `k2` bigint(20) NULL,
        `k3` bigint(20) NULL
        ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        INSERT INTO groupingSetsTable VALUES
            (1, 1, 1),
            (1, 1, 2),
            (1, 1, 3),
            (1, 0, null),
            (2, 2, 2),
            (2, 2, 4),
            (2, 2, 6),
            (2, 2, null),
            (3, 3, 3),
            (3, 3, 6),
            (3, 3, 9),
            (3, 0, null)
    """

    sql "SET enable_fallback_to_original_planner=false"

    // grouping
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1), (k2));";
    order_qt_select "select k1+1, grouping(k1+1) from groupingSetsTable group by grouping sets((k1), (k1+1), (k2));";
    order_qt_select "select k1+1, grouping(k1) from groupingSetsTable group by grouping sets((k1));";
    order_qt_select "select sum(k2), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1));";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1)) having (k1+1) > 1;";
    order_qt_select "select sum(k2+1), grouping(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1)) having (k1+1) > 1;";


    // grouping_id
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1), (k2));";
    order_qt_select "select k1+1, grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1), (k1+1), (k2));";
    order_qt_select "select k1+1, grouping_id(k1) from groupingSetsTable group by grouping sets((k1));";
    order_qt_select "select k1+1, grouping_id(k1, k2) from groupingSetsTable group by grouping sets((k1), (k2));";
    order_qt_select "select sum(k2), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1));";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1)) having (k1+1) > 1;";
    order_qt_select "select sum(k2+1), grouping_id(k1+1) from groupingSetsTable group by grouping sets((k1+1), (k1)) having (k1+1) > 1;";

}
