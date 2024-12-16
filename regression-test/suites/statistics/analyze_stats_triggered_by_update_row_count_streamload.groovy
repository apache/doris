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

import java.util.Date
import java.util.stream.Collectors
import java.text.SimpleDateFormat

suite("test_analyze_triggered_by_update_row_count_streamload") {
    String tbl = "analyzetestlimited_agg_streamload"

    sql "show tables"

    sql """ DROP TABLE IF EXISTS ${tbl} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL,
            `v1` tinyint(4) SUM NULL,
            `v2` tinyint(4) REPLACE NULL,
            `v3` tinyint(4) REPLACE_IF_NOT_NULL NULL,
            `v4` smallint(6) REPLACE_IF_NOT_NULL NULL,
            `v5` int(11) REPLACE_IF_NOT_NULL NULL,
            `v6` bigint(20) REPLACE_IF_NOT_NULL NULL,
            `v7` largeint(40) REPLACE_IF_NOT_NULL NULL,
            `v8` datetime REPLACE_IF_NOT_NULL NULL,
            `v9` date REPLACE_IF_NOT_NULL NULL,
            `v10` char(10) REPLACE_IF_NOT_NULL NULL,
            `v11` varchar(6) REPLACE_IF_NOT_NULL NULL,
            `v12` decimal(27, 9) REPLACE_IF_NOT_NULL NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`, `k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "sync"

    sql """
        SET enable_nereids_planner=true;

    """

    sql """
        SET enable_fallback_to_original_planner=false;
        """

    sql """
         analyze table ${tbl} with sync;
        """

    Thread.sleep(1000 * 3)

    streamLoad {
        table "${tbl}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'strict_mode', 'true'

        file 'test_strict_mode_1.csv'
        time 10000 // limit inflight 10s
    }

    Thread.sleep(1000 * 2)

    def result1 = sql """
        show table stats ${tbl}
        """

    log.info("updated_rows: ${result1[0][0]}".toString())
    assertTrue(Integer.valueOf(result1[0][0]) == 2, "Streamload should update 2 rows")

    // test strict_mode success
    streamLoad {
        table "${tbl}"

        set 'column_separator', '\t'
        set 'columns', 'k1, k2, v2, v10, v11'
        set 'strict_mode', 'true'

        file 'test_strict_mode_2.csv'
        time 10000 // limit inflight 10s
    }

    Thread.sleep(1000 * 2)

    def result2 = sql """
        show table stats ${tbl}
        """

    log.info("updated_rows: ${result2[0][0]}".toString())
    assertTrue(Integer.valueOf(result2[0][0]) == 4, "Streamload should update 4 rows")
}
