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

suite("partition_filter_selectivity") {
    sql "DROP TABLE IF EXISTS partition_filter_selectivity"
    sql """
        CREATE TABLE partition_filter_selectivity (
            dt DATE NOT NULL,
            id INT NOT NULL,
            payload INT NOT NULL
        )
        DUPLICATE KEY(dt, id)
        PARTITION BY LIST(dt) (
            PARTITION p1 VALUES IN ('2026-01-01'),
            PARTITION p2 VALUES IN ('2026-01-02', '2026-01-03'),
            PARTITION p3 VALUES IN ('2026-01-04')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO partition_filter_selectivity
        SELECT DATE_ADD('2026-01-01', INTERVAL (number % 4) DAY), number, (number DIV 4) % 2
        FROM numbers("number" = "4000")
    """
    awaitUntil(180) {
        def partitions = sql_return_maparray "SHOW PARTITIONS FROM partition_filter_selectivity"
        partitions.find { it.PartitionName == "p1" }.RowCount.toLong() == 1_000L
    }
    sql "ANALYZE TABLE partition_filter_selectivity WITH SYNC"

    explain {
        sql """
            OPTIMIZED PLAN
            SELECT id FROM partition_filter_selectivity WHERE dt = '2026-01-01'
        """
        contains "stats=1,000, projects="
    }

    explain {
        sql """
            OPTIMIZED PLAN
            SELECT id FROM partition_filter_selectivity WHERE dt = '2026-01-01' AND payload = 0
        """
        contains "stats=500, projects="
    }

    explain {
        sql """
            OPTIMIZED PLAN
            SELECT id FROM partition_filter_selectivity WHERE dt = '2026-01-02'
        """
        contains "PhysicalFilter"
    }

    order_qt_partition_filter_result """
        SELECT dt, COUNT(*)
        FROM partition_filter_selectivity
        WHERE (dt = '2026-01-01' AND payload = 0) OR dt = '2026-01-02'
        GROUP BY dt
    """
}
