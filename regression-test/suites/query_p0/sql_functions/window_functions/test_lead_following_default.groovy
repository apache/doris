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

suite("test_lead_following_default", "p0") {
    sql "DROP TABLE IF EXISTS lead_following_default"
    sql """CREATE TABLE lead_following_default (
        id INT, p INT, u UUID, ud UUID, s STRING, sd STRING, v INT, vd INT
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql """INSERT INTO lead_following_default VALUES
        (1,0,'00000000-0000-0000-0000-000000000001','80000000-0000-0000-0000-000000000001','a','d1',10,101),
        (2,0,NULL,NULL,NULL,NULL,NULL,NULL),
        (3,0,'80000000-0000-0000-0000-000000000003','00000000-0000-0000-0000-000000000003','c','d3',30,103),
        (4,1,'ffffffff-ffff-ffff-ffff-ffffffffffff','00000000-0000-0000-0000-000000000004','d','d4',40,104),
        (5,1,NULL,'00000000-0000-0000-0000-000000000005',NULL,'d5',NULL,105)"""

    // An out-of-partition LEAD must use the current row's default, including the first row
    // of each partition. Exercise offset zero, in-range targets and targets beyond the partition.
    for (String column : ['u', 's', 'v']) {
        for (int offset : [0, 1, 3, 100]) {
            qt_defaults """SELECT id,
                LEAD(${column},${offset},${column}d) OVER (PARTITION BY p ORDER BY id),
                LAG(${column},${offset},${column}d) OVER (PARTITION BY p ORDER BY id),
                LEAD(${column},${offset},NULL) OVER (PARTITION BY p ORDER BY id)
                FROM lead_following_default ORDER BY id"""
        }
    }

    // The original crash also occurs with folded NULL UUID arguments.
    qt_null_uuid """SELECT id,
        LEAD(CAST(NULL AS UUID),100,CAST(NULL AS UUID)) OVER (ORDER BY id),
        LAG(CAST(NULL AS UUID),100,CAST(NULL AS UUID)) OVER (ORDER BY id)
        FROM lead_following_default ORDER BY id"""

    // Prefix initialization is still needed for aggregates sharing a FOLLOWING frame.
    for (int offset : [1, 100]) {
        String frame = "OVER (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND ${offset} FOLLOWING)"
        qt_following_frame """SELECT id,
            LEAD(s,${offset},sd) OVER (PARTITION BY p ORDER BY id),
            SUM(id) ${frame}, COUNT(*) ${frame},
            FIRST_VALUE(s) ${frame}, LAST_VALUE(s) ${frame}
            FROM lead_following_default ORDER BY id"""
    }
    qt_nth_following """SELECT id, NTH_VALUE(s,3) OVER
        (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)
        FROM lead_following_default ORDER BY id"""
}
