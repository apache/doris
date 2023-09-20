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

suite("test_repeat_operator") {
    sql """ DROP TABLE IF EXISTS REPEATNODE """
    sql """
       CREATE TABLE IF NOT EXISTS REPEATNODE (
              `k1` INT(11) NULL COMMENT "",
              `k2` INT(11) NULL COMMENT "",
              `k3` INT(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """ set forbid_unknown_col_stats = false """
    sql """
    INSERT INTO REPEATNODE (k1, k2, k3) VALUES
    (5, 10, 15),
    (8, 12, 6),
    (3, 7, 11),
    (9, 4, 14),
    (2, 13, 1),
    (6, 20, 16),
    (11, 17, 19),
    (7, 18, 8),
    (12, 9, 2),
    (4, 15, 10),
    (16, 3, 13),
    (10, 1, 7),
    (14, 5, 12),
    (19, 6, 4),
    (1, 2, 18),
    (13, 11, 3),
    (18, 8, 5),
    (15, 19, 9),
    (17, 14, 17),
    (20, 16, 20);
    """

    sql"""set enable_pipeline_engine = true; """

    qt_pipeline """
        SELECT k1, k2
        FROM REPEATNODE
        GROUP BY GROUPING SETS ((k1, k2), (k2), (k1), ())
        ORDER BY k1, k2;
    """
    qt_pipeline """
        SELECT k1, k2 , k3
        FROM REPEATNODE
        GROUP BY GROUPING SETS ((k1, k2 , k3), (k2 , k3), (k1), (k2))
        ORDER BY k1, k2,k3;
    """
    
    sql"""set experimental_enable_pipeline_x_engine=true;    """

    qt_pipelineX """
        SELECT k1, k2
        FROM REPEATNODE
        GROUP BY GROUPING SETS ((k1, k2), (k2), (k1), ())
        ORDER BY k1, k2;
    """
    qt_pipelineX """
        SELECT k1, k2 , k3
        FROM REPEATNODE
        GROUP BY GROUPING SETS ((k1, k2 , k3), (k2 , k3), (k1), (k2))
        ORDER BY k1, k2,k3;
    """


}