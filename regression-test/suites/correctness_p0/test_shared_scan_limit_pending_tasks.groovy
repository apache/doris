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

suite("test_shared_scan_limit_pending_tasks") {
    sql "DROP TABLE IF EXISTS shared_scan_limit_t1"
    sql "DROP TABLE IF EXISTS shared_scan_limit_t3"
    sql "DROP TABLE IF EXISTS shared_scan_limit_t4"
    sql "DROP TABLE IF EXISTS shared_scan_limit_t5"

    sql """
        CREATE TABLE shared_scan_limit_t1 (
            pk INT,
            v1 BIGINT,
            v2 BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE shared_scan_limit_t3 (
            pk INT,
            v1 BIGINT,
            v2 BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE shared_scan_limit_t4 (
            pk INT,
            v1 BIGINT,
            v2 BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE shared_scan_limit_t5 (
            pk INT,
            v1 BIGINT,
            v2 BIGINT
        ) ENGINE=OLAP
        DUPLICATE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO shared_scan_limit_t1 VALUES
            (0,70,32163), (1,-71,NULL), (2,-85,-222125), (3,26,22867),
            (4,17,NULL), (5,NULL,6236439), (6,-16907,-24720), (7,26877,1893),
            (8,30405,9), (9,5873343,66), (10,-17,-47), (11,NULL,20987),
            (12,NULL,-4672), (13,NULL,3551614), (14,28521,-1372), (15,NULL,NULL),
            (16,38,NULL), (17,NULL,-54), (18,NULL,-4686447), (19,NULL,-8420)
    """
    sql """
        INSERT INTO shared_scan_limit_t3 VALUES
            (0,28862,-25225), (1,5516858,-4609390), (2,-7815300,NULL),
            (3,NULL,-7685824), (4,22293,26373), (5,NULL,NULL), (6,1237,-31),
            (7,1606,3318374), (8,NULL,-21355), (9,NULL,-20127), (10,5413995,-3),
            (11,-4718995,NULL), (12,3179854,3733421), (13,18867,76),
            (14,15517,-4932405), (15,-30778,NULL), (16,NULL,-7258),
            (17,NULL,7520313), (18,6936629,NULL), (19,-45,6231596)
    """
    sql """
        INSERT INTO shared_scan_limit_t4 VALUES
            (0,5649134,4812327), (1,8249417,4670054), (2,106,6714155),
            (3,NULL,NULL), (4,-52850,-5113499), (5,-23911,-9637), (6,-26168,9),
            (7,-47,-28398), (8,-29518,-2365317), (9,20685,22956), (10,97,25099),
            (11,-32617,6143808), (12,NULL,NULL), (13,-52,NULL), (14,7680925,NULL),
            (15,10848,NULL), (16,64,-3394), (17,113,-12488), (18,-87,-12093),
            (19,NULL,22)
    """
    sql """
        INSERT INTO shared_scan_limit_t5 VALUES
            (0,-3018453,-5763927), (1,-60,-6233027), (2,78,-5304), (3,63,NULL),
            (4,23287,NULL), (5,98,7989209), (6,-61,-30493), (7,-6781665,-22321),
            (8,-5165806,-75), (9,NULL,31290), (10,1311226,7754904), (11,-72,54),
            (12,114,3741372), (13,NULL,NULL), (14,-40,2014555), (15,69,-4555977),
            (16,118,-8708), (17,NULL,NULL), (18,-123,NULL), (19,NULL,19843)
    """
    sql "sync"

    sql "SET enable_sql_cache = false"
    sql "SET experimental_enable_local_shuffle = false"
    sql "SET query_timeout = 60"

    // A shared LIMIT can be exhausted while another ScannerContext still owns pending tasks.
    // Every context must still reach EOS instead of leaving its pipeline task blocked forever.
    sql "SET parallel_pipeline_task_num = 2"
    qt_shared_scan_limit_parallel_2 """
        SELECT v1, pk
        FROM shared_scan_limit_t1 AS t1
        WHERE v2 > (
            SELECT SUM(v1)
            FROM shared_scan_limit_t5 AS t3
            WHERE EXISTS (
                SELECT pk
                FROM shared_scan_limit_t3 AS t4
                WHERE NOT EXISTS (
                    SELECT pk, v1, v1, v2, v1
                    FROM shared_scan_limit_t4 AS t5
                    WHERE v2 NOT IN (
                        SELECT pk FROM shared_scan_limit_t1 AS t6 ORDER BY pk
                    )
                ) AND v1 < 7
            )
        )
        ORDER BY v1, pk DESC
        LIMIT 6
    """

    sql "SET parallel_pipeline_task_num = 6"
    qt_shared_scan_limit_parallel_6 """
        SELECT v1, pk
        FROM shared_scan_limit_t1 AS t1
        WHERE v2 > (
            SELECT SUM(v1)
            FROM shared_scan_limit_t5 AS t3
            WHERE EXISTS (
                SELECT pk
                FROM shared_scan_limit_t3 AS t4
                WHERE NOT EXISTS (
                    SELECT pk, v1, v1, v2, v1
                    FROM shared_scan_limit_t4 AS t5
                    WHERE v2 NOT IN (
                        SELECT pk FROM shared_scan_limit_t1 AS t6 ORDER BY pk
                    )
                ) AND v1 < 7
            )
        )
        ORDER BY v1, pk DESC
        LIMIT 6
    """

    sql "SET parallel_pipeline_task_num = 8"
    qt_shared_scan_limit_parallel_8 """
        SELECT v1, pk
        FROM shared_scan_limit_t1 AS t1
        WHERE v2 > (
            SELECT SUM(v1)
            FROM shared_scan_limit_t5 AS t3
            WHERE EXISTS (
                SELECT pk
                FROM shared_scan_limit_t3 AS t4
                WHERE NOT EXISTS (
                    SELECT pk, v1, v1, v2, v1
                    FROM shared_scan_limit_t4 AS t5
                    WHERE v2 NOT IN (
                        SELECT pk FROM shared_scan_limit_t1 AS t6 ORDER BY pk
                    )
                ) AND v1 < 7
            )
        )
        ORDER BY v1, pk DESC
        LIMIT 6
    """
}
