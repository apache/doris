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

suite("prune_predicates_mv_test") {
    String currentDb = context.config.getDbNameByFile(context.file)
    sql """
        drop table if exists base_t;
        CREATE TABLE base_t (
            top_asset    varchar(64) NOT NULL,
            tag_key      int         NOT NULL,
            tag_value    int         NOT NULL,
            frame_count  int         NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(top_asset, tag_key, tag_value)
        AUTO PARTITION BY LIST (tag_key) ()
        DISTRIBUTED BY HASH(top_asset) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        );        

        INSERT INTO base_t VALUES
        ('a', 1, 100, 5),  ('a', 1, 101, 0),
        ('a', 2, 200, 7),  ('a', 2, 201, 0),
        ('a', 3, 300, 0), 
        ('a', 4, 400, 9),
        ('a', 5, 500, 1),
        ('a', 6, 600, 2);
    """

    // case 1:
    def mv_1 = """
        SELECT top_asset, tag_key, SUM(frame_count) AS frame_count
        FROM base_t
        WHERE frame_count != 0
        GROUP BY top_asset, tag_key;
    """

    def query_1 = """
        SELECT  /*+ USE_MV(mv_1) */ tag_key FROM base_t
        WHERE tag_key IN (1, 2, 3) AND frame_count != 0
        GROUP BY tag_key
        ORDER BY tag_key;
    """

    //Execute (force rewrite):
    //1. verify result is correct
    //2. verify shape plan contains filter
    //3. verify rewrite succeeded: async_mv_rewrite_success

    // The base table is partitioned and the predicate was removed after partition prune;
    // the MV is not partitioned, so verify the MV did not drop the predicate.
    async_mv_rewrite_success(currentDb, mv_1, query_1, "mv_1")
    order_qt_mv_1 query_1
    explain {
        sql "shape plan ${query_1}"
        contains "filter"
    }

    // case2: the MV is also partitioned; verify that partition pruning on the MV
    // is performed and the always-true predicate is removed after the prune.

    def async_partition_mv_rewrite_success = { db, mv_sql, query_sql, mv_name, partition, expected_pre_rewrite_strategys = [] ->
        if (!mvShouldContinueCheck(expected_pre_rewrite_strategys)) {
            return;
        }
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        ${partition}
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """
        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        // force meta sync to avoid stale meta data on follower fe
        sql """sync;"""
        mv_rewrite_success(query_sql, mv_name, true, expected_pre_rewrite_strategys)
    }

    sql """
         drop table if exists base_t2;
         CREATE TABLE base_t2 (
             top_asset    varchar(64) NOT NULL,
             tag_key      int         NOT NULL,
             tag_value    int         NOT NULL,
             frame_count  int         NOT NULL
         ) ENGINE=OLAP
         UNIQUE KEY(top_asset, tag_key, tag_value)
         AUTO PARTITION BY LIST (tag_key) ()
         DISTRIBUTED BY HASH(top_asset) BUCKETS 4
         PROPERTIES (
             "enable_unique_key_merge_on_write" = "true",
             "replication_num" = "1"
         );
         
         INSERT INTO base_t2 VALUES
         ('a', 1, 100, 5),  ('a', 1, 101, 0),
         ('a', 2, 200, 7),  ('a', 2, 201, 0),
         ('a', 3, 300, 0),
         ('a', 4, 400, 9),
         ('a', 5, 500, 1),
         ('a', 6, 600, 2);
    """
    def mv_2 = """
         SELECT top_asset, tag_key, SUM(frame_count) AS frame_count
         FROM base_t2
         WHERE frame_count != 0
         GROUP BY top_asset, tag_key;
    """
    def query_2 = """
         SELECT /*+use_mv(mv_2)*/ tag_key FROM base_t2
         WHERE tag_key IN (1, 2, 3) AND frame_count != 0
         GROUP BY tag_key
         ORDER BY tag_key;
    """

    async_partition_mv_rewrite_success(currentDb, mv_2, query_2, "mv_2", "PARTITION BY (tag_key)")
    order_qt_mv_2 query_2
    explain {
        sql "physical plan ${query_2}"
        contains "partitions(2/6)"
        notContains "PhysicalFilter"
    }

    sql """
        drop table if exists base_t3;
        CREATE TABLE base_t3 (
            top_asset    varchar(64) NOT NULL,
            tag_key      int         NOT NULL,
            tag_value    int         NOT NULL,
            frame_count  int         NOT NULL
        ) ENGINE=OLAP
        duplicate KEY(top_asset, tag_key, tag_value)
        AUTO PARTITION BY LIST (tag_key) ()
        DISTRIBUTED BY HASH(top_asset) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        );

        INSERT INTO base_t3 VALUES
        ('a', 1, 100, 5),  ('a', 1, 101, 0),
        ('a', 2, 200, 7),  ('a', 2, 201, 0),
        ('a', 3, 300, 0),  
        ('a', 4, 400, 9),
        ('a', 5, 500, 1),
        ('a', 6, 600, 2);
    """
    create_sync_mv(currentDb, "base_t3", "mv_3", """
        SELECT top_asset as mv_ta, tag_key as mv_tk, SUM(frame_count) AS mv_sum
        FROM base_t3
        GROUP BY top_asset, tag_key;
    """)

    def query_3 = """
        SELECT top_asset as mv_ta, tag_key as mv_tk, SUM(frame_count) AS mv_sum
        FROM base_t3
        where tag_key in (1,2,3)
        GROUP BY top_asset, tag_key;
    """
    mv_rewrite_success(query_3, "mv_3")
    order_qt_query_3 query_3
    explain {
        sql "physical plan ${query_3}"
        notContains "PhysicalFilter"
    }
}