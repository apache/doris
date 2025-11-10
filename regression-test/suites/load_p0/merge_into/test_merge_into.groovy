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

suite("test_merge_into") {
    multi_sql """
    DROP TABLE IF EXISTS merge_into_source_table FORCE;
    DROP TABLE IF EXISTS merge_into_target_base_table FORCE;
    DROP TABLE IF EXISTS merge_into_target_seq_col_table FORCE;
    DROP TABLE IF EXISTS merge_into_target_seq_map_table FORCE;
    DROP TABLE IF EXISTS merge_into_target_gen_col_table FORCE;
    
    CREATE TABLE `merge_into_source_table` (
      `c1` int NULL,
      `c2` varchar(255) NULL
    ) ENGINE=OLAP
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    
    CREATE TABLE `merge_into_target_base_table` (
      `c1` int NULL,
      `c2` varchar(255) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`c1`)
    DISTRIBUTED BY HASH(`c1`)
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    
    CREATE TABLE `merge_into_target_seq_col_table` (
      `c1` int NULL,
      `c2` varchar(255) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`c1`)
    DISTRIBUTED BY HASH(`c1`) BUCKETS 10
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "function_column.sequence_type" = "date"
    );
    
    CREATE TABLE `merge_into_target_seq_map_table` (
      `c1` int NULL,
      `c2` varchar(255) NULL,
      `c3` date NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`c1`)
    DISTRIBUTED BY HASH(`c1`) BUCKETS 10
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "function_column.sequence_col" = "c3"
    );
    
    CREATE TABLE `merge_into_target_gen_col_table` (
      `c1` int NULL,
      `c2` int NULL,
      `c3` bigint AS (c1 + c2) NULL
    ) ENGINE=OLAP
    UNIQUE KEY(`c1`)
    DISTRIBUTED BY HASH(`c1`) BUCKETS 10
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    
    INSERT INTO merge_into_source_table VALUES (1, 12), (2, 22), (3, 33);
    INSERT INTO merge_into_target_base_table VALUES (1, 1), (2, 10);
    INSERT INTO merge_into_target_seq_col_table (c1, c2, __DORIS_SEQUENCE_COL__) VALUES (1, 1, '2020-02-02'), (2, 10, '2020-02-02');
    INSERT INTO merge_into_target_seq_map_table VALUES (1, 1, '2020-02-02'), (2, 10, '2020-02-02');
    INSERT INTO merge_into_target_gen_col_table (c1, c2) VALUES (1, 1), (2, 10);
    
    SYNC;
    """

    // base merge
    sql """
        WITH tmp AS (SELECT * FROM merge_into_source_table)
        MERGE INTO merge_into_target_base_table t1
        USING tmp t2
        ON t1.c1 = t2.c1
        WHEN MATCHED AND t1.c2 = 10 THEN DELETE
        WHEN MATCHED THEN UPDATE SET c2 = 10
        WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
    """

    sql """
        MERGE INTO merge_into_target_seq_col_table t1
        USING merge_into_source_table t2
        ON t1.c1 = t2.c1
        WHEN MATCHED AND t1.c2 = 10 THEN DELETE
        WHEN MATCHED THEN UPDATE SET c2 = 10
        WHEN NOT MATCHED THEN INSERT (c1, c2, __DORIS_SEQUENCE_COL__) VALUES(t2.c1, t2.c2, '2025-02-02')
    """

    sql """
        MERGE INTO merge_into_target_seq_map_table t1
        USING merge_into_source_table t2
        ON t1.c1 = t2.c1
        WHEN MATCHED AND t1.c2 = 10 THEN DELETE
        WHEN MATCHED THEN UPDATE SET c2 = 10
        WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2, '2025-02-02')
    """

    sql """
        MERGE INTO merge_into_target_gen_col_table t1
        USING merge_into_source_table t2
        ON t1.c1 = t2.c1
        WHEN MATCHED AND t1.c2 = 10 THEN DELETE
        WHEN MATCHED THEN UPDATE SET c2 = 10
        WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES(t2.c1, t2.c2)
    """

    sql """
        SYNC
    """

    order_qt_base_1 """
        SELECT * FROM merge_into_target_base_table;
    """
    order_qt_seq_col_1 """
        SELECT * FROM merge_into_target_seq_col_table;
    """
    order_qt_seq_map_1 """
        SELECT * FROM merge_into_target_seq_map_table;
    """
    order_qt_gen_col_1 """
        SELECT * FROM merge_into_target_gen_col_table;
    """

    // target has seq col but insert without seq col
    test {
        sql """
            MERGE INTO merge_into_target_seq_col_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c2 = 10
            WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES(t2.c1, t2.c2)
        """

        exception """has sequence column, need to specify the sequence column"""
    }

    // target has generated col, update try to update it
    test {
        sql """
            MERGE INTO merge_into_target_gen_col_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c2 = 10, c3 = 10
            WHEN NOT MATCHED THEN INSERT (c1, c2) VALUES(t2.c1, t2.c2)
        """

        exception """The value specified for generated column 'c3'"""
    }

    // target has generated col, insert try to insert it explicitly
    test {
        sql """
            MERGE INTO merge_into_target_gen_col_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c2 = 10
            WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES(t2.c1, t2.c2, t2.c2)
        """

        exception """The value specified for generated column 'c3'"""
    }

    // update key column
    test {
        sql """
            MERGE INTO merge_into_target_base_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c1 = 10
            WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
        """

        exception """Only value columns of unique table could be updated"""
    }

    // update not exist column
    test {
        sql """
            MERGE INTO merge_into_target_base_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c4 = 10
            WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
        """

        exception """unknown column in assignment list: c4"""
    }

    // insert not exist column
    test {
        sql """
            MERGE INTO merge_into_target_base_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c2 = 10
            WHEN NOT MATCHED THEN INSERT (c1, c2, c3) VALUES(t2.c1, t2.c2, t2.c2)
        """

        exception """unknown column in target table: c3"""
    }

    // matched clause without predicate in the middle
    test {
        sql """
            MERGE INTO merge_into_target_base_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED THEN UPDATE SET c2 = 10
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
        """

        exception """Only the last matched clause could without case predicate"""
    }

    // not matched clause without predicate in the middle
    test {
        sql """
            MERGE INTO merge_into_target_base_table t1
            USING merge_into_source_table t2
            ON t1.c1 = t2.c1
            WHEN MATCHED AND t1.c2 = 10 THEN DELETE
            WHEN MATCHED THEN UPDATE SET c2 = 10
            WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
            WHEN NOT MATCHED THEN INSERT VALUES(t2.c1, t2.c2)
        """

        exception """Only the last not matched clause could without case predicate"""
    }
}