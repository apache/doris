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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/delete/delete-manual.md") {
    try {
        multi_sql """
            CREATE DATABASE IF NOT EXISTS test_db;
            USE test_db;
            DROP TABLE IF EXISTS my_table;
            CREATE TABLE IF NOT EXISTS my_table(
                k1 INT,
                k2 STRING
            )
            PARTITION BY RANGE(k1) (
                PARTITION p1 VALUES LESS THAN (1),
                PARTITION p2 VALUES LESS THAN (2)
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """

        sql """
            DELETE FROM my_table PARTITION p1
                WHERE k1 = 3;
        """
        sql """
            DELETE FROM my_table PARTITION p1
            WHERE k1 = 3 AND k2 = "abc";
        """
        sql """
            DELETE FROM my_table PARTITIONS (p1, p2)
            WHERE k1 = 3 AND k2 = "abc";
        """

        multi_sql """
            DROP TABLE IF EXISTS t1;
            DROP TABLE IF EXISTS t2;
            DROP TABLE IF EXISTS t3;
        """
        multi_sql """
            -- Create t1, t2, t3 tables
            CREATE TABLE t1
              (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
            UNIQUE KEY (id)
            DISTRIBUTED BY HASH (id)
            PROPERTIES('replication_num'='1', "function_column.sequence_col" = "c4");
            
            CREATE TABLE t2
              (id INT, c1 BIGINT, c2 STRING, c3 DOUBLE, c4 DATE)
            DISTRIBUTED BY HASH (id)
            PROPERTIES('replication_num'='1');
            
            CREATE TABLE t3
              (id INT)
            DISTRIBUTED BY HASH (id)
            PROPERTIES('replication_num'='1');
            
            -- insert data
            INSERT INTO t1 VALUES
              (1, 1, '1', 1.0, '2000-01-01'),
              (2, 2, '2', 2.0, '2000-01-02'),
              (3, 3, '3', 3.0, '2000-01-03');
            
            INSERT INTO t2 VALUES
              (1, 10, '10', 10.0, '2000-01-10'),
              (2, 20, '20', 20.0, '2000-01-20'),
              (3, 30, '30', 30.0, '2000-01-30'),
              (4, 4, '4', 4.0, '2000-01-04'),
              (5, 5, '5', 5.0, '2000-01-05');
            
            INSERT INTO t3 VALUES
              (1),
              (4),
              (5);
            
            -- remove rows from t1
            DELETE FROM t1
              USING t2 INNER JOIN t3 ON t2.id = t3.id
              WHERE t1.id = t2.id;
        """
        order_qt_sql "SELECT * FROM t1"

        sql "DROP TABLE IF EXISTS test_tbl"
        sql "CREATE TABLE IF NOT EXISTS test_tbl LIKE my_table"
        sql "delete from test_tbl PARTITION p1 where k1 = 1;"
        sql "delete from test_tbl partition p1 where k1 > 80;"
        sql "show delete from test_db;"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/delete/delete-manual.md failed to exec, please fix it", t)
    }
}
