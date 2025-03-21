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

suite("test_aggregate_percentile_no_cast") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "set batch_size = 4096"

    sql "DROP TABLE IF EXISTS percentile_test_db"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db (
            id int,
	          level tinyint
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO percentile_test_db values(1,10), (2,8), (2,114) ,(3,10) ,(5,29) ,(6,101)"

    qt_select "select id,percentile(level,0.5) , percentile(level,0.55) , percentile(level,0.805) , percentile_array(level,[0.5,0.55,0.805])from percentile_test_db group by id order by id"

    sql "DROP TABLE IF EXISTS percentile_test_db"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db (
            id int,
	          level smallint
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO percentile_test_db values(1,10), (2,8), (2,114) ,(3,10) ,(5,29) ,(6,101)"

    qt_select "select id,percentile(level,0.5) , percentile(level,0.55) , percentile(level,0.805) , percentile_array(level,[0.5,0.55,0.805])from percentile_test_db group by id order by id"

    sql "DROP TABLE IF EXISTS percentile_test_db"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db (
            id int,
	          level int
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO percentile_test_db values(1,10), (2,8), (2,114) ,(3,10) ,(5,29) ,(6,101)"
  
    qt_select "select id,percentile(level,0.5) , percentile(level,0.55) , percentile(level,0.805) , percentile_array(level,[0.5,0.55,0.805])from percentile_test_db group by id order by id"

    sql "DROP TABLE IF EXISTS percentile_test_db"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db (
            id int,
	          level bigint
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO percentile_test_db values(1,10), (2,8), (2,114) ,(3,10) ,(5,29) ,(6,101)"
    qt_select "select id,percentile(level,0.5) , percentile(level,0.55) , percentile(level,0.805) , percentile_array(level,[0.5,0.55,0.805])from percentile_test_db group by id order by id"

   sql "DROP TABLE IF EXISTS percentile_test_db"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db (
            id int,
	          level largeint
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    sql "INSERT INTO percentile_test_db values(1,10), (2,8), (2,114) ,(3,10) ,(5,29) ,(6,101)"
    qt_select "select id,percentile(level,0.5) , percentile(level,0.55) , percentile(level,0.805) , percentile_array(level,[0.5,0.55,0.805])from percentile_test_db group by id order by id"

    sql "DROP TABLE IF EXISTS percentile_test_db2"
    sql """
        CREATE TABLE IF NOT EXISTS percentile_test_db2 (
            id int,
	          level double
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1"
        ) 
        """
    explain {
        sql("""select percentile_array(level,[0.5,0.55,0.805])from percentile_test_db2;""")
        notContains("cast")
    }
    sql "INSERT INTO percentile_test_db2 values(1,10.1), (2,8.2), (2,114.3) ,(3,10.4) ,(5,29.5) ,(6,101.6)"
    qt_select "select percentile_array(level,[0.5,0.55,0.805])from percentile_test_db2;"

    sql "DROP TABLE IF EXISTS TINYINTDATA_NOT_EMPTY_NOT_NULLABLE"
    sql """
    CREATE TABLE TINYINTDATA_NOT_EMPTY_NOT_NULLABLE(id INT, data TINYINT  NOT NULL) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql "DROP TABLE IF EXISTS TEMPDATA"
    sql """
    CREATE TABLE IF NOT EXISTS TEMPDATA(id INT, data INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """
    INSERT INTO TINYINTDATA_NOT_EMPTY_NOT_NULLABLE values (0, -1),(1, 0),(2, 1),(3, 2),(4, 12),(5, -128),(6, 127);
    """
    sql """
    INSERT INTO TEMPDATA values(1, 1); 
    """
    qt_test """
    SELECT ARG0,PERCENTILE(NULLABLE(ARG0),1) OVER(ORDER BY t.ARG0 ROWS BETWEEN  CURRENT ROW  AND  UNBOUNDED FOLLOWING ) as a,PERCENTILE(NULLABLE(ARG0),0.9999) OVER(ORDER BY t.ARG0 ROWS BETWEEN  CURRENT ROW  AND  UNBOUNDED FOLLOWING ) as b,PERCENTILE(NULLABLE(ARG0),0) OVER(ORDER BY t.ARG0 ROWS BETWEEN  CURRENT ROW  AND  UNBOUNDED FOLLOWING ) as c,PERCENTILE(NULLABLE(ARG0),0.0001) OVER(ORDER BY t.ARG0 ROWS BETWEEN  CURRENT ROW  AND  UNBOUNDED FOLLOWING ) as d FROM (SELECT TEMPDATA . data, TABLE0.ARG0 FROM TEMPDATA CROSS JOIN (SELECT data AS ARG0
 FROM TINYINTDATA_NOT_EMPTY_NOT_NULLABLE ) AS TABLE0) t  GROUP BY ARG0 order by ARG0;
    """
}
