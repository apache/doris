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

}
