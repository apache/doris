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


suite("test_index_delete", "p0") {
    def indexTbName1 = "test_index_delete"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
        `a` int(11) NULL COMMENT "",
        `b` text NULL COMMENT "",
        INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, "1"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, "1"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, "1"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (4, "2"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (5, "2"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (6, "2"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (7, "3"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (8, "3"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (9, "3"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (10, "4"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true """

        sql """ delete from ${indexTbName1} where a >= 9; """
        sql "sync"

        qt_sql """ select count() from ${indexTbName1} where a >= 1 and a <= 10; """
        qt_sql """ select count() from ${indexTbName1} where a >= 1; """
        qt_sql """ select count() from ${indexTbName1} where a <= 10; """
        
        sql """ delete from ${indexTbName1} where b = '3'; """
        sql "sync"

        qt_sql """ select count() from ${indexTbName1} where a >= 1; """
        qt_sql """ select count() from ${indexTbName1} where b match '3'; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}