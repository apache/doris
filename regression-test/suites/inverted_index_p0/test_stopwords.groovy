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


suite("test_stopwords", "p0"){
    def indexTbName = "test_stopwords"

    sql "DROP TABLE IF EXISTS ${indexTbName}"

    sql """
      CREATE TABLE ${indexTbName} (
        `a` int(11) NULL COMMENT "",
        `b` text NULL COMMENT "",
        `c` text NULL COMMENT "",
        INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "unicode") COMMENT '',
        INDEX c_idx (`c`) USING INVERTED PROPERTIES("parser" = "unicode", "stopwords" = "none") COMMENT ''
      ) ENGINE=OLAP
        DUPLICATE KEY(`a`)
        COMMENT "OLAP"
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """ INSERT INTO ${indexTbName} VALUES (1, "华夏智胜新税股票A", "华夏智胜新税股票A"); """
    sql """ INSERT INTO ${indexTbName} VALUES (2, "Life is like a box of chocolates, you never know what you are going to get. ", "Life is like a box of chocolates, you never know what you are going to get. "); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true """

        qt_sql """ select * from ${indexTbName} where b match 'a'; """
        qt_sql """ select * from ${indexTbName} where b match 'are'; """
        qt_sql """ select * from ${indexTbName} where b match 'to'; """

        qt_sql """ select * from ${indexTbName} where c match 'a'; """
        qt_sql """ select * from ${indexTbName} where c match 'are'; """
        qt_sql """ select * from ${indexTbName} where c match 'to'; """

        qt_sql """ select * from ${indexTbName} where b match_phrase 'like a box'; """
        qt_sql """ select * from ${indexTbName} where c match_phrase 'like a box'; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
