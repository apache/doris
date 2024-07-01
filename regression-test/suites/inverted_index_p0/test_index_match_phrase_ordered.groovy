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


suite("test_index_match_phrase_ordered", "p0"){
    def indexTbName1 = "test_index_match_phrase_ordered"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `a` int(11) NULL COMMENT "",
      `b` string NULL COMMENT "",
      INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, "the quick brown fox jumped over the lazy dog"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, "the quick brown fox jumped over the lazy dog over"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, "the quick brown fox jumped over the lazy dog jumped"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (4, "the quick brown fox jumped over the lazy dog fox"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (5, "the quick brown fox jumped over the lazy dog brown"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (6, "the quick brown fox jumped over the lazy dog quick"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (7, "quick brown fox jumped over the lazy dog over"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (8, "quick brown fox jumped over the lazy dog jumped"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (9, "quick brown fox jumped over the lazy dog fox"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (10, "quick brown fox jumped over the lazy dog brown"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (11, "quick brown fox jumped over the lazy dog quick"); """

    try {
        sql "sync"

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1+'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the lazy ~1+ '; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the over ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the over ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~2'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~2+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~3'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the jumped ~3+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~4'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the fox ~4+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~5'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the brown ~5+'; """

        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~6'; """
        qt_sql """ select count() from ${indexTbName1} where b match_phrase 'the quick ~6+'; """
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}