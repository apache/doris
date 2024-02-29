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


suite("test_index_match_phrase_prefix_1", "p0"){
    def indexTbName1 = "test_index_match_phrase_prefix_1"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `a` int(11) NULL COMMENT "",
      `b` string NULL COMMENT "",
      `c` string NULL COMMENT "",
      `d` string NULL COMMENT "",
      INDEX b_idx (`b`) USING INVERTED COMMENT '',
      INDEX c_idx (`c`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
      INDEX d_idx (`d`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, "O1704361998540E2Cemx9S", "O1704361998540E2Cemx9S", "O1704361998540E2Cemx9S"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, "O1704361998540E2Cemx9S)123456789", "O1704361998540E2Cemx9S)123456789", "O1704361998540E2Cemx9S)123456789"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, "O1704361998540E2Cemx9S=123456789", "O1704361998540E2Cemx9S=123456789", "O1704361998540E2Cemx9S=123456789"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (4, "O1704361998540E2Cemx9S+123456789", "O1704361998540E2Cemx9S+123456789", "O1704361998540E2Cemx9S+123456789"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (5, "O1704361998540E2Cemx9S!123456789", "O1704361998540E2Cemx9S!123456789", "O1704361998540E2Cemx9S!123456789"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (6, "O1704361998540E2Cemx9S 123456789", "O1704361998540E2Cemx9S 123456789", "O1704361998540E2Cemx9S 123456789"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (7, "O1704361998540E2Cemx9S*123456789", "O1704361998540E2Cemx9S*123456789", "O1704361998540E2Cemx9S*123456789"); """

    try {
        sql "sync"

        qt_sql """ select count() from ${indexTbName1} where c match_phrase_prefix 'O1704361998540E2Cemx9S'; """
        qt_sql """ select count() from ${indexTbName1} where d match_phrase_prefix 'O1704361998540E2Cemx9S'; """

        qt_sql """ select count() from ${indexTbName1} where c match_phrase_prefix 'O1704361998540E2Cemx9S=123456789'; """
        qt_sql """ select count() from ${indexTbName1} where d match_phrase_prefix 'O1704361998540E2Cemx9S=123456789'; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}