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


suite("test_index_skip_read_data", "p0"){
    def indexTbName1 = "test_index_skip_read_data_dup"
    def indexTbName2 = "test_index_skip_read_data_mow"
    def indexTbName3 = "test_index_skip_read_data_mor"


    // dup
    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `k1` int(11) NULL COMMENT "",
      `k2` varchar(20) NULL COMMENT "",
      `data` text NULL COMMENT "",
      INDEX idx_k1 (`k1`) USING INVERTED COMMENT '',
      INDEX idx_k2 (`k2`) USING BITMAP  COMMENT '',
      INDEX idx_data (`data`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`k1`, `k2`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "disable_auto_compaction" = "true"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, 20, 300); """
    sql """ INSERT INTO ${indexTbName1} VALUES (1, 20, 400); """

    qt_sql10 """ SELECT * FROM ${indexTbName1} ORDER BY k1,k2,data; """
    qt_sql11 """ SELECT * FROM ${indexTbName1} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql12 """ SELECT * FROM ${indexTbName1} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql13 """ SELECT * FROM ${indexTbName1} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql14 """ SELECT * FROM ${indexTbName1} WHERE data = 400 ORDER BY k1,k2,data; """
    qt_sql15 """ SELECT k2 FROM ${indexTbName1} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql16 """ SELECT k1 FROM ${indexTbName1} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql17 """ SELECT k1, k2 FROM ${indexTbName1} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql18 """ SELECT k1, k2 FROM ${indexTbName1} WHERE data = 400 ORDER BY k1,k2,data; """
    


    // mow
    sql "DROP TABLE IF EXISTS ${indexTbName2}"

    sql """
      CREATE TABLE ${indexTbName2} (
      `k1` int(11) NULL COMMENT "",
      `k2` varchar(20) NULL COMMENT "",
      `data` text NULL COMMENT "",
      INDEX idx_k1 (`k1`) USING INVERTED COMMENT '',
      INDEX idx_k2 (`k2`) USING BITMAP  COMMENT '',
      INDEX idx_data (`data`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      UNIQUE KEY(`k1`, `k2`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "enable_unique_key_merge_on_write" = "true",
      "disable_auto_compaction" = "true"
      );
    """

    sql """ INSERT INTO ${indexTbName2} VALUES (1, 20, 300); """
    sql """ INSERT INTO ${indexTbName2} VALUES (1, 20, 400); """

    qt_sql20 """ SELECT * FROM ${indexTbName2} ORDER BY k1,k2,data; """
    qt_sql21 """ SELECT * FROM ${indexTbName2} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql22 """ SELECT * FROM ${indexTbName2} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql23 """ SELECT * FROM ${indexTbName2} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql24 """ SELECT * FROM ${indexTbName2} WHERE data = 400 ORDER BY k1,k2,data; """
    qt_sql25 """ SELECT k2 FROM ${indexTbName2} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql26 """ SELECT k1 FROM ${indexTbName2} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql27 """ SELECT k1, k2 FROM ${indexTbName2} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql28 """ SELECT k1, k2 FROM ${indexTbName2} WHERE data = 400 ORDER BY k1,k2,data; """


    // mor
    sql "DROP TABLE IF EXISTS ${indexTbName3}"

    sql """
      CREATE TABLE ${indexTbName3} (
      `k1` int(11) NULL COMMENT "",
      `k2` varchar(20) NULL COMMENT "",
      `data` text NULL COMMENT "",
      INDEX idx_k1 (`k1`) USING INVERTED COMMENT '',
      INDEX idx_k2 (`k2`) USING BITMAP  COMMENT '',
      INDEX idx_data (`data`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      UNIQUE KEY(`k1`, `k2`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1",
      "enable_unique_key_merge_on_write" = "false",
      "disable_auto_compaction" = "true"
      );
    """

    sql """ INSERT INTO ${indexTbName3} VALUES (1, 20, 300); """
    sql """ INSERT INTO ${indexTbName3} VALUES (1, 20, 400); """

    qt_sql30 """ SELECT * FROM ${indexTbName3} ORDER BY k1,k2,data; """
    qt_sql31 """ SELECT * FROM ${indexTbName3} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql32 """ SELECT * FROM ${indexTbName3} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql33 """ SELECT * FROM ${indexTbName3} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql34 """ SELECT * FROM ${indexTbName3} WHERE data = 400 ORDER BY k1,k2,data; """
    qt_sql35 """ SELECT k2 FROM ${indexTbName3} WHERE k1 = 1 ORDER BY k1,k2,data; """
    qt_sql36 """ SELECT k1 FROM ${indexTbName3} WHERE k2 = 20 ORDER BY k1,k2,data; """
    qt_sql37 """ SELECT k1, k2 FROM ${indexTbName3} WHERE data = 300 ORDER BY k1,k2,data; """
    qt_sql38 """ SELECT k1, k2 FROM ${indexTbName3} WHERE data = 400 ORDER BY k1,k2,data; """
}