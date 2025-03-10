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


suite("test_basic_analyzer", "p0"){
    def indexTbName1 = "test_basic_analyzer"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `a` int(11) NULL COMMENT "",
      `b` text NULL COMMENT "",
      INDEX b_idx (`b`) USING INVERTED PROPERTIES("parser" = "basic") COMMENT '',
      ) ENGINE=OLAP
      DUPLICATE KEY(`a`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    sql """ INSERT INTO ${indexTbName1} VALUES (1, "GET /images/hm_bg.jpg HTTP/1.0"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (2, "มนไมเปนไปตามความตองการมนมหมายเลขอยในเนอหา"); """
    sql """ INSERT INTO ${indexTbName1} VALUES (3, "在启动新的 BE 节点前，需要先在 FE 集群中注册新的 BE 节点"); """

    try {
        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select * from ${indexTbName1} where b match_phrase 'images hm_bg.jpg'; """
        qt_sql """ select * from ${indexTbName1} where b match_phrase 'อย ใน'; """
        qt_sql """ select * from ${indexTbName1} where b match_phrase '新的 be'; """
    } finally {
    }
}