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

import java.sql.SQLException

suite("test_inverted_index_writer_exception", "nonConcurrent"){
  def indexTbName1 = "test_inverted_index_writer_exception"

  sql "DROP TABLE IF EXISTS ${indexTbName1}"

  sql """
    CREATE TABLE ${indexTbName1} (
    `@timestamp` int(11) NULL COMMENT "",
    `clientip` varchar(20) NULL COMMENT "",
    `request` text NULL COMMENT "",
    `status` int(11) NULL COMMENT "",
    `size` int(11) NULL COMMENT "",
    INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
    ) ENGINE=OLAP
    DUPLICATE KEY(`@timestamp`)
    COMMENT "OLAP"
    DISTRIBUTED BY RANDOM BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
  """

  try {
    GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close")
    try {
      sql """ INSERT INTO ${indexTbName1} VALUES (1, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    } catch (SQLException e) {
      if (e.message.contains("E-6002")) {
        log.info("Test passed 1: Encountered expected exception [E-6002].")
      } else {
        throw e
      }
    }
  } finally {
    GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close")
  }

  try {
    GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::close_close_with_error")
    try {
      sql """ INSERT INTO ${indexTbName1} VALUES (2, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    } catch (SQLException e) {
      if (e.message.contains("E-6002")) {
        log.info("Test passed 2: Encountered expected exception [E-6002].")
      } else {
        throw e
      }
    }
  } finally {
    GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::close_close_with_error")
  }

  try {
    GetDebugPoint().enableDebugPointForAllBEs("InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close")
    GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::close_close_with_error")

    try {
      sql """ INSERT INTO ${indexTbName1} VALUES (3, "40.135.0.0", "GET /images/hm_bg.jpg HTTP/1.0", 1, 2); """
    } catch (SQLException e) {
      if (e.message.contains("E-6002")) {
        log.info("Test passed 3: Encountered expected exception [E-6002].")
      } else {
        throw e
      }
    }
  } finally {
    GetDebugPoint().disableDebugPointForAllBEs("InvertedIndexWriter._throw_clucene_error_in_fulltext_writer_close")
    GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::close_close_with_error")
  }
}