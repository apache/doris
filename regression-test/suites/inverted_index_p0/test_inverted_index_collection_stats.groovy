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

import java.util.regex.Pattern

suite('test_inverted_index_collection_stats', 'p0') {
    def indexTbName1 = "test_inverted_index_collection_stats_tbl"
    
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    
    sql """
      CREATE TABLE ${indexTbName1} (
      `id` int(11) NULL COMMENT "",
      `content` text NULL COMMENT "",
      INDEX content_idx (`content`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`id`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """
    
    sql """ INSERT INTO ${indexTbName1} VALUES (1, 'hello world'), (2, 'hello doris'), (3, 'doris is great') """
    
    sql "sync"
    
    // Enable profile
    sql """ set enable_profile = true; """
    sql """ set profile_level = 2; """
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
    
    // Execute MATCH_ALL query which triggers CollectionStatistics::collect
    def queryId = "test_inverted_index_collection_stats_${System.currentTimeMillis()}"
    try {
        profile("${queryId}") {
            run {
                sql "/* ${queryId} */ select score() as score from ${indexTbName1} where content match_all 'hello' order by score desc limit 10"
            }
            
            check { profileString, exception ->
                def statisticsCollectTime = 0
                def matcher = Pattern.compile("StatisticsCollectTime:\\s*(\\d+)").matcher(profileString)
                if (matcher.find()) {
                    statisticsCollectTime = Integer.parseInt(matcher.group(1))
                    log.info("StatisticsCollectTime: {}", statisticsCollectTime)
                }
                assertTrue(statisticsCollectTime > 0, "StatisticsCollectTime should be > 0, got: ${statisticsCollectTime}")
            }
        }
    } catch (Exception e) {
        if (e.message?.contains("HttpCliAction failed")) {
            log.warn("Profile HTTP request failed, skipping profile check: {}", e.message)
        } else {
            log.warn("Profile check failed: {}", e.message)
            throw e
        }
    } finally {
        // sql "DROP TABLE IF EXISTS ${indexTbName1}"
    }
}
