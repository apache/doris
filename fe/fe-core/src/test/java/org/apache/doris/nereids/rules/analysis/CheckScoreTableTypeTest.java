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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckScoreTableTypeTest extends TestWithFeService {
    private static final String ERROR_MESSAGE =
            "score() function is not supported on AGG_KEYS table or merge-on-read UNIQUE_KEYS table";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_score_table_type");
        connectContext.setDatabase("test_score_table_type");

        createTable("CREATE TABLE agg_table ("
                + "k1 INT, content VARCHAR(255), v1 INT SUM, "
                + "INDEX idx_content(content) USING INVERTED) "
                + "AGGREGATE KEY(k1, content) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE mor_table ("
                + "k1 INT, content VARCHAR(255), INDEX idx_content(content) USING INVERTED) "
                + "UNIQUE KEY(k1, content) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', "
                + "'enable_unique_key_merge_on_write' = 'false')");
        createTable("CREATE TABLE mow_table ("
                + "k1 INT, content VARCHAR(255), INDEX idx_content(content) USING INVERTED) "
                + "UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', "
                + "'enable_unique_key_merge_on_write' = 'true')");
        createTable("CREATE TABLE dup_table ("
                + "k1 INT, content VARCHAR(255), INDEX idx_content(content) USING INVERTED) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void testRejectScoreOnAggTable() {
        assertScoreRejected("agg_table");
    }

    @Test
    public void testRejectScoreOnMorTable() {
        assertScoreRejected("mor_table");
    }

    @Test
    public void testAllowScoreOnSupportedTableTypes() {
        Assertions.assertDoesNotThrow(() -> analyzeScoreQuery("mow_table"));
        Assertions.assertDoesNotThrow(() -> analyzeScoreQuery("dup_table"));
    }

    private void assertScoreRejected(String tableName) {
        AnalysisException exception = Assertions.assertThrows(
                AnalysisException.class, () -> analyzeScoreQuery(tableName));
        Assertions.assertTrue(exception.getMessage().contains(ERROR_MESSAGE), exception.getMessage());
    }

    private void analyzeScoreQuery(String tableName) {
        PlanChecker.from(connectContext).analyze("SELECT score() AS s FROM " + tableName
                + " WHERE content MATCH 'doris' ORDER BY s LIMIT 10");
    }
}
