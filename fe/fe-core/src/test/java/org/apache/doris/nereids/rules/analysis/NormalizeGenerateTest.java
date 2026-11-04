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

import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NormalizeGenerateTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("normalize_generate_test");
        connectContext.setDatabase("normalize_generate_test");
        createTable("CREATE TABLE base_table (id INT NOT NULL, n INT NOT NULL) "
                + "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void testSubqueryKeepsGenerateChildOutput() {
        assertAnalyzes("SELECT b.id, e FROM base_table b "
                + "LATERAL VIEW explode_numbers((SELECT MAX(t2.n) FROM base_table t2)) lv AS e");
    }

    @Test
    public void testSubqueryKeepsGeneratorInput() {
        assertAnalyzes("SELECT b.id, e FROM base_table b "
                + "LATERAL VIEW explode_numbers(b.n + (SELECT MAX(t2.n) FROM base_table t2)) lv AS e");
    }

    private void assertAnalyzes(String sql) {
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan(), sql);
    }
}
