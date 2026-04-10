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

import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.UseCboRuleHint;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for EliminateLogicalSelectHint, specifically for NO_USE_CBO_RULE hint handling.
 */
public class EliminateLogicalSelectHintTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("CREATE TABLE `t1` (\n"
                + "  `ka` bigint(20) NULL,\n"
                + "  `kb` bigint(20) NULL,\n"
                + "  `kc` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`ka`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`ka`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");

        createTable("CREATE TABLE `t2` (\n"
                + "  `ka` bigint(20) NULL,\n"
                + "  `kb` bigint(20) NULL,\n"
                + "  `kc` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`ka`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`ka`) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");");
    }

    @Test
    public void testNoUseCboRuleHintIsRegistered() {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("SELECT /*+ no_use_cbo_rule(INFER_SET_OPERATOR_DISTINCT) */"
                        + " ka, kb, kc FROM t1 INTERSECT SELECT ka, kb, kc FROM t2");

        List<Hint> hints = checker.getCascadesContext().getStatementContext().getHints();
        boolean foundNoUseCboRuleHint = false;
        for (Hint hint : hints) {
            if (hint instanceof UseCboRuleHint) {
                UseCboRuleHint useCboRuleHint = (UseCboRuleHint) hint;
                if (useCboRuleHint.getHintName().equalsIgnoreCase("INFER_SET_OPERATOR_DISTINCT")
                        && useCboRuleHint.isNotUseCboRule()) {
                    foundNoUseCboRuleHint = true;
                    break;
                }
            }
        }
        Assertions.assertTrue(foundNoUseCboRuleHint,
                "NO_USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) hint should be registered in StatementContext");
    }

    @Test
    public void testUseCboRuleHintIsRegistered() {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("SELECT /*+ use_cbo_rule(INFER_SET_OPERATOR_DISTINCT) */"
                        + " ka, kb, kc FROM t1 INTERSECT SELECT ka, kb, kc FROM t2");

        List<Hint> hints = checker.getCascadesContext().getStatementContext().getHints();
        boolean foundUseCboRuleHint = false;
        for (Hint hint : hints) {
            if (hint instanceof UseCboRuleHint) {
                UseCboRuleHint useCboRuleHint = (UseCboRuleHint) hint;
                if (useCboRuleHint.getHintName().equalsIgnoreCase("INFER_SET_OPERATOR_DISTINCT")
                        && !useCboRuleHint.isNotUseCboRule()) {
                    foundUseCboRuleHint = true;
                    break;
                }
            }
        }
        Assertions.assertTrue(foundUseCboRuleHint,
                "USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) hint should be registered in StatementContext");
    }
}
