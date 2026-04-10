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
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for EliminateLogicalSelectHint, verifying that no_use_cbo_rule hints are correctly
 * propagated into the statement context after analysis.
 */
public class EliminateLogicalSelectHintTest extends SqlTestBase {

    @Test
    public void testNoUseCboRuleHintIsRecognized() {
        String sql = "SELECT /*+ no_use_cbo_rule(INFER_SET_OPERATOR_DISTINCT) */ * FROM T1";
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);

        List<Hint> hints = checker.getCascadesContext().getStatementContext().getHints();
        List<UseCboRuleHint> cboHints = hints.stream()
                .filter(h -> h instanceof UseCboRuleHint)
                .map(h -> (UseCboRuleHint) h)
                .collect(Collectors.toList());

        Assertions.assertFalse(cboHints.isEmpty(),
                "no_use_cbo_rule hint should be added to statementContext hints");
        Assertions.assertTrue(cboHints.stream().anyMatch(UseCboRuleHint::isNotUseCboRule),
                "hint should have isNotUseCboRule=true");
        Assertions.assertTrue(
                cboHints.stream().anyMatch(h -> h.getHintName().equalsIgnoreCase("INFER_SET_OPERATOR_DISTINCT")),
                "hint should carry the rule name INFER_SET_OPERATOR_DISTINCT");
    }
}
