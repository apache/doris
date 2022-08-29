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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Supplier;
import org.junit.jupiter.api.Assertions;

import java.util.function.Consumer;

/**
 * Utility to apply rules to plan and check output plan matches the expected pattern.
 */
public class PlanChecker {
    private ConnectContext connectContext;
    private CascadesContext cascadesContext;

    private Plan parsedPlan;

    public PlanChecker(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public PlanChecker(CascadesContext cascadesContext) {
        this.connectContext = cascadesContext.getConnectContext();
        this.cascadesContext = cascadesContext;
    }

    public PlanChecker checkParse(String sql, Consumer<PlanParseChecker> consumer) {
        PlanParseChecker checker = new PlanParseChecker(sql);
        consumer.accept(checker);
        parsedPlan = checker.parsedSupplier.get();
        return this;
    }

    public PlanChecker analyze() {
        MemoTestUtils.createCascadesContext(connectContext, parsedPlan);
        return this;
    }

    public PlanChecker analyze(String sql) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, sql);
        this.cascadesContext.newAnalyzer().analyze();
        return this;
    }

    public PlanChecker analyze(Plan plan) {
        this.cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        this.cascadesContext.newAnalyzer().analyze();
        return this;
    }

    public PlanChecker applyTopDown(RuleFactory rule) {
        cascadesContext.topDownRewrite(rule);
        return this;
    }

    public PlanChecker applyBottomUp(RuleFactory rule) {
        cascadesContext.bottomUpRewrite(rule);
        return this;
    }

    public void matchesFromRoot(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpression()).iterator().hasNext());
    }

    public void matches(PatternDescriptor<? extends Plan> patternDesc) {
        Memo memo = cascadesContext.getMemo();
        assertMatches(memo, () -> GroupMatchingUtils.topDownFindMatching(memo.getRoot(), patternDesc.pattern));
    }

    private void assertMatches(Memo memo, Supplier<Boolean> asserter) {
        Assertions.assertTrue(asserter.get(),
                () -> "pattern not match, plan :\n"
                        + memo.getRoot().getLogicalExpression().getPlan().treeString()
                        + "\n"
        );
    }

    public static PlanChecker from(ConnectContext connectContext) {
        return new PlanChecker(connectContext);
    }

    public static PlanChecker from(ConnectContext connectContext, Plan initPlan) {
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, initPlan);
        return new PlanChecker(cascadesContext);
    }
}
