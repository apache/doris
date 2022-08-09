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

import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.pattern.PatternDescriptor;
import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;

/**
 * Utility to apply rules to plan and check output plan matches the expected pattern.
 */
public class PlanChecker {

    private Plan inputPlan;
    private Memo memo;

    public PlanChecker plan(Plan plan) {
        this.inputPlan = plan;
        return this;
    }

    public PlanChecker applyTopDown(RuleFactory rule) {
        return applyTopDown(rule, new ConnectContext());
    }

    public PlanChecker applyTopDown(RuleFactory rule, ConnectContext connectContext) {
        memo = PlanRewriter.topDownRewriteMemo(inputPlan, connectContext, rule);
        return this;
    }

    public PlanChecker applyBottomUp(RuleFactory rule) {
        return applyBottomUp(rule);
    }

    public PlanChecker applyBottomUp(RuleFactory rule, ConnectContext connectContext) {
        memo = PlanRewriter.bottomUpRewriteMemo(inputPlan, connectContext, rule);
        return this;
    }

    public void matches(PatternDescriptor<? extends Plan> patternDesc) {
        GroupExpressionMatching matchResult = new GroupExpressionMatching(patternDesc.pattern,
                memo.getRoot().getLogicalExpression());
        Assertions.assertTrue(matchResult.iterator().hasNext(), () ->
                "pattern not match, plan :\n" + memo.getRoot().getLogicalExpression().getPlan().treeString() + "\n"
        );
    }
}
