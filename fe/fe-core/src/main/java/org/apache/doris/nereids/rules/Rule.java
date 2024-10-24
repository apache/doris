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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.TransformException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.RuleType.RuleTypeClass;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.BitSet;
import java.util.List;

/**
 * Abstract class for all rules.
 */
public abstract class Rule {
    private final RuleType ruleType;
    private final Pattern<? extends Plan> pattern;
    private final RulePromise rulePromise;

    /**
     * Constructor for Rule.
     *
     * @param ruleType type of rule
     * @param pattern target pattern of rule
     * @param rulePromise rule promise
     */
    public Rule(RuleType ruleType, Pattern<? extends Plan> pattern, RulePromise rulePromise) {
        this.ruleType = ruleType;
        this.pattern = pattern;
        this.rulePromise = rulePromise;
    }

    public RuleType getRuleType() {
        return ruleType;
    }

    public RulePromise getRulePromise() {
        return rulePromise;
    }

    public Pattern<? extends Plan> getPattern() {
        return pattern;
    }

    public boolean isRewrite() {
        return ruleType.getRuleTypeClass() == RuleTypeClass.REWRITE;
    }

    public boolean isExploration() {
        return ruleType.getRuleTypeClass() == RuleTypeClass.EXPLORATION;
    }

    @Override
    public String toString() {
        return getRuleType().toString();
    }

    public abstract List<Plan> transform(Plan node, CascadesContext context) throws TransformException;

    /** callback this function when the traverse framework accept a new plan which produce by this rule */
    public void acceptPlan(Plan plan) {

    }

    /**
     * Filter out already applied rules and rules that are not matched on root node.
     */
    public boolean isInvalid(BitSet disableRules, GroupExpression groupExpression) {
        return disableRules.get(this.getRuleType().type())
                || !groupExpression.notApplied(this)
                || !this.getPattern().matchRoot(groupExpression.getPlan());
    }
}
