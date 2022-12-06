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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * some check need to do after analyze whole plan.
 */
public class CheckAfterRewrite extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return any().then(plan -> {
            checkAllSlotReferenceFromChildren(plan);
            return null;
        }).toRule(RuleType.CHECK_ANALYSIS);
    }

    private void checkAllSlotReferenceFromChildren(Plan plan) {
        Set<Slot> notFromChildren = plan.getExpressions().stream()
                .flatMap(expr -> expr.getInputSlots().stream())
                .collect(Collectors.toSet());
        Set<Slot> childrenOutput = plan.children().stream()
                .map(Plan::getOutput)
                .flatMap(List::stream)
                .collect(Collectors.toSet());
        notFromChildren.removeAll(childrenOutput);
        notFromChildren = removeValidVirtualSlots(notFromChildren, childrenOutput);
        if (!notFromChildren.isEmpty()) {
            throw new AnalysisException(String.format("Input slot(s) not in child's output: %s",
                    StringUtils.join(notFromChildren.stream()
                            .map(ExpressionTrait::toSql)
                            .collect(Collectors.toSet()), ", ")));
        }
    }

    private Set<Slot> removeValidVirtualSlots(Set<Slot> virtualSlots, Set<Slot> childrenOutput) {
        return virtualSlots.stream()
                .filter(expr -> {
                    if (expr instanceof VirtualSlotReference) {
                        List<Expression> realExpressions = ((VirtualSlotReference) expr).getRealExpressions();
                        if (realExpressions.isEmpty()) {
                            // valid
                            return false;
                        }
                        return realExpressions.stream()
                                .anyMatch(realUsedExpr -> !childrenOutput.contains(realUsedExpr));
                    } else {
                        return true;
                    }
                })
                .collect(Collectors.toSet());
    }
}
