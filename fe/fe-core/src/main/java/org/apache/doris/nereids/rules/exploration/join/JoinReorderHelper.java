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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper class for three left deep tree ( original plan tree is (a b) c )create new join.
 */
public class JoinReorderHelper {
    public List<Expression> newTopHashConjuncts;
    public List<Expression> newTopOtherConjuncts;
    public List<Expression> newBottomHashConjuncts;
    public List<Expression> newBottomOtherConjuncts;

    public List<NamedExpression> oldProjects;
    public List<NamedExpression> newLeftProjects;
    public List<NamedExpression> newRightProjects;

    /**
     * Constructor.
     */
    public JoinReorderHelper(List<Expression> newTopHashConjuncts, List<Expression> newTopOtherConjuncts,
            List<Expression> newBottomHashConjuncts, List<Expression> newBottomOtherConjuncts,
            List<NamedExpression> oldProjects, List<NamedExpression> newLeftProjects,
            List<NamedExpression> newRightProjects) {
        this.newTopHashConjuncts = newTopHashConjuncts;
        this.newTopOtherConjuncts = newTopOtherConjuncts;
        this.newBottomHashConjuncts = newBottomHashConjuncts;
        this.newBottomOtherConjuncts = newBottomOtherConjuncts;
        this.oldProjects = oldProjects;
        this.newLeftProjects = newLeftProjects;
        this.newRightProjects = newRightProjects;
        replaceConjuncts(oldProjects);
    }

    private void replaceConjuncts(List<NamedExpression> projects) {
        Map<ExprId, Slot> inputToOutput = new HashMap<>();
        Map<ExprId, Slot> outputToInput = new HashMap<>();
        for (NamedExpression expr : projects) {
            Slot outputSlot = expr.toSlot();
            Set<Slot> usedSlots = expr.getInputSlots();
            Preconditions.checkState(usedSlots.size() == 1);
            Slot inputSlot = usedSlots.iterator().next();
            inputToOutput.put(inputSlot.getExprId(), outputSlot);
            outputToInput.put(outputSlot.getExprId(), inputSlot);
        }

        newBottomHashConjuncts = JoinReorderUtils.replaceJoinConjuncts(newBottomHashConjuncts, outputToInput);
        newTopHashConjuncts = JoinReorderUtils.replaceJoinConjuncts(newTopHashConjuncts, inputToOutput);
        newBottomOtherConjuncts = JoinReorderUtils.replaceJoinConjuncts(newBottomOtherConjuncts, outputToInput);
        newTopOtherConjuncts = JoinReorderUtils.replaceJoinConjuncts(newTopOtherConjuncts, inputToOutput);
    }

    /**
     * Add all slots used by OnCondition when projects not empty.
     * @param cOutputExprIdSet we want to get abOnUsedSlots, we need filter cOutputExprIdSet.
     */
    public void addSlotsUsedByOn(Set<ExprId> splitIds, Set<ExprId> cOutputExprIdSet) {
        Map<Boolean, Set<Slot>> abOnUsedSlots = Stream.concat(
                        newTopHashConjuncts.stream(),
                        newTopOtherConjuncts.stream())
                .flatMap(onExpr -> onExpr.getInputSlots().stream())
                .filter(slot -> !cOutputExprIdSet.contains(slot.getExprId()))
                .collect(Collectors.partitioningBy(slot -> splitIds.contains(slot.getExprId()), Collectors.toSet()));
        Set<Slot> aUsedSlots = abOnUsedSlots.get(true);
        Set<Slot> bUsedSlots = abOnUsedSlots.get(false);

        JoinReorderUtils.addSlotsUsedByOn(aUsedSlots, newLeftProjects);
        JoinReorderUtils.addSlotsUsedByOn(bUsedSlots, newRightProjects);
    }
}
