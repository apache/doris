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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Infer output column name when it refers an expression and not has an alias manually.
 */
public class InferPlanOutputAlias extends DefaultPlanVisitor<Void, ImmutableMultimap<ExprId, Integer>> {

    private final List<Slot> currentOutputs;
    private final List<NamedExpression> finalOutputs;

    public InferPlanOutputAlias(List<Slot> currentOutputs) {
        this.currentOutputs = currentOutputs;
        this.finalOutputs = new ArrayList<>(currentOutputs);
    }

    @Override
    public Void visit(Plan plan, ImmutableMultimap<ExprId, Integer> currentExprIdAndIndexMap) {

        List<Alias> aliasProjects = plan.getExpressions().stream()
                .filter(expression -> expression instanceof Alias)
                .map(Alias.class::cast)
                .collect(Collectors.toList());

        ImmutableSet<ExprId> currentOutputExprIdSet = currentExprIdAndIndexMap.keySet();
        for (Alias projectItem : aliasProjects) {
            ExprId exprId = projectItem.getExprId();
            // Infer name when alias child is expression and alias's name is from child
            if (currentOutputExprIdSet.contains(projectItem.getExprId())
                    && projectItem.isNameFromChild()) {
                String inferredAliasName = projectItem.child().getExpressionName();
                ImmutableCollection<Integer> outPutExprIndexes = currentExprIdAndIndexMap.get(exprId);
                // replace output name by inferred name
                outPutExprIndexes.forEach(index -> {
                    Slot slot = currentOutputs.get(index);
                    finalOutputs.set(index, slot.withName("__" + inferredAliasName + "_" + index));
                });
            }
        }
        return super.visit(plan, currentExprIdAndIndexMap);
    }

    public List<NamedExpression> getOutputs() {
        return finalOutputs;
    }
}
