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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.Set;

/** Pure top-down analysis of slots that must remain eager in a physical TopN candidate subtree. */
public final class SlotDemandAnalyzer {

    public Set<Slot> analyzeRequiredEagerSlots(Plan root) {
        Set<Slot> requiredEagerSlots = new LinkedHashSet<>();
        collect(root, requiredEagerSlots);
        return ImmutableSet.copyOf(requiredEagerSlots);
    }

    private void collect(Plan plan, Set<Slot> requiredEagerSlots) {
        addOperatorLocalDemands(plan, requiredEagerSlots);
        for (Plan child : plan.children()) {
            collect(child, requiredEagerSlots);
        }
    }

    private void addOperatorLocalDemands(Plan plan, Set<Slot> requiredEagerSlots) {
        if (plan instanceof PhysicalProject) {
            for (NamedExpression expression : ((PhysicalProject<?>) plan).getProjects()) {
                if (expression instanceof SlotReference
                        || expression instanceof Alias && expression.child(0) instanceof SlotReference) {
                    continue;
                }
                requiredEagerSlots.addAll(expression.getInputSlots());
            }
            return;
        }
        boolean indexEvaluable = plan instanceof PhysicalFilter
                && plan.child(0) instanceof PhysicalOlapScan
                && SessionVariable.getTopNLazyMaterializationUsingIndex();
        // The scan index evaluates these predicates before lazy fetch, so their inputs alone do not
        // force the source columns eager. Demands from TopN, joins, and other operators are collected separately.
        if (!indexEvaluable) {
            requiredEagerSlots.addAll(plan.getInputSlots());
        }
    }
}
