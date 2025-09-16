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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotNotFromChildren;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.util.LazyCompute;

import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * validator plan.
 */
public class Validator extends PlanPostProcessor {

    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        Preconditions.checkArgument(!project.getProjects().isEmpty(), "Project list can't be empty");
        return visit(project, context);
    }

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        Preconditions.checkArgument(!filter.getConjuncts().isEmpty()
                && filter.getPredicate() != BooleanLiteral.TRUE, "Filter predicate can't be empty or true");

        return visit(filter, context);
    }

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        for (Plan child : plan.children()) {
            child.accept(this, context);
        }

        Optional<Slot> opt = checkAllSlotFromChildren(plan);
        if (opt.isPresent()) {
            List<Slot> childrenOutput = plan.children().stream().flatMap(p -> p.getOutput().stream()).collect(
                    Collectors.toList());
            throw new AnalysisException("A expression contains slot not from children\n"
                    + "Slot: " + opt.get() + "  Children Output:" + childrenOutput + "\n"
                    + "Plan: " + plan.treeString() + "\n");
        }
        return plan;
    }

    /**
     * Check all slot must from children.
     */
    public static Optional<Slot> checkAllSlotFromChildren(Plan plan) {
        if (plan.arity() == 0) {
            return Optional.empty();
        }
        // agg exist multi-phase
        if (plan instanceof Aggregate) {
            return Optional.empty();
        }

        Supplier<BitSet> childrenOutputIds = LazyCompute.of(() -> {
            BitSet ids = new BitSet();
            for (Plan child : plan.children()) {
                for (Slot slot : child.getOutput()) {
                    ids.set(slot.getExprId().asInt());
                }
            }
            return ids;
        });

        for (Expression expression : plan.getExpressions()) {
            AtomicReference<Slot> invalidSlot = new AtomicReference<>();
            expression.anyMatch(e -> {
                if (e instanceof Slot) {
                    Slot slot = (Slot) e;
                    if (slot.getName().startsWith("mv") || slot instanceof SlotNotFromChildren) {
                        return false;
                    }
                    if (!childrenOutputIds.get().get(slot.getExprId().asInt())) {
                        invalidSlot.set(slot);
                        return true;
                    }
                }
                return false;
            });
            return Optional.ofNullable(invalidSlot.get());
        }
        return Optional.empty();
    }
}
