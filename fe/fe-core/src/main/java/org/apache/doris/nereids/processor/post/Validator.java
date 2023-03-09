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
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * validator plan.
 */
public class Validator extends PlanPostProcessor {

    private static final Set<Class<? extends DataType>> UNSUPPORTED_TYPE = ImmutableSet.of(
            MapType.class, StructType.class, JsonType.class, ArrayType.class);

    @Override
    public Plan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        Plan child = project.child();
        // Forbidden project-project, we must merge project.
        Preconditions.checkArgument(!(child instanceof PhysicalProject));

        // TODO: Check projects is from child output.
        // List<NamedExpression> projects = project.getProjects();
        // Set<Slot> childOutputSet = child.getOutputSet();

        child.accept(this, context);
        return visit(project, context);
    }

    @Override
    public Plan visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext context) {
        Preconditions.checkArgument(!filter.getConjuncts().isEmpty()
                && filter.getPredicate() != BooleanLiteral.TRUE);

        Plan child = filter.child();
        // Forbidden filter-project, we must make filter-project -> project-filter.
        if (child instanceof PhysicalProject) {
            throw new AnalysisException(
                    "Nereids generate a filter-project plan, but backend not support:\n" + filter.treeString());
        }

        // Check filter is from child output.
        Set<Slot> childOutputSet = child.getOutputSet();
        Set<Slot> slotsUsedByFilter = filter.getConjuncts().stream()
                .<Set<Slot>>map(expr -> expr.collect(Slot.class::isInstance))
                .flatMap(Collection::stream).collect(Collectors.toSet());
        for (Slot slot : slotsUsedByFilter) {
            Preconditions.checkState(childOutputSet.contains(slot)
                    || slot instanceof MarkJoinSlotReference);
        }

        child.accept(this, context);
        return visit(filter, context);
    }

    @Override
    public Plan visit(Plan plan, CascadesContext context) {
        plan.getExpressions().forEach(ExpressionChecker.INSTANCE::check);
        plan.children().forEach(child -> child.accept(this, context));
        return plan;
    }

    private static class ExpressionChecker extends DefaultExpressionVisitor<Expression, Void> {
        public static final ExpressionChecker INSTANCE = new ExpressionChecker();

        public void check(Expression expression) {
            expression.accept(this, null);
        }

        public Expression visit(Expression expr, Void unused) {
            try {
                checkTypes(expr.getDataType());
            } catch (UnboundException ignored) {
                return expr;
            }
            expr.children().forEach(child -> child.accept(this, null));
            return expr;
        }

        private void checkTypes(DataType dataType) {
            if (UNSUPPORTED_TYPE.contains(dataType.getClass())) {
                throw new AnalysisException(String.format("type %s is unsupported for Nereids", dataType));
            }
        }
    }
}
