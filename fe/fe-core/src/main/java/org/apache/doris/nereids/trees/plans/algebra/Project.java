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

package org.apache.doris.nereids.trees.plans.algebra;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Common interface for logical/physical project.
 */
public interface Project {
    List<NamedExpression> getProjects();

    /**
     * Generate a map that the key is the alias slot, corresponding value is the expression produces the slot.
     * For example:
     * <pre>
     * projects:
     * [a, alias(b as c), alias((d + e + 1) as f)]
     * result map:
     * c -> b
     * f -> d + e + 1
     * </pre>
     */
    default Map<Slot, Expression> getAliasToProducer() {
        return getProjects()
                .stream()
                .filter(Alias.class::isInstance)
                .collect(
                        Collectors.toMap(
                                NamedExpression::toSlot,
                                // Avoid cast to alias, retrieving the first child expression.
                                alias -> alias.child(0)
                        )
                );
    }

    /**
     * combine upper level and bottom level projections
     * 1. alias combination, for example
     * proj(x as y, b) --> proj(a as x, b, c) =>(a as y, b)
     * 2. remove used projection in bottom project
     * @param childProject bottom project
     * @return project list for merged project
     */
    default List<NamedExpression> mergeProjections(Project childProject) {
        List<NamedExpression> thisProjectExpressions = getProjects();
        List<NamedExpression> childProjectExpressions = childProject.getProjects();
        Map<Expression, Expression> bottomAliasMap = childProjectExpressions.stream()
                .filter(e -> e instanceof Alias)
                .collect(Collectors.toMap(
                        NamedExpression::toSlot, e -> e)
                );
        return thisProjectExpressions.stream()
                .map(e -> ExpressionReplacer.INSTANCE.replace(e, bottomAliasMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
    }

    /**
     * find projects, if not found the slot, then throw AnalysisException
     */
    static List<NamedExpression> findProject(
            Collection<? extends Slot> slotReferences,
            List<NamedExpression> projects) throws AnalysisException {
        Map<ExprId, NamedExpression> exprIdToProject = projects.stream()
                .collect(ImmutableMap.toImmutableMap(p -> p.getExprId(), p -> p));

        return slotReferences.stream()
                .map(slot -> {
                    ExprId exprId = slot.getExprId();
                    NamedExpression project = exprIdToProject.get(exprId);
                    if (project == null) {
                        throw new AnalysisException("ExprId " + slot.getExprId() + " no exists in " + projects);
                    }
                    return project;
                })
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * findUsedProject. if not found the slot, then skip it
     */
    static <OUTPUT_TYPE extends NamedExpression> List<OUTPUT_TYPE> filterUsedOutputs(
            Collection<? extends Slot> slotReferences, List<OUTPUT_TYPE> childOutput) {
        Map<ExprId, OUTPUT_TYPE> exprIdToChildOutput = childOutput.stream()
                .collect(ImmutableMap.toImmutableMap(p -> p.getExprId(), p -> p));

        return slotReferences.stream()
                .map(slot -> exprIdToChildOutput.get(slot.getExprId()))
                .filter(project -> project != null)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * replace alias
     */
    public static class ExpressionReplacer extends DefaultExpressionRewriter<Map<Expression, Expression>> {
        public static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        public Expression replace(Expression expr, Map<Expression, Expression> substitutionMap) {
            if (expr instanceof SlotReference) {
                Slot ref = ((SlotReference) expr).withQualifier(Collections.emptyList());
                return substitutionMap.getOrDefault(ref, expr);
            }
            return visit(expr, substitutionMap);
        }

        /**
         * case 1:
         *          project(alias(c) as d, alias(x) as y)
         *                      |
         *                      |                          ===>       project(alias(a) as d, alias(b) as y)
         *                      |
         *          project(slotRef(a) as c, slotRef(b) as x)
         * case 2:
         *         project(slotRef(x.c), slotRef(x.d))
         *                      |                          ===>       project(slotRef(a) as x.c, slotRef(b) as x.d)
         *         project(slotRef(a) as c, slotRef(b) as d)
         * case 3: others
         */
        @Override
        public Expression visit(Expression expr, Map<Expression, Expression> substitutionMap) {
            if (expr instanceof Alias && expr.child(0) instanceof SlotReference) {
                // case 1:
                Expression c = expr.child(0);
                // Alias doesn't contain qualifier
                Slot ref = ((SlotReference) c).withQualifier(Collections.emptyList());
                if (substitutionMap.containsKey(ref)) {
                    return expr.withChildren(substitutionMap.get(ref).children());
                }
            } else if (expr instanceof SlotReference) {
                // case 2:
                Slot ref = ((SlotReference) expr).withQualifier(Collections.emptyList());
                if (substitutionMap.containsKey(ref)) {
                    Alias res = (Alias) substitutionMap.get(ref);
                    return res.child();
                }
            } else if (substitutionMap.containsKey(expr)) {
                return substitutionMap.get(expr).child(0);
            }
            return super.visit(expr, substitutionMap);
        }
    }
}


