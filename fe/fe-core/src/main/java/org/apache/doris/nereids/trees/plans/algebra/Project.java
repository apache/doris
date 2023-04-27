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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
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
        Map<Expression, Alias> replaceMap = childProject.getProjects().stream()
                .filter(e -> e instanceof Alias)
                .collect(Collectors.toMap(
                        NamedExpression::toSlot,
                        e -> (Alias) e));
        return getProjects().stream()
                .map(expr -> {
                    if (expr instanceof Alias) {
                        Alias alias = (Alias) expr;
                        Expression insideExpr = alias.child();
                        Expression newInsideExpr = insideExpr.rewriteUp(e -> {
                            Alias getAlias = replaceMap.get(e);
                            return getAlias == null ? e : getAlias.child();
                        });
                        return newInsideExpr == insideExpr ? expr : alias.withChildren(ImmutableList.of(newInsideExpr));
                    } else {
                        Alias getAlias = replaceMap.get(expr);
                        return getAlias == null ? expr : getAlias;
                    }
                })
                .collect(ImmutableList.toImmutableList());
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
}


