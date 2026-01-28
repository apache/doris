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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UniqueFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.logical.ProjectMergeable;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Check whether the exprId of NamedExpression and UniqueFunction is unique in the plan tree.
 * If not, throw AnalysisException.
 * This rule is only enabled when the session variable 'fe_debug' is set to true.
 * This rule is used to find some potential problems in the plan, such as:
 * 1. Two different NamedExpressions have the same exprId, which may be caused by a bug in the code.
 * 2. Two different UniqueFunctions have the same uniqueId, which may be caused by a bug in the code
 *    or the lack of a Project node between two UniqueFunctions.
 */
public class CheckUniqueExprId implements CustomRewriter {

    private final Map<ExprId, NamedExpression> dedupNameExpressions = Maps.newHashMap();
    private final Map<ExprId, UniqueFunction> uniqueFunctions = Maps.newHashMap();

    @Override
    public Plan rewriteRoot(Plan rootPlan, JobContext jobContext) {
        if (SessionVariable.isFeDebug()) {
            checkPlan(rootPlan);
        }
        return rootPlan;
    }

    private void checkPlan(Plan plan) {
        for (Plan child : plan.children()) {
            checkPlan(child);
        }

        checkSlotExprId(plan);
        checkUniqueFunctionExprId(plan);
    }

    private void checkUniqueFunctionExprId(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            expression.foreach(expr -> {
                if (expr instanceof UniqueFunction) {
                    UniqueFunction uniqueFunction = (UniqueFunction) expr;
                    UniqueFunction existUniqueFunction
                            = uniqueFunctions.putIfAbsent(uniqueFunction.getUniqueId(), uniqueFunction);
                    if (existUniqueFunction != null && !(plan instanceof LogicalEmptyRelation)) {
                        throw new AnalysisException("Found duplicated unique id in two unique functions, "
                                + "need add project from them, first func: "
                                + existUniqueFunction.toSql() + ", second func: " + uniqueFunction.toSql());
                    }
                }
            });
        }
    }

    private void checkSlotExprId(Plan plan) {
        if (plan instanceof LogicalCatalogRelation) {
            for (Slot slot : ((LogicalCatalogRelation) plan).getOutput()) {
                addNameExpression(slot);
            }
        } else {
            Optional<List<NamedExpression>> outputs = getOutputs(plan);
            if (outputs.isPresent()) {
                for (NamedExpression namedExpression : outputs.get()) {
                    if (namedExpression instanceof Alias) {
                        addNameExpression(namedExpression);
                    }
                }
            }
        }
    }

    private void addNameExpression(NamedExpression namedExpression) {
        NamedExpression existNameExpressions
                = dedupNameExpressions.putIfAbsent(namedExpression.getExprId(), namedExpression);
        if (existNameExpressions != null) {
            throw new AnalysisException("Found duplicated expr id in two name expression, first name expression: "
                    + existNameExpressions.toSql() + ", second name expression: " + namedExpression.toSql());
        }
    }

    private Optional<List<NamedExpression>> getOutputs(Plan plan) {
        if (plan instanceof ProjectMergeable) {
            return Optional.of(((ProjectMergeable) plan).getProjects());
        } else if (plan instanceof Aggregate) {
            return Optional.of(((Aggregate<?>) plan).getOutputExpressions());
        } else if (plan instanceof LogicalWindow) {
            return Optional.of(((LogicalWindow<?>) plan).getWindowExpressions());
        } else if (plan instanceof LogicalSetOperation) {
            return Optional.of(((LogicalSetOperation) plan).getOutputs());
        } else {
            return Optional.empty();
        }
    }
}
