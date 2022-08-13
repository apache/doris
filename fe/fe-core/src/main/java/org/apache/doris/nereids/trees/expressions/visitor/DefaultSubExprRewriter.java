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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.analysis.Scope;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.Optional;

/**
 * Use the visitor to iterate sub expression.
 */
public class DefaultSubExprRewriter<C> extends DefaultExpressionRewriter<C> {
    private final Scope scope;
    private final CascadesContext cascadesContext;

    public DefaultSubExprRewriter(Scope scope, CascadesContext cascadesContext) {
        this.scope = scope;
        this.cascadesContext = cascadesContext;
    }

    @Override
    public Expression visitSubqueryExpr(SubqueryExpr expr, C context) {
        return new SubqueryExpr(analyzeSubquery(expr));
    }

    @Override
    public Expression visitInSubquery(InSubquery expr, C context) {
        return new InSubquery(expr.getCompareExpr(), new ListQuery(analyzeSubquery(expr)));
    }

    @Override
    public Expression visitScalarSubquery(ScalarSubquery scalar, C context) {
        LogicalPlan analyzed = analyzeSubquery(scalar);
        if (analyzed.getOutput().size() != 1) {
            throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                    + analyzed.getOutput().size());
        }
        return new ScalarSubquery(analyzed);
    }

    private LogicalPlan analyzeSubquery(SubqueryExpr expr) {
        CascadesContext subqueryContext = new Memo(expr.getQueryPlan())
                .newCascadesContext(cascadesContext.getStatementContext());
        subqueryContext.newAnalyzer(Optional.ofNullable(getScope())).analyze();
        return (LogicalPlan) subqueryContext.getMemo().copyOut();
    }

    public Scope getScope() {
        return scope;
    }
}
