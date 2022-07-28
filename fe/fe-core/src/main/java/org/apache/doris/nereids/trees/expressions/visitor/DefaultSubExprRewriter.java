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

import org.apache.doris.nereids.analyzer.NereidsAnalyzer;
import org.apache.doris.nereids.rules.analysis.Scope;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

/**
 * Use the visitor to iterate sub expression.
 */
public class DefaultSubExprRewriter<C> extends DefaultExpressionRewriter<C> {
    private final Scope scope;

    public DefaultSubExprRewriter(Scope scope) {
        this.scope = scope;
    }

    @Override
    public Expression visit(Expression expr, C context) {
        if (expr instanceof SubqueryExpr) {
            NereidsAnalyzer subAnalyzer = new NereidsAnalyzer(ConnectContext.get());
            LogicalPlan analyzed = subAnalyzer.analyze(((SubqueryExpr) expr).getQueryPlan(), scope);
            if (expr instanceof InSubquery) {
                return new InSubquery(((InSubquery) expr).getCompareExpr(), analyzed);
            } else if (expr instanceof Exists) {
                return new Exists(analyzed);
            }
            return new SubqueryExpr(analyzed);
        }
        return super.visit(expr, context);
    }

    public Scope getScope() {
        return scope;
    }
}
