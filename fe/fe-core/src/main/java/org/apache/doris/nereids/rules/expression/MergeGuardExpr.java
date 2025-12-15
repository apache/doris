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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NeedSessionVarGuard;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;

/**MergeGuardExpr*/
public class MergeGuardExpr extends AbstractExpressionRewriteRule {
    public static final MergeGuardExpr INSTANCE = new MergeGuardExpr();

    @Override
    public Expression visitSessionVarGuardExpr(SessionVarGuardExpr sessionVarGuardExpr,
            ExpressionRewriteContext context) {
        Expression child = sessionVarGuardExpr.child();
        Expression res;
        if (child instanceof NeedSessionVarGuard) {
            res = sessionVarGuardExpr;
            return super.visit(res, context);
        } else {
            if (child instanceof SessionVarGuardExpr) {
                if (sessionVarGuardExpr.getSessionVars().equals(((SessionVarGuardExpr) child).getSessionVars())) {
                    res = child;
                } else {
                    throw new AnalysisException("Conflicting session variable guards:"
                            + sessionVarGuardExpr.getSessionVars() + " and "
                            + ((SessionVarGuardExpr) child).getSessionVars());
                }
            } else {
                res = child;
            }
            return res.accept(this, context);
        }
    }
}
