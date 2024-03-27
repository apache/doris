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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter;

import java.util.HashMap;
import java.util.Map;

public class SetUserDefinedVar extends SetVar {
    public SetUserDefinedVar(String variable, Expr value) {
        super(SetType.USER, variable, value, SetVarType.SET_USER_DEFINED_VAR);
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException  {
        Expr expression = getValue();
        if (!(expression instanceof LiteralExpr)) {
            boolean changed = true;
            for (int i = 0; i < 1000 && changed; i++) {
                Pair<Expr, Boolean> result = fold(analyzer, expression);
                expression = result.first;
                changed = result.second;
            }
        }
        if (expression instanceof NullLiteral) {
            setResult(NullLiteral.create(ScalarType.NULL));
        } else if (expression instanceof LiteralExpr) {
            setResult((LiteralExpr) expression);
        } else {
            throw new AnalysisException("Unsupported to set " + expression.toSql() + " user defined variables.");
        }
    }

    public void registerExprId(Expr expr, Analyzer analyzer) {
        if (expr.getId() == null) {
            analyzer.registerExprId(expr);
        }
        for (Expr child : expr.getChildren()) {
            registerExprId(child, analyzer);
        }
    }

    private Pair<Expr, Boolean> fold(Analyzer analyzer, Expr expression) throws AnalysisException {
        expression.analyze(analyzer);
        registerExprId(expression, analyzer);
        ExprRewriter rewriter = analyzer.getExprRewriter();
        rewriter.reset();
        Map<String, Expr> exprMap = new HashMap<>();
        exprMap.put(expression.getId().toString(), expression);
        rewriter.rewriteConstant(exprMap, analyzer, ConnectContext.get().getSessionVariable().toThrift());
        return Pair.of(exprMap.get(expression.getId().toString()), rewriter.changed());
    }
}
