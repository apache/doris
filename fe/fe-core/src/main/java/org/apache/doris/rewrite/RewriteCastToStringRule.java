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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionParams;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;

/**
 * transfer cast(xx as char(N)/varchar(N)) to substr(cast(xx as char), 1, N)
 */
public class RewriteCastToStringRule implements ExprRewriteRule {
    public static RewriteCastToStringRule INSTANCE = new RewriteCastToStringRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (expr instanceof CastExpr) {
            CastExpr castExpr = ((CastExpr) expr);
            TypeDef targetType = castExpr.getTargetTypeDef();
            if (targetType.getType().getLength() != -1
                    && (targetType.getType().getPrimitiveType() == PrimitiveType.VARCHAR
                        || targetType.getType().getPrimitiveType() == PrimitiveType.CHAR)) {
                ArrayList<Expr> exprs = new ArrayList<>();
                CastExpr subCastExpr = new CastExpr(TypeDef.createVarchar(-1), castExpr.getChild(0));
                exprs.add(subCastExpr);
                exprs.add(new IntLiteral(1));
                exprs.add(new IntLiteral(targetType.getType().getLength()));
                return new FunctionCallExpr("substr", new FunctionParams(exprs));
            }
        }
        return expr;
    }
}
