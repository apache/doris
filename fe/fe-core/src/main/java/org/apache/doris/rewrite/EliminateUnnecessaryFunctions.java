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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

public class EliminateUnnecessaryFunctions implements ExprRewriteRule {

    public static final EliminateUnnecessaryFunctions INSTANCE = new EliminateUnnecessaryFunctions();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof FunctionCallExpr)) {
            return expr;
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        if (functionCallExpr.getFnName().getFunction().equalsIgnoreCase("date")) {
            Expr realParam = functionCallExpr.getChild(0);
            if (realParam instanceof CastExpr) {
                realParam = realParam.getChild(0);
            }
            if (realParam.getType().getPrimitiveType() == functionCallExpr.getType().getPrimitiveType()) {
                return realParam;
            }
        }
        return expr;
    }
}
