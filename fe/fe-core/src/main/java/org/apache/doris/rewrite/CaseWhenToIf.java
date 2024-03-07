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
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;

import java.util.ArrayList;

/**
 * Change function 'case when' to 'if', same with
 * nereids/rules/expression/rules/CaseWhenToIf.java
 */
public final class CaseWhenToIf implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new CaseWhenToIf();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof CaseExpr)) {
            return expr;
        }
        CaseExpr caseWhen = (CaseExpr) expr;
        if (caseWhen.getConditionExprs().size() == 1) {
            ArrayList<Expr> ifArgs = Lists.newArrayList();
            ifArgs.add(caseWhen.getConditionExprs().get(0));
            ifArgs.add(caseWhen.getReturnExprs().get(0));
            ifArgs.add(caseWhen.getFinalResult());
            return new FunctionCallExpr("if", ifArgs);
        }
        return expr;
    }
}
