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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.common.AnalysisException;

/**
 * MatchPredicate only support in WHERE_CLAUSE
 */
public final class MatchPredicateRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new MatchPredicateRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (expr instanceof MatchPredicate && clauseType != ExprRewriter.ClauseType.WHERE_CLAUSE) {
            throw new AnalysisException("Not support in " + clauseType.toString()
                + ", only support in WHERE_CLAUSE, expression: " + expr.toSql());
        }
        return expr;
    }

    private MatchPredicateRule() {}
}
