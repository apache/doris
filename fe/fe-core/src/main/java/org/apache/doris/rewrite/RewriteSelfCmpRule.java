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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;

/**
 * Rewrites predicate which is self compare self like A = A.
 * =, >= , <=: where col is not null;
 * <, >, !=: where false;
 * <=>: where true;
 */
public class RewriteSelfCmpRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RewriteSelfCmpRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof BinaryPredicate) || !(expr.getChild(0) instanceof SlotRef)
                || !expr.getChild(0).equals(expr.getChild(1)))
            return expr;

        switch (((BinaryPredicate) expr).getOp()) {
            case EQ:
            case LE:
            case GE:
                return new IsNullPredicate(expr.getChild(0), true);
            case NE:
            case LT:
            case GT:
                return new BoolLiteral(false);
            case EQ_FOR_NULL:
                return new BoolLiteral(true);
        }
        return expr;
    }
}
