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
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.analysis.BoolLiteral;

/**
 * In predicate date rule try to convert date expression, if date is invalid, it will be
 * converted into bool literal to avoid to scan all partitions
 * Examples:
 * date in ("2020-10-32") => FALSE
 */
public class InPredicatesDateRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new InPredicatesDateRule();
    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof InPredicate)) return expr;
        Expr lChild = expr.getChild(0);
        if (lChild.getType() != Type.DATETIME) {
            return expr;
        }
        int i = 1;
        while (i < expr.getChildren().size()) {
            if (expr.getChild(i) instanceof CastExpr) {
                expr.removeNode(i);
            } else {
                i++;
            }
        }
        if (expr.getChildren().size() <= 1) {
            return new BoolLiteral(false);
        }
        return expr;
    }
}
