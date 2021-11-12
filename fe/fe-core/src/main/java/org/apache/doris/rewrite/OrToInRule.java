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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.AnalysisException;

import java.util.Arrays;
import java.util.List;

/**
 * OR Expr cannot be pushdown on the storage side, rewrite it to InPredicate.
 * SQL: select * from tbl where k = 0 or k = 1 or k in (2, 3);
 * After rewrite: select * from tbl where k in (0, 1, 2, 3)
 * */
public class OrToInRule implements ExprRewriteRule {
    public static final ExprRewriteRule INSTANCE = new OrToInRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }

        CompoundPredicate cp = (CompoundPredicate) expr;
        if (cp.getOp() != CompoundPredicate.Operator.OR) {
            return expr;
        }

        Pair<Expr, List<Expr>> left = extractLiteralExpr(cp.getChild(0));
        if (left == null) {
            return expr;
        }

        Pair<Expr, List<Expr>> right = extractLiteralExpr(cp.getChild(1));
        if (right == null) {
            return expr;
        }

        if (!left.getKey().equals(right.getKey())) {
            return expr;
        }

        InPredicate ret = new InPredicate(left.getKey(), left.getValue(), false);
        ret.addChildren(right.getValue());
        return ret;
    }

    private Pair<Expr, List<Expr>> extractLiteralExpr(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            BinaryPredicate bp = (BinaryPredicate) expr;
            if (bp.getOp() != BinaryPredicate.Operator.EQ) {
                return null;
            }

            if (!(bp.getChild(0) instanceof SlotRef)) {
                return null;
            }

            if (!(bp.getChild(1) instanceof LiteralExpr)) {
                return null;
            }

            return Pair.of(bp.getChild(0), Arrays.asList(bp.getChild(1)));
        }

        if (expr instanceof InPredicate) {
            InPredicate ip = (InPredicate) expr;
            if (ip.isNotIn()) {
                return null;
            }

            if (!(ip.getChild(0) instanceof SlotRef)) {
                return null;
            }

            boolean allIsLiteral = ip.getListChildren().stream().allMatch(c -> c instanceof LiteralExpr);
            if (!allIsLiteral) {
                return null;
            } else {
                return Pair.of(ip.getChild(0), ip.getListChildren());
            }
        }

        return null;
    }
}
