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
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.clearspring.analytics.util.Lists;

import java.util.List;

/**
 * Optimize the InPredicate when the child expr type is integerType, largeIntType, floatingPointType, decimalV2,
 * char, varchar, string and the column type is integerType, largeIntType: convert the child expr type to the column
 * type and discard the expressions that cannot be converted exactly.
 * <p>
 * For example:
 * column type is integerType or largeIntType, then
 * col in (1, 2.5, 2.0, "3.0", "4.6") -> col in (1, 2, 3)
 * col in (2.5, "4.6") -> false
 * <p>
 * column type is tinyType, then
 * col in (1, 2.0, 128, "1000") -> col in (1, 2)
 */
public class RewriteInPredicateRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new RewriteInPredicateRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ClauseType clauseType) throws AnalysisException {
        if (!(expr instanceof InPredicate)) {
            return expr;
        }
        InPredicate inPredicate = (InPredicate) expr;
        if (inPredicate.contains(Subquery.class) || !inPredicate.isLiteralChildren() || inPredicate.isNotIn() ||
                !(inPredicate.getChild(0).unwrapExpr(false) instanceof SlotRef)) {
            return expr;
        }
        SlotRef slotRef = inPredicate.getChild(0).getSrcSlotRef();
        Type columnType = slotRef.getColumn().getType();
        if (!columnType.isFixedPointType()) {
            return expr;
        }

        InPredicate newInPredicate = inPredicate.clone();
        boolean isCast = false;
        List<Expr> invalidChildren = Lists.newArrayList();
        for (int i = 1; i < newInPredicate.getChildren().size(); ++i) {
            LiteralExpr childExpr = (LiteralExpr) newInPredicate.getChild(i);
            if (childExpr.getType().getPrimitiveType().equals(columnType.getPrimitiveType())) {
                continue;
            }

            if (childExpr.getType().getPrimitiveType().isCharFamily() || childExpr.getType().isFloatingPointType()) {
                try {
                    childExpr = (LiteralExpr) childExpr.castTo(Type.DECIMALV2);
                } catch (Exception e) {
                    newInPredicate.setChild(i, childExpr);
                    invalidChildren.add(childExpr);
                    continue;
                }
            }

            if (childExpr.getType().isNumericType()) {
                try {
                    LiteralExpr newExpr = (LiteralExpr) childExpr.castTo(columnType);
                    newExpr.checkValueValid();
                    if (childExpr.compareLiteral(newExpr) == 0) {
                        newInPredicate.setChild(i, newExpr);
                        isCast = true;
                    } else {
                        throw new AnalysisException("Converting the type will result in a loss of accuracy.");
                    }
                } catch (Exception e) {
                    newInPredicate.setChild(i, childExpr);
                    invalidChildren.add(childExpr);
                }
            } else {
                return expr;
            }
        }
        if (invalidChildren.size() == newInPredicate.getChildren().size() - 1) {
            return new BoolLiteral(false);
        }
        if (newInPredicate.getChild(0).getType().getPrimitiveType() != columnType.getPrimitiveType()) {
            newInPredicate.castChild(columnType, 0);
            isCast = true;
        }
        newInPredicate.getChildren().removeAll(invalidChildren);
        return !isCast && invalidChildren.isEmpty() ? expr : newInPredicate;
    }
}
