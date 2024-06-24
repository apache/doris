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
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.Subquery;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rewrite.ExprRewriter.ClauseType;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Optimize the InPredicate when the child expr type is integerType, largeIntType, floatingPointType, decimalV2,
 * char, varchar, string and the column type is integerType, largeIntType: convert the child expr type to the column
 * type and discard the expressions that cannot be converted exactly.
 *
 * <p>For example:<br>
 * column type is integerType or largeIntType, then:<br>
 * col in (1, 2.5, 2.0, "3.0", "4.6") -> col in (1, 2, 3)<br>
 * col in (2.5, "4.6") -> false<br>
 * column type is tinyType, then:<br>
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
        SlotRef slotRef;
        // When the select stmt contains group by, we use oriGroupingExprs to store the original group by statement
        // and reset it with the rewritten groupingExpr. Therefore, origroupingexprs cannot be analyzed.
        // However, in #4197, oriGroupingExprs is rewritten to fix the problem of constant fold.
        // The newly added InPredicteRewriteRule requires that expr must be analyzed before being rewritten
        if (!inPredicate.isAnalyzed() || inPredicate.contains(Subquery.class) || !inPredicate.isLiteralChildren()
                || inPredicate.isNotIn() || !(inPredicate.getChild(0).unwrapExpr(false) instanceof SlotRef)
                || (slotRef = inPredicate.getChild(0).tryGetSrcSlotRef()) == null || slotRef.getColumn() == null) {
            return expr;
        }
        Type columnType = slotRef.getColumn().getType();
        if (!columnType.isFixedPointType()) {
            return expr;
        }

        Expr newColumnExpr;
        if (expr.getChild(0).getType().getPrimitiveType() == columnType.getPrimitiveType()) {
            newColumnExpr = expr.getChild(0);
        } else {
            if (inPredicate.getChild(0) instanceof CastExpr && inPredicate.getChild(0).getChild(0).getType()
                    .equals(columnType)) {
                newColumnExpr = inPredicate.getChild(0).getChild(0);
            } else {
                newColumnExpr = expr.getChild(0).castTo(columnType);
            }
        }
        List<Expr> newInList = Lists.newArrayList();
        boolean isCast = false;
        for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
            LiteralExpr childExpr = (LiteralExpr) inPredicate.getChild(i);
            if (!(childExpr.getType().isNumericType() || childExpr.getType().getPrimitiveType().isCharFamily())) {
                return expr;
            }
            if (childExpr.getType().getPrimitiveType().equals(columnType.getPrimitiveType())) {
                newInList.add(childExpr);
                continue;
            }

            // StringLiteral "2.0" cannot be directly converted to IntLiteral or LargeIntLiteral, and FloatLiteral
            // cannot be directly converted to LargeIntLiteral, so it is converted to decimal first.
            if (childExpr.getType().getPrimitiveType().isCharFamily() || childExpr.getType().isFloatingPointType()) {
                try {
                    if (childExpr instanceof PlaceHolderExpr) {
                        childExpr = ((PlaceHolderExpr) childExpr).getLiteral();
                    }
                    childExpr = (LiteralExpr) childExpr.castTo(Type.DECIMALV2);
                } catch (AnalysisException e) {
                    if (ConnectContext.get() != null) {
                        ConnectContext.get().getState().reset();
                    }
                    continue;
                }
            }

            try {
                // Convert childExpr to column type and compare the converted values. There are 3 possible situations:
                // 1. The value of childExpr exceeds the range of the column type, then castTo() will throw an
                //   exception. For example, the value of childExpr is 128 and the column type is tinyint.
                // 2. childExpr is converted to column type, but the value of childExpr loses precision.
                //   For example, 2.1 is converted to 2;
                // 3. childExpr is precisely converted to column type. For example, 2.0 is converted to 2.
                // In cases 1 and 2 above, childExpr should be discarded.
                if (childExpr instanceof PlaceHolderExpr) {
                    childExpr = ((PlaceHolderExpr) childExpr).getLiteral();
                }
                LiteralExpr newExpr = (LiteralExpr) childExpr.castTo(columnType);
                if (childExpr.compareLiteral(newExpr) == 0) {
                    isCast = true;
                    newInList.add(newExpr);
                }
            } catch (AnalysisException ignored) {
                if (ConnectContext.get() != null) {
                    ConnectContext.get().getState().reset();
                }
                // pass
            }
        }
        if (newInList.isEmpty()) {
            return new BoolLiteral(false);
        }
        // Expr rewriting if there is childExpr discarded or type is converted.
        return newInList.size() + 1 < expr.getChildren().size() || isCast
                ? new InPredicate(newColumnExpr, newInList, false) : expr;
    }
}
