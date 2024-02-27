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

package org.apache.doris.rewrite.mvrewrite;

import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

/**
 * Only support the once match from originExpr and newExpr
 * TODO：one query expr could be calculate by a group by mv columns
 * TODO: mvExprEqual(queryexpr, mvColumnExprList)
 */
public class MVExprEquivalent {

    private static final ImmutableList<MVExprEqualRule> exprRewriteRuleList = ImmutableList
            .<MVExprEqualRule>builder()
            .add(FunctionCallEqualRule.INSTANCE)
            .add(SlotRefEqualRule.INSTANCE)
            .build();

    public static boolean mvExprEqual(Expr queryExpr, Expr mvColumnExpr) {
        for (MVExprEqualRule rule : exprRewriteRuleList) {
            if (rule.equal(queryExpr, mvColumnExpr)) {
                return true;
            }
        }
        return false;
    }

    public static boolean aggregateArgumentEqual(Expr expr, Expr mvArgument) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }

        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (fnExpr.getChildren().size() != 1) {
            return false;
        }

        String fnName = fnExpr.getFnName().getFunction();

        String slotName = MaterializedIndexMeta.normalizeName(fnExpr.getChild(0).toSqlWithoutTbl());

        if (!CreateMaterializedViewStmt.isMVColumnAggregate(slotName)) {
            return false;
        }

        if (fnName.equalsIgnoreCase(FunctionSet.COUNT)) {
            return sumArgumentEqual(fnExpr.getChild(0), mvArgument);
        }

        if (fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)) {
            return bitmapArgumentEqual(fnExpr.getChild(0), mvArgument);
        }

        return CreateMaterializedViewStmt
                .mvColumnBreaker(slotName).equals(MaterializedIndexMeta.normalizeName(mvArgument.toSqlWithoutTbl()));
    }

    // count(k1) = sum(case when ... k1)
    public static boolean sumArgumentEqual(Expr expr, Expr mvArgument) {
        try {
            String lhs = CountFieldToSum.slotToCaseWhen(expr).toSqlWithoutTbl();
            String rhs = mvArgument.toSqlWithoutTbl();
            return lhs.equalsIgnoreCase(rhs);
        } catch (AnalysisException e) {
            if (ConnectContext.get() != null) {
                ConnectContext.get().getState().reset();
            }
            return false;
        }
    }

    // to_bitmap(k1) = to_bitmap_with_check(k1)
    public static boolean bitmapArgumentEqual(Expr expr, Expr mvArgument) {
        String lhs = MaterializedIndexMeta
                .normalizeName(expr.toSqlWithoutTbl().replace(FunctionSet.TO_BITMAP_WITH_CHECK, FunctionSet.TO_BITMAP));
        String rhs = MaterializedIndexMeta.normalizeName(
                mvArgument.toSqlWithoutTbl().replace(FunctionSet.TO_BITMAP_WITH_CHECK, FunctionSet.TO_BITMAP));
        return CreateMaterializedViewStmt
                .mvColumnBreaker(lhs).equalsIgnoreCase(rhs);
    }
}
