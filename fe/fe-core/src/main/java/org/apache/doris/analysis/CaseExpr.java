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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CaseExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TCaseExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * CASE and DECODE are represented using this class. The backend implementation is
 * always the "case" function.
 *
 * The internal representation of
 *   CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr). If a case
 * expr is given then it is the first child. If an else expr is given then it is the
 * last child.
 *
 * The internal representation of
 *   DECODE(expr, key_expr, val_expr [, key_expr, val_expr ...] [, default_val_expr])
 * has a pair of children for each pair of key/val_expr and an additional child if the
 * default_val_expr was given. The first child represents the comparison of expr to
 * key_expr. Decode has three forms:
 *   1) DECODE(expr, null_literal, val_expr) -
 *       child[0] = IsNull(expr)
 *   2) DECODE(expr, non_null_literal, val_expr) -
 *       child[0] = Eq(expr, literal)
 *   3) DECODE(expr1, expr2, val_expr) -
 *       child[0] = Or(And(IsNull(expr1), IsNull(expr2)),  Eq(expr1, expr2))
 * The children representing val_expr (child[1]) and default_val_expr (child[2]) are
 * simply the exprs themselves.
 *
 * Example of equivalent CASE for DECODE(foo, 'bar', 1, col, 2, NULL, 3, 4):
 *   CASE
 *     WHEN foo = 'bar' THEN 1   -- no need for IS NULL check
 *     WHEN foo IS NULL AND col IS NULL OR foo = col THEN 2
 *     WHEN foo IS NULL THEN 3  -- no need for equality check
 *     ELSE 4
 *   END
 */
public class CaseExpr extends Expr {
    @SerializedName("hce")
    private boolean hasCaseExpr;
    @SerializedName("hee")
    private boolean hasElseExpr;

    private CaseExpr() {
        // use for serde only
    }

    public CaseExpr(Expr caseExpr, List<CaseWhenClause> whenClauses, Expr elseExpr) {
        super();
        if (caseExpr != null) {
            children.add(caseExpr);
            hasCaseExpr = true;
        }
        for (CaseWhenClause whenClause : whenClauses) {
            Preconditions.checkNotNull(whenClause.getWhenExpr());
            children.add(whenClause.getWhenExpr());
            Preconditions.checkNotNull(whenClause.getThenExpr());
            children.add(whenClause.getThenExpr());
        }
        if (elseExpr != null) {
            children.add(elseExpr);
            hasElseExpr = true;
        }
    }

    /**
     * use for Nereids ONLY
     */
    public CaseExpr(List<CaseWhenClause> whenClauses, Expr elseExpr) {
        this(null, whenClauses, elseExpr);
        // nereids do not have CaseExpr, and nereids will unify the types,
        // so just use the first then type
        type = children.get(1).getType();
    }

    protected CaseExpr(CaseExpr other) {
        super(other);
        hasCaseExpr = other.hasCaseExpr;
        hasElseExpr = other.hasElseExpr;
    }

    @Override
    public Expr clone() {
        return new CaseExpr(this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), hasCaseExpr, hasElseExpr);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        CaseExpr expr = (CaseExpr) obj;
        return hasCaseExpr == expr.hasCaseExpr && hasElseExpr == expr.hasElseExpr;
    }

    public boolean hasCaseExpr() {
        return hasCaseExpr;
    }

    @Override
    public String toSqlImpl() {
        StringBuilder output = new StringBuilder("CASE");
        int childIdx = 0;
        if (hasCaseExpr) {
            output.append(' ').append(children.get(childIdx++).toSql());
        }
        while (childIdx + 2 <= children.size()) {
            output.append(" WHEN " + children.get(childIdx++).toSql());
            output.append(" THEN " + children.get(childIdx++).toSql());
        }
        if (hasElseExpr) {
            output.append(" ELSE " + children.get(children.size() - 1).toSql());
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder sb = new StringBuilder("CASE");
        int childIdx = 0;
        if (hasCaseExpr) {
            sb.append(" ").append(children.get(childIdx++).toDigest());
        }
        while (childIdx + 2 <= children.size()) {
            sb.append(" WHEN ").append(children.get(childIdx++).toDigest());
            sb.append(" THEN ").append(children.get(childIdx++).toDigest());
        }
        if (hasElseExpr) {
            sb.append(" ELSE ").append(children.get(children.size() - 1).toDigest());
        }
        sb.append(" END");
        return sb.toString();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.CASE_EXPR;
        msg.case_expr = new TCaseExpr(hasCaseExpr, hasElseExpr);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // Keep track of maximum compatible type of case expr and all when exprs.
        Type whenType = null;
        // Keep track of maximum compatible type of else expr and all then exprs.
        Type returnType = null;
        // Remember last of these exprs for error reporting.
        Expr lastCompatibleThenExpr = null;
        Expr lastCompatibleWhenExpr = null;
        int loopEnd = children.size();
        if (hasElseExpr) {
            --loopEnd;
        }
        int loopStart;
        Expr caseExpr = null;
        // Set loop start, and initialize returnType as type of castExpr.
        if (hasCaseExpr) {
            loopStart = 1;
            caseExpr = children.get(0);
            caseExpr.analyze(analyzer);
            if (caseExpr instanceof Subquery && !caseExpr.getType().isScalarType()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            if (caseExpr.getType().isBitmapType()) {
                throw new AnalysisException("Unsupported bitmap type in expression: " + toSql());
            }
            whenType = caseExpr.getType();
            lastCompatibleWhenExpr = children.get(0);
        } else {
            whenType = Type.BOOLEAN;
            loopStart = 0;
        }

        // Go through when/then exprs and determine compatible types.
        for (int i = loopStart; i < loopEnd; i += 2) {
            Expr whenExpr = children.get(i);
            if (hasCaseExpr) {
                // Determine maximum compatible type of the case expr,
                // and all when exprs seen so far. We will add casts to them at the very end.
                whenType = analyzer.getCompatibleType(whenType, lastCompatibleWhenExpr, whenExpr);
                lastCompatibleWhenExpr = whenExpr;
            } else {
                // If no case expr was given, then the when exprs should always return
                // boolean or be castable to boolean.
                if (!Type.canCastTo(whenExpr.getType(), Type.BOOLEAN)) {
                    throw new AnalysisException("When expr '" + whenExpr.toSql() + "'"
                            + " is not of type boolean and not castable to type boolean.");
                }
                // Add a cast if necessary.
                if (!whenExpr.getType().isBoolean()) {
                    castChild(Type.BOOLEAN, i);
                }
            }
            if (whenExpr instanceof Subquery && !whenExpr.getType().isScalarType()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            if (whenExpr.contains(Predicates.instanceOf(Subquery.class))
                    && !((hasCaseExpr() && whenExpr instanceof Subquery || !checkSubquery(whenExpr)))) {
                throw new AnalysisException("Only support subquery in binary predicate in case statement.");
            }
            if (whenExpr.getType().isBitmapType()) {
                throw new AnalysisException("Unsupported bitmap type in expression: " + toSql());
            }
            // Determine maximum compatible type of the then exprs seen so far.
            // We will add casts to them at the very end.
            Expr thenExpr = children.get(i + 1);
            if (thenExpr instanceof Subquery && !thenExpr.getType().isScalarType()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            returnType = analyzer.getCompatibleType(returnType, lastCompatibleThenExpr, thenExpr);
            lastCompatibleThenExpr = thenExpr;
        }
        if (hasElseExpr) {
            Expr elseExpr = children.get(children.size() - 1);
            if (elseExpr instanceof Subquery && !elseExpr.getType().isScalarType()) {
                throw new AnalysisException("Subquery in case-when must return scala type");
            }
            returnType = analyzer.getCompatibleType(returnType, lastCompatibleThenExpr, elseExpr);
        }

        // Add casts to case expr to compatible type.
        if (hasCaseExpr) {
            // Cast case expr.
            if (!children.get(0).getType().equals(whenType)) {
                castChild(whenType, 0);
            }
            // Add casts to when exprs to compatible type.
            for (int i = loopStart; i < loopEnd; i += 2) {
                if (!children.get(i).getType().equals(whenType)) {
                    castChild(whenType, i);
                }
            }
        }
        // Cast then exprs to compatible type.
        for (int i = loopStart + 1; i < children.size(); i += 2) {
            if (!children.get(i).getType().equals(returnType)) {
                castChild(returnType, i);
            }
        }
        // Cast else expr to compatible type.
        if (hasElseExpr) {
            if (!children.get(children.size() - 1).getType().equals(returnType)) {
                castChild(returnType, children.size() - 1);
            }
        }

        type = returnType;
    }

    // case and when
    public List<Expr> getConditionExprs() {
        List<Expr> exprs = Lists.newArrayList();
        int childIdx = 0;
        if (hasCaseExpr) {
            exprs.add(children.get(childIdx++));
        }
        while (childIdx + 2 <= children.size()) {
            exprs.add(children.get(childIdx++));
            childIdx++;
        }
        return exprs;
    }

    // then
    public List<Expr> getReturnExprs() {
        List<Expr> exprs = Lists.newArrayList();
        int childIdx = 0;
        if (hasCaseExpr) {
            childIdx++;
        }
        while (childIdx + 2 <= children.size()) {
            childIdx++;
            exprs.add(children.get(childIdx++));
        }
        if (hasElseExpr) {
            exprs.add(children.get(children.size() - 1));
        }
        return exprs;
    }

    // this method just compare literal value and not completely consistent with be,for two cases
    // 1 not deal float
    // 2 just compare literal value with same type.
    //      for a example sql 'select case when 123 then '1' else '2' end as col'
    //      for be will return '1', because be only regard 0 as false
    //      but for current LiteralExpr.compareLiteral, `123`' won't be regard as true
    //  the case which two values has different type left to be
    public static Expr computeCaseExpr(CaseExpr expr) {
        if (expr.getType() == Type.NULL) {
            // if expr's type is NULL_TYPE, means all possible return values are nulls
            // it's safe to return null literal here
            return new NullLiteral();
        }
        LiteralExpr caseExpr;
        int startIndex = 0;
        int endIndex = expr.getChildren().size();

        // CastExpr contains SlotRef child should be reset to re-analyze in selectListItem
        for (Expr child : expr.getChildren()) {
            if (child instanceof CastExpr && (child.contains(SlotRef.class))) {
                List<CastExpr> castExprList = Lists.newArrayList();
                child.collect(CastExpr.class, castExprList);
                for (CastExpr castExpr : castExprList) {
                    castExpr.resetAnalysisState();
                }
            }
        }

        if (expr.hasCaseExpr()) {
            // just deal literal here
            // and avoid `float compute` in java,float should be dealt in be
            Expr caseChildExpr = expr.getChild(0);
            if (!caseChildExpr.isLiteral()
                    || caseChildExpr instanceof DecimalLiteral || caseChildExpr instanceof FloatLiteral) {
                return expr;
            }
            caseExpr = (LiteralExpr) expr.getChild(0);
            startIndex++;
        } else {
            caseExpr = new BoolLiteral(true);
        }

        if (caseExpr instanceof NullLiteral) {
            return expr.getFinalResult();
        }

        if (expr.hasElseExpr) {
            endIndex--;
        }

        // early return when the `when expr` can't be converted to constants
        Expr startExpr = expr.getChild(startIndex);
        if ((!startExpr.isLiteral() || startExpr instanceof DecimalLiteral || startExpr instanceof FloatLiteral)
                || (!(startExpr instanceof NullLiteral)
                && !startExpr.getClass().toString().equals(caseExpr.getClass().toString()))) {
            return expr;
        }

        for (int i = startIndex; i < endIndex; i = i + 2) {
            Expr currentWhenExpr = expr.getChild(i);
            // skip null literal
            if (currentWhenExpr instanceof NullLiteral) {
                continue;
            }
            // stop convert in three cases
            // 1 not literal
            // 2 float
            // 3 `case expr` and `when expr` don't have same type
            if ((!currentWhenExpr.isLiteral()
                    || currentWhenExpr instanceof DecimalLiteral
                    || currentWhenExpr instanceof FloatLiteral)
                    || !currentWhenExpr.getClass().toString().equals(caseExpr.getClass().toString())) {
                // remove the expr which has been evaluated
                List<Expr> exprLeft = new ArrayList<>();
                if (expr.hasCaseExpr()) {
                    exprLeft.add(caseExpr);
                }
                for (int j = i; j < expr.getChildren().size(); j++) {
                    exprLeft.add(expr.getChild(j));
                }
                Expr retCaseExpr = expr.clone();
                retCaseExpr.getChildren().clear();
                retCaseExpr.addChildren(exprLeft);
                return retCaseExpr;
            } else if (caseExpr.compareLiteral((LiteralExpr) currentWhenExpr) == 0) {
                return expr.getChild(i + 1);
            }
        }

        return expr.getFinalResult();
    }

    public Expr getFinalResult() {
        if (hasElseExpr) {
            return getChild(getChildren().size() - 1);
        } else {
            return new NullLiteral();
        }
    }

    // check if subquery in `in` or `exists` Predicate
    private boolean checkSubquery(Expr expr) {
        for (Expr child : expr.getChildren()) {
            if (child instanceof Subquery && (expr instanceof ExistsPredicate || expr instanceof InPredicate)) {
                return true;
            }
            if (checkSubquery(child)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isNullable() {
        int loopStart;
        int loopEnd = children.size();
        if (hasCaseExpr) {
            loopStart = 2;
        } else {
            loopStart = 1;
        }
        if (hasElseExpr) {
            --loopEnd;
        }
        for (int i = loopStart; i < loopEnd; i += 2) {
            Expr thenExpr = children.get(i);
            if (thenExpr.isNullable()) {
                return true;
            }
        }
        if (hasElseExpr) {
            if (children.get(children.size() - 1).isNullable()) {
                return true;
            }
        } else {
            return true;
        }
        return false;
    }
}
