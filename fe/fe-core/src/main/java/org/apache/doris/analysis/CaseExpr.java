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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.thrift.TCaseExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

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
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        StringBuilder output = new StringBuilder("CASE");
        int childIdx = 0;
        if (hasCaseExpr) {
            output.append(' ')
                    .append(children.get(childIdx++).toSql(disableTableName, needExternalSql, tableType, table));
        }
        while (childIdx + 2 <= children.size()) {
            output.append(
                    " WHEN " + children.get(childIdx++).toSql(disableTableName, needExternalSql, tableType, table));
            output.append(
                    " THEN " + children.get(childIdx++).toSql(disableTableName, needExternalSql, tableType, table));
        }
        if (hasElseExpr) {
            output.append(" ELSE " + children.get(children.size() - 1)
                    .toSql(disableTableName, needExternalSql, tableType, table));
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
