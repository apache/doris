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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/InPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TInPredicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Class representing a [NOT] IN predicate. It determines if a specified value
 * (first child) matches any value in a subquery (second child) or a list
 * of values (remaining children).
 */
public class InPredicate extends Predicate {

    private static final String IN_SET_LOOKUP = "in_set_lookup";
    private static final String NOT_IN_SET_LOOKUP = "not_in_set_lookup";
    private static final String IN_ITERATE = "in_iterate";
    private static final String NOT_IN_ITERATE = "not_in_iterate";
    @SerializedName("ini")
    private boolean isNotIn;

    private InPredicate() {
        // use for serde only
    }

    // First child is the comparison expr for which we
    // should check membership in the inList (the remaining children).
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
        children.add(compareExpr);
        children.addAll(inList);
        this.isNotIn = isNotIn;
    }

    /**
     * use for Nereids ONLY
     */
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn, boolean allConstant) {
        this(compareExpr, inList, isNotIn);
        type = Type.BOOLEAN;
        if (allConstant) {
            opcode = isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
        } else {
            opcode = isNotIn ? TExprOpcode.FILTER_NEW_NOT_IN : TExprOpcode.FILTER_NEW_IN;
            fn = new Function(new FunctionName(isNotIn ? NOT_IN_ITERATE : IN_ITERATE),
                    Lists.newArrayList(getChild(0).getType(), getChild(1).getType()), Type.BOOLEAN,
                    true, true, NullableMode.DEPEND_ON_ARGUMENT);
        }
    }

    protected InPredicate(InPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
    }

    public int getInElementNum() {
        // the first child is compare expr
        return getChildren().size() - 1;
    }

    @Override
    public InPredicate clone() {
        return new InPredicate(this);
    }

    // C'tor for initializing an [NOT] IN predicate with a subquery child.
    public InPredicate(Expr compareExpr, Expr subquery, boolean isNotIn) {
        Preconditions.checkNotNull(compareExpr);
        Preconditions.checkNotNull(subquery);
        children.add(compareExpr);
        children.add(subquery);
        this.isNotIn = isNotIn;
    }

    /**
     * Negates an InPredicate.
     */
    @Override
    public Expr negate() {
        return new InPredicate(getChild(0), children.subList(1, children.size()), !isNotIn);
    }

    public List<Expr> getListChildren() {
        return children.subList(1, children.size());
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public boolean isLiteralChildren() {
        for (int i = 1; i < children.size(); ++i) {
            if (!(children.get(i) instanceof LiteralExpr)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.in_predicate = new TInPredicate(isNotIn);
        msg.node_type = TExprNodeType.IN_PRED;
        msg.setOpcode(opcode);
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append(getChild(0).toSql() + " " + notStr + "IN (");
        for (int i = 1; i < children.size(); ++i) {
            strBuilder.append(getChild(i).toSql());
            strBuilder.append((i + 1 != children.size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append(
                getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " " + notStr + "IN (");
        for (int i = 1; i < children.size(); ++i) {
            strBuilder.append(getChild(i).toSql(disableTableName, needExternalSql, tableType, table));
            strBuilder.append((i + 1 != children.size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String toDigestImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append(getChild(0).toDigest() + " " + notStr + "IN (");
        for (int i = 1; i < children.size(); ++i) {
            strBuilder.append(getChild(i).toDigest());
            strBuilder.append((i + 1 != children.size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InPredicate expr = (InPredicate) obj;
            if (isNotIn == expr.isNotIn) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isNullable() {
        return hasNullableChild();
    }
}
