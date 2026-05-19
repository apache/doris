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
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;

/**
 * Class representing a [NOT] IN predicate. It determines if a specified value
 * (first child) matches any value in a subquery (second child) or a list
 * of values (remaining children).
 */
public class InPredicate extends Predicate {

    private static final String IN_ITERATE = "in_iterate";
    private static final String NOT_IN_ITERATE = "not_in_iterate";
    @SerializedName("ini")
    private boolean isNotIn;
    @SerializedName("ac")
    private boolean allConstant;

    private InPredicate() {
        // use for serde only
        this.allConstant = false;
    }

    /** First child is the comparison expr for which we
     * should check membership in the inList (the remaining children).
     * NOTICE: only used in test
     */
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
        children.add(compareExpr);
        children.addAll(inList);
        this.isNotIn = isNotIn;
        this.allConstant = false;
    }

    /**
     * use for Nereids ONLY
     */
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn, boolean allConstant, boolean nullable) {
        this(compareExpr, inList, isNotIn);
        this.allConstant = allConstant;
        type = Type.BOOLEAN;
        if (!allConstant) {
            fn = new Function(new FunctionName(isNotIn ? NOT_IN_ITERATE : IN_ITERATE),
                    Lists.newArrayList(getChild(0).getType(), getChild(1).getType()), Type.BOOLEAN,
                    true, true, NullableMode.DEPEND_ON_ARGUMENT);
        }
        this.nullable = nullable;
    }

    protected InPredicate(InPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
        allConstant = other.allConstant;
    }

    public int getInElementNum() {
        // the first child is compare expr
        return getChildren().size() - 1;
    }

    @Override
    public InPredicate clone() {
        return new InPredicate(this);
    }

    public List<Expr> getListChildren() {
        return children.subList(1, children.size());
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public boolean getAllConstant() {
        return allConstant;
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
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitInPredicate(this, context);
    }

    @Override
    public String toString() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = isNotIn ? "NOT " : "";
        strBuilder.append(getChild(0))
                .append(" ").append(notStr).append("IN (");
        for (int i = 1; i < getChildren().size(); ++i) {
            strBuilder.append(getChild(i));
            strBuilder.append((i + 1 != getChildren().size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
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
}
