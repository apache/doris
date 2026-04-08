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

package org.apache.doris.analysis;

import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StructLiteral extends LiteralExpr {
    // only for persist
    public StructLiteral() {
        type = new StructType();
        children = new ArrayList<>();
    }

    /**
     * for nereids
     */
    public StructLiteral(Type type, LiteralExpr... exprs) throws AnalysisException {
        this.type = type;
        this.children = new ArrayList<>();
        for (LiteralExpr expr : exprs) {
            if (!StructType.STRUCT.supportSubType(expr.getType())) {
                throw new AnalysisException("Invalid element type in STRUCT: " + expr.getType());
            }
            children.add(expr);
        }
        this.nullable = false;
    }

    protected StructLiteral(StructLiteral other) {
        super(other);
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitStructLiteral(this, context);
    }

    private String getStringValue(Expr expr) {
        String stringValue = expr.getStringValue();
        if (stringValue.isEmpty()) {
            return "''";
        }
        if (expr instanceof StringLiteral) {
            return "\"" + stringValue + "\"";
        }
        return stringValue;
    }

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(getStringValue(v)));
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public Expr clone() {
        return new StructLiteral(this);
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        for (Expr e : children) {
            e.checkValueValid();
        }
    }

    public int hashCode() {
        return Objects.hashCode(children);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StructLiteral)) {
            return false;
        }
        if (this == o) {
            return true;
        }

        StructLiteral that = (StructLiteral) o;
        return Objects.equals(children, that.children);
    }
}
