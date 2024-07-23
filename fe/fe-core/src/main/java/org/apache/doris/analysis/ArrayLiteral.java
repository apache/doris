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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ArrayLiteral extends LiteralExpr {

    public ArrayLiteral() {
        type = new ArrayType(Type.NULL);
        children = new ArrayList<>();
    }

    public ArrayLiteral(Type type, LiteralExpr... exprs) {
        this.type = type;
        children = new ArrayList<>(Arrays.asList(exprs));
        analysisDone();
    }

    public ArrayLiteral(LiteralExpr... exprs) throws AnalysisException {
        Type itemType = Type.NULL;
        boolean containsNull = true;
        for (LiteralExpr expr : exprs) {
            if (!ArrayType.ARRAY.supportSubType(expr.getType())) {
                throw new AnalysisException("Invalid item type in Array, not support " + expr.getType());
            }
            if (itemType == Type.NULL) {
                itemType = expr.getType();
            } else {
                itemType = Type.getAssignmentCompatibleType(itemType, expr.getType(), false, false);
            }

            if (expr.isNullable()) {
                containsNull = true;
            }
        }

        if (itemType == Type.INVALID) {
            throw new AnalysisException("Invalid element type in ARRAY");
        }

        type = new ArrayType(itemType, containsNull);

        children = new ArrayList<>();
        try {
            for (LiteralExpr expr : exprs) {
                if (expr.getType().equals(itemType)) {
                    children.add(expr);
                } else {
                    children.add(expr.convertTo(itemType));
                }
            }
        } catch (AnalysisException e) {
            String s = "[" + StringUtils.join(exprs, ',') + "]";
            throw new AnalysisException("Invalid ARRAY " + s + " literal: " + e.getMessage());
        }
        analysisDone();
    }

    protected ArrayLiteral(ArrayLiteral other) {
        super(other);
    }


    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        int size = Math.min(expr.getChildren().size(), this.children.size());
        for (int i = 0; i < size; i++) {
            if (((LiteralExpr) (this.getChild(i))).compareTo((LiteralExpr) (expr.getChild(i))) != 0) {
                return ((LiteralExpr) (this.getChild(i))).compareTo((LiteralExpr) (expr.getChild(i)));
            }
        }
        return this.children.size() > expr.getChildren().size() ? 1 :
                (this.children.size() == expr.getChildren().size() ? 0 : -1);
    }

    @Override
    protected String toSqlImpl() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toSqlImpl()));

        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String toDigestImpl() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toDigestImpl()));

        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.getStringValue()));
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.getStringValueForArray(options)));
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValueInFe(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> {
            String stringLiteral;
            if (v instanceof NullLiteral) {
                stringLiteral = options.getNullFormat();
            } else {
                stringLiteral = getStringLiteralForComplexType(v, options);
            }
            // we should use type to decide we output array is suitable for json format
            list.add(stringLiteral);
        });
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValueForStreamLoad(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> {
            String stringLiteral;
            if (v instanceof NullLiteral) {
                stringLiteral = "null";
            } else {
                stringLiteral = getStringLiteralForStreamLoad(v, options);
            }
            // we should use type to decide we output array is suitable for json format
            list.add(stringLiteral);
        });
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARRAY_LITERAL;
        msg.setChildType(((ArrayType) type).getItemType().getPrimitiveType().toThrift());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(children);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ArrayLiteral)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        ArrayLiteral that = (ArrayLiteral) o;
        return Objects.equals(children, that.children);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        children = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            children.add(Expr.readIn(in));
        }
    }

    public static ArrayLiteral read(DataInput in) throws IOException {
        ArrayLiteral literal = new ArrayLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public Expr clone() {
        return new ArrayLiteral(this);
    }

    @Override
    public LiteralExpr convertTo(Type targetType) throws AnalysisException {
        Preconditions.checkState(targetType instanceof ArrayType);
        Type itemType = ((ArrayType) targetType).getItemType();
        LiteralExpr[] literals = new LiteralExpr[children.size()];
        for (int i = 0; i < children.size(); i++) {
            literals[i] = (LiteralExpr) (Expr.convertLiteral(children.get(i), itemType));
        }
        return new ArrayLiteral(literals);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!targetType.isArrayType()) {
            return super.uncheckedCastTo(targetType);
        }
        Type itemType = ((ArrayType) targetType).getItemType();
        ArrayLiteral literal = new ArrayLiteral(this);
        for (int i = 0; i < children.size(); ++ i) {
            Expr child = Expr.convertLiteral(children.get(i), itemType);
            // all children should be literal or else it will make be core
            if (!child.isLiteral()) {
                throw new AnalysisException("Unexpected array literal cast failed. from type: "
                        + this.type + ", to type: " + targetType);
            }
            literal.children.set(i, child);
        }
        literal.setType(targetType);
        return literal;
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        for (Expr e : children) {
            e.checkValueValid();
        }
    }
}
