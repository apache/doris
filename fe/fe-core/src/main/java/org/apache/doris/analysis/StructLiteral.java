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

import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StructLiteral extends LiteralExpr {
    // only for persist
    public StructLiteral() {
        type = new StructType();
        children = new ArrayList<>();
    }

    public StructLiteral(LiteralExpr... exprs) throws AnalysisException {
        type = new StructType();
        children = new ArrayList<>();
        for (int i = 0; i < exprs.length; i++) {
            if (!StructType.STRUCT.supportSubType(exprs[i].getType())) {
                throw new AnalysisException("Invalid element type in STRUCT: " + exprs[i].getType());
            }
            ((StructType) type).addField(
                    new StructField(StructField.DEFAULT_FIELD_NAME + (i + 1), exprs[i].getType()));
            children.add(exprs[i]);
        }
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
    }

    protected StructLiteral(StructLiteral other) {
        super(other);
    }

    @Override
    protected String toSqlImpl() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toSqlImpl()));
        return "STRUCT(" + StringUtils.join(list, ", ") + ")";
    }

    @Override
    public String toDigestImpl() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toDigestImpl()));
        return "STRUCT(" + StringUtils.join(list, ", ") + ")";
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
    public String getStringValueForArray(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.getStringValueForArray(options)));
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public String getStringValueInFe(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        // same with be default field index start with 1
        for (int i = 0; i < children.size(); i++) {
            Expr child = children.get(i);
            list.add(options.getNestedStringWrapper()
                    + ((StructType) type).getFields().get(i).getName()
                    + options.getNestedStringWrapper()
                    + options.getMapKeyDelim()
                    + getStringLiteralForComplexType(child, options));
        }
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public String getStringValueForStreamLoad(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(getStringLiteralForComplexType(v, options)));
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.STRUCT_LITERAL;
        ((StructType) type).getFields().forEach(v -> msg.setChildType(v.getType().getPrimitiveType().toThrift()));
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        type.toThrift(container);
        msg.setType(container);
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

    public static StructLiteral read(DataInput in) throws IOException {
        StructLiteral literal = new StructLiteral();
        literal.readFields(in);
        return literal;
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
    public LiteralExpr convertTo(Type targetType) throws AnalysisException {
        Preconditions.checkState(targetType instanceof StructType);
        List<StructField> fields = ((StructType) targetType).getFields();
        LiteralExpr[] literals = new LiteralExpr[children.size()];
        for (int i = 0; i < children.size(); i++) {
            literals[i] = (LiteralExpr) Expr.convertLiteral(children.get(i), fields.get(i).getType());
        }
        return new StructLiteral(literals);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!targetType.isStructType()) {
            return super.uncheckedCastTo(targetType);
        }
        ArrayList<StructField> fields = ((StructType) targetType).getFields();
        StructLiteral literal = new StructLiteral(this);
        for (int i = 0; i < children.size(); ++ i) {
            Expr child = Expr.convertLiteral(children.get(i), fields.get(i).getType());
            // all children should be literal or else it will make be core
            if (!child.isLiteral()) {
                throw new AnalysisException("Unexpected struct literal cast failed. from type: "
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
