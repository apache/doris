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

import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


// INSERT INTO table_map VALUES ({'key1':1, 'key2':10, 'k3':100}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
// MapLiteral is one row-based literal
public class MapLiteral extends LiteralExpr {

    public MapLiteral() {
        type = new MapType(Type.NULL, Type.NULL);
        children = new ArrayList<>();
    }

    public MapLiteral(Type type, List<LiteralExpr> keys, List<LiteralExpr> values) {
        this.type = type;
        children = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            children.add(keys.get(i));
            children.add(values.get(i));
        }
    }

    public MapLiteral(LiteralExpr... exprs) throws AnalysisException {
        Type keyType = Type.NULL;
        Type valueType = Type.NULL;
        children = new ArrayList<>();
        // check types here
        // 1. limit key type with map-key support
        // 2. check type can be assigment for cast
        for (int idx = 0; idx < exprs.length && idx + 1 < exprs.length; idx += 2) {
            if (exprs[idx].getType().isComplexType() || !MapType.MAP.supportSubType(exprs[idx].getType())) {
                throw new AnalysisException("Invalid key type in Map, not support " + exprs[idx].getType());
            }
            if (!MapType.MAP.supportSubType(exprs[idx + 1].getType())) {
                throw new AnalysisException("Invalid value type in Map, not support " + exprs[idx + 1].getType());
            }
            boolean enableDecimal256 = SessionVariable.getEnableDecimal256();
            keyType = Type.getAssignmentCompatibleType(keyType, exprs[idx].getType(), true, enableDecimal256);
            valueType = Type.getAssignmentCompatibleType(valueType, exprs[idx + 1].getType(), true, enableDecimal256);
        }

        if (keyType == Type.INVALID) {
            throw new AnalysisException("Invalid key type in Map.");
        }
        if (valueType == Type.INVALID) {
            throw new AnalysisException("Invalid value type in Map.");
        }

        try {
            for (int idx = 0; idx < exprs.length && idx + 1 < exprs.length; idx += 2) {
                if (exprs[idx].getType().equals(keyType)) {
                    children.add(exprs[idx]);
                } else {
                    children.add(exprs[idx].convertTo(keyType));
                }
                if (exprs[idx + 1].getType().equals(valueType)) {
                    children.add(exprs[idx + 1]);
                } else {
                    children.add(exprs[idx + 1].convertTo(valueType));
                }
            }
        } catch (AnalysisException e) {
            String s = "{" + StringUtils.join(exprs, ',') + "}";
            throw new AnalysisException("Invalid Map " + s + " literal: " + e.getMessage());
        }

        type = new MapType(keyType, valueType);
    }

    protected MapLiteral(MapLiteral other) {
        super(other);
    }

    @Override
    public LiteralExpr convertTo(Type targetType) throws AnalysisException {
        Preconditions.checkState(targetType instanceof MapType);
        Type keyType = ((MapType) targetType).getKeyType();
        Type valType = ((MapType) targetType).getValueType();
        LiteralExpr[] literals = new LiteralExpr[children.size()];
        for (int i = 0; i < children.size(); i += 2) {
            literals[i] = (LiteralExpr) Expr.convertLiteral(children.get(i), keyType);
            literals[i + 1] = (LiteralExpr) Expr.convertLiteral(children.get(i + 1), valType);
        }
        return new MapLiteral(literals);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!targetType.isMapType()) {
            return super.uncheckedCastTo(targetType);
        }
        MapLiteral literal = new MapLiteral(this);
        Type keyType = ((MapType) targetType).getKeyType();
        Type valueType = ((MapType) targetType).getValueType();

        for (int i = 0; i < children.size() &&  i + 1 < children.size(); i += 2) {
            Expr key = Expr.convertLiteral(children.get(i), keyType);
            Expr value = Expr.convertLiteral(children.get(i + 1), valueType);
            // all children should be literal or else it will make be core
            if ((!key.isLiteral()) || (!value.isLiteral())) {
                throw new AnalysisException("Unexpected map literal cast failed. from type: "
                        + this.type + ", to type: " + targetType);
            }
            literal.children.set(i, key);
            literal.children.set(i + 1, value);
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

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
            list.add(getStringValue(children.get(i)) + ":" + getStringValue(children.get(i + 1)));
        }
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    private String getStringValue(Expr expr) {
        if (expr instanceof NullLiteral) {
            return "null";
        }
        if (expr instanceof StringLiteral) {
            return "\"" + expr.getStringValue() + "\"";
        }
        return expr.getStringValue();
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
            list.add(children.get(i).getStringValueForArray(options)
                    + options.getMapKeyDelim()
                    + children.get(i + 1).getStringValueForArray(options));
        }
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public String getStringValueInFe(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
            // we should use type to decide we output array is suitable for json format
            if (children.get(i).getType().isComplexType()) {
                // map key type do not support complex type
                throw new UnsupportedOperationException("Unsupported key type for MAP: " + children.get(i).getType());
            }
            list.add(getStringLiteralForComplexType(children.get(i), options)
                    + options.getMapKeyDelim() + getStringLiteralForComplexType(children.get(i + 1), options));
        }
        return "{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    protected String toSqlImpl() {
        List<String> list = new ArrayList<>(children.size());
        for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
            list.add(children.get(i).toSqlImpl() + ":" + children.get(i + 1).toSqlImpl());
        }
        return "MAP{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.MAP_LITERAL;
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        type.toThrift(container);
        msg.setType(container);
    }

    @Override
    public Expr clone() {
        return new MapLiteral(this);
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
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        children = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            children.add(Expr.readIn(in));
        }
    }

    public static MapLiteral read(DataInput in) throws IOException {
        MapLiteral literal = new MapLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(children);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MapLiteral)) {
            return false;
        }
        if (this == o) {
            return true;
        }

        MapLiteral that = (MapLiteral) o;
        return Objects.equals(children, that.children);
    }
}
