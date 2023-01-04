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
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


// INSERT INTO table_map VALUES ({'key1':1, 'key2':10, 'k3':100}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
// MapLiteral is one row-based literal
public class MapLiteral extends LiteralExpr {

    public MapLiteral() {
        type = new MapType(Type.NULL, Type.NULL);
        children = new ArrayList<>();
    }

    public MapLiteral(LiteralExpr... exprs) throws AnalysisException {
        Type keyType = Type.NULL;
        Type valueType = Type.NULL;
        children = new ArrayList<>();
        int idx = 0;
        for (LiteralExpr expr : exprs) {
            if (idx % 2 == 0) {
                if (keyType == Type.NULL) {
                    keyType = expr.getType();
                } else {
                    keyType = Type.getAssignmentCompatibleType(keyType, expr.getType(), false);
                }
                if (keyType == Type.INVALID) {
                    throw new AnalysisException("Invalid element type in Map");
                }
            } else {
                if (valueType == Type.NULL) {
                    valueType = expr.getType();
                } else {
                    valueType = Type.getAssignmentCompatibleType(valueType, expr.getType(), false);
                }
                if (valueType == Type.INVALID) {
                    throw new AnalysisException("Invalid element type in Map");
                }
            }
            children.add(expr);
            ++ idx;
        }

        type = new MapType(keyType, valueType);
    }

    protected MapLiteral(MapLiteral other) {
        super(other);
    }

    @Override
    public Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (!targetType.isMapType()) {
            return super.uncheckedCastTo(targetType);
        }
        MapLiteral literal = new MapLiteral(this);
        Type keyType = ((MapType) targetType).getKeyType();
        Type valueType = ((MapType) targetType).getValueType();

        for (int i = 0; i < children.size(); ++ i) {
            Expr child = children.get(i);
            if ((i & 1) == 0) {
                literal.children.set(i, child.uncheckedCastTo(keyType));
            } else {
                literal.children.set(i, child.uncheckedCastTo(valueType));
            }
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
    protected String toSqlImpl() {
        List<String> list = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i += 2) {
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
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(children.size());
        for (Expr e : children) {
            Expr.writeTo(e, out);
        }
    }

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.getStringValue()));
        return "MAP{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public String getStringValueForArray() {
        return null;
    }
}
