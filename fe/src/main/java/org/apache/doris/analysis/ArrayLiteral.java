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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

public class ArrayLiteral extends LiteralExpr {

//    private List<LiteralExpr> values = new LinkedList<>();

    public ArrayLiteral() {
        children = new ArrayList<>();
    }

    public ArrayLiteral(LiteralExpr... v) {
        if (v.length < 1) {
            this.type = new ArrayType(Type.NULL);
            return;
        }

        this.type = new ArrayType(v[0].type);
        children = new ArrayList<>(v.length);
        children.addAll(Arrays.asList(v));
    }

    protected ArrayLiteral(ArrayLiteral other) {
        super(other);
        this.type = other.type;
        this.children.addAll(other.children);
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
    protected String toSqlImpl() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toSqlImpl()));

        return "array(" + StringUtils.join(list, ", ") + ")";
    }

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(((LiteralExpr) v).getStringValue()));

        return "array[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    protected void treeToThriftHelper(TExpr container) {
        super.treeToThriftHelper(container);

//        for (LiteralExpr expr: values) {
//            expr.treeToThriftHelper(container);
//        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.ARRAY_LITERAL;
//        msg.num_children = values.size();
        msg.setChild_type(((ArrayType) type).getItemType().getPrimitiveType().toThrift());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }

    @Override
    public Expr clone() {
        return new ArrayLiteral(this);
    }
}
