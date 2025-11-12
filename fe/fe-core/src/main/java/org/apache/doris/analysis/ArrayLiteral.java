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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import org.apache.commons.lang3.StringUtils;

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
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.toSqlImpl(disableTableName, needExternalSql, tableType, table)));

        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValue() {
        List<String> list = new ArrayList<>(children.size());
        children.forEach(v -> list.add(v.getStringValue()));
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String getStringValueForQuery(FormatOptions options) {
        List<String> list = new ArrayList<>(children.size());
        ++options.level;
        children.forEach(v -> {
            list.add(v.getStringValueInComplexTypeForQuery(options));
        });
        --options.level;
        return "[" + StringUtils.join(list, options.getCollectionDelim()) + "]";
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
    public Expr clone() {
        return new ArrayLiteral(this);
    }

    @Override
    public void checkValueValid() throws AnalysisException {
        for (Expr e : children) {
            e.checkValueValid();
        }
    }
}
