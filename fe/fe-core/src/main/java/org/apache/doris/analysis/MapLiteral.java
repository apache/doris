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

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

// INSERT INTO table_map VALUES ({'key1':1, 'key2':10, 'k3':100}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
// MapLiteral is one row-based literal
public class MapLiteral extends LiteralExpr {

    public MapLiteral() {
        type = new MapType(Type.NULL, Type.NULL);
        children = new ArrayList<>();
        this.nullable = false;
    }

    public MapLiteral(Type type, List<LiteralExpr> keys, List<LiteralExpr> values) {
        this.type = type;
        children = Lists.newArrayList();
        for (int i = 0; i < keys.size(); i++) {
            children.add(keys.get(i));
            children.add(values.get(i));
        }
        this.nullable = false;
    }

    protected MapLiteral(MapLiteral other) {
        super(other);
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
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitMapLiteral(this, context);
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
