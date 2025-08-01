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

package org.apache.doris.nereids.load;

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Preconditions;

/**
 * NereidsImportColumnDesc
 */
public class NereidsImportColumnDesc {
    private final String columnName;
    private final Expression expr;

    public NereidsImportColumnDesc(String column) {
        this.columnName = column;
        this.expr = null;
    }

    public NereidsImportColumnDesc(String column, Expression expr) {
        this.columnName = column;
        this.expr = expr;
    }

    public static NereidsImportColumnDesc newDeleteSignImportColumnDesc(Expression expr) {
        return new NereidsImportColumnDesc(Column.DELETE_SIGN, expr);
    }

    public NereidsImportColumnDesc withExpr(Expression expr) {
        return new NereidsImportColumnDesc(columnName, expr);
    }

    public String getColumnName() {
        return columnName;
    }

    public Expression getExpr() {
        return expr;
    }

    public boolean isColumn() {
        return expr == null;
    }

    public Expression toBinaryPredicate() {
        Preconditions.checkState(!isColumn());
        EqualTo pred = new EqualTo(new UnboundSlot(columnName), expr);
        return pred;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName);
        if (expr != null) {
            sb.append("=").append(expr.toSql());
        }
        return sb.toString();
    }
}
