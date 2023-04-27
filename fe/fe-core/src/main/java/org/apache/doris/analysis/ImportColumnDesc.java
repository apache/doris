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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;

import com.google.common.base.Preconditions;

public class ImportColumnDesc {
    private String columnName;
    private Expr expr;

    public ImportColumnDesc(ImportColumnDesc other) {
        this.columnName = other.columnName;
        if (other.expr != null) {
            this.expr = other.expr.clone();
        }
    }

    public ImportColumnDesc(String column) {
        this.columnName = column;
    }

    public ImportColumnDesc(String column, Expr expr) {
        this.columnName = column;
        this.expr = expr;
    }

    public static ImportColumnDesc newDeleteSignImportColumnDesc(Expr expr) {
        return new ImportColumnDesc(Column.DELETE_SIGN, expr);
    }

    public String getColumnName() {
        return columnName;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    public boolean isColumn() {
        return expr == null;
    }

    public Expr toBinaryPredicate() {
        Preconditions.checkState(!isColumn());
        BinaryPredicate pred = new BinaryPredicate(Operator.EQ, new SlotRef(null, columnName), expr);
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
