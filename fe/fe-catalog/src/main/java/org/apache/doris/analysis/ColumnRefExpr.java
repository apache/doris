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

import com.google.gson.annotations.SerializedName;

public class ColumnRefExpr extends Expr {
    @SerializedName("cn")
    private String columnName;
    @SerializedName("ci")
    private int columnId;

    public ColumnRefExpr(boolean nullable) {
        super();
        this.nullable = nullable;
    }

    public ColumnRefExpr(ColumnRefExpr rhs) {
        super(rhs);
        this.columnId = rhs.columnId;
        this.columnName = rhs.columnName;
    }

    public String getName() {
        return columnName;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setName(String name) {
        this.columnName = name;
    }

    public void setColumnId(int id) {
        this.columnId = id;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitColumnRefExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new ColumnRefExpr(this);
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    public String debugString() {
        return columnName + " (" + columnId + ")id";
    }
}
