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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.thrift.TColumnRef;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class ColumnRefExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(ColumnRefExpr.class);
    private String columnName;
    private int columnId;
    private boolean isNullable;

    public ColumnRefExpr() {
        super();
    }

    public ColumnRefExpr(int columnId, String columnName, boolean isNullable) {
        super();
        this.columnId = columnId;
        this.columnName = columnName;
        this.isNullable = isNullable;
    }

    public ColumnRefExpr(ColumnRefExpr rhs) {
        super(rhs);
        this.columnId = rhs.columnId;
        this.columnName = rhs.columnName;
        this.isNullable = rhs.isNullable;
    }

    public String getName() {
        return columnName;
    }

    @Override
    protected String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(getName(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
    }

    public void setName(String name) {
        this.columnName = name;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int id) {
        this.columnId = id;
    }

    @Override
    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        this.isNullable = nullable;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (columnId < 0) {
            throw new AnalysisException("the columnId is invalid : " + columnId);
        }
    }

    @Override
    protected String toSqlImpl() {
        return columnName;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.COLUMN_REF;
        TColumnRef columnRef = new TColumnRef();
        columnRef.setColumnId(columnId);
        columnRef.setColumnName(columnName);
        msg.column_ref = columnRef;
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
