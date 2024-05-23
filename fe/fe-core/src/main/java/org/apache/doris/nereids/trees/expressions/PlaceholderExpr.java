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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

/**
 * Placeholder for prepared statement
 */
public class PlaceholderExpr extends Expression {
    private final int exprId;
    private Expression expr;

    private int mysqlTypeCode;

    public PlaceholderExpr(int exprId) {
        this.exprId = exprId;
    }

    protected PlaceholderExpr(int exprId, Expression expr) {
        this.exprId = exprId;
        this.expr = expr;
    }

    public Expression getExpr() {
        return expr;
    }

    public int getExprId() {
        return exprId;
    }

    public void setExpr(Expression expr) {
        this.expr = expr;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPlaceholderExpr(this, context);
    }

    public void setTypeCode(int mysqlTypeCode) {
        this.mysqlTypeCode = mysqlTypeCode;
    }

    public int getMysqlTypeCode() {
        return mysqlTypeCode;
    }

    @Override
    public boolean nullable() {
        return expr.nullable();
    }

    @Override
    public String toSql() {
        return "?";
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return expr.getDataType();
    }
}
