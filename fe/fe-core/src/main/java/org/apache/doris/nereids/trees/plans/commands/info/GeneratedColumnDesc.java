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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.GeneratedColumnInfo;
import org.apache.doris.nereids.trees.expressions.Expression;

/**GeneratedColumnDesc for nereids*/
public class GeneratedColumnDesc {
    /**GeneratedColumnType*/
    public enum GeneratedColumnType {
        VIRTUAL,
        STORED
    }

    private final GeneratedColumnType type;
    private final String exprSql;
    private Expr expr;
    private Expr expandExprForLoad;
    private final Expression expression;

    /** constructor */
    public GeneratedColumnDesc(String exprSql, Expression expression) {
        this.exprSql = exprSql;
        this.expression = expression;
        this.type = GeneratedColumnType.STORED;
    }

    public Expr getExpr() {
        return expr;
    }

    public void setExpr(Expr expr) {
        this.expr = expr;
    }

    public void setExpandExprForLoad(Expr expandExprForLoad) {
        this.expandExprForLoad = expandExprForLoad;
    }

    public Expression getExpression() {
        return expression;
    }

    public GeneratedColumnInfo translateToInfo() {
        return new GeneratedColumnInfo(exprSql, expr, expandExprForLoad);
    }
}
