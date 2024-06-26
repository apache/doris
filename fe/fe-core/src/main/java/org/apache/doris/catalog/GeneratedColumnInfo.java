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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Expr;

import com.google.gson.annotations.SerializedName;
import jline.internal.Nullable;

/**GeneratedColumnInfo*/
public class GeneratedColumnInfo {
    /**GeneratedColumnType*/
    public enum GeneratedColumnType {
        VIRTUAL,
        STORED
    }

    @SerializedName(value = "t")
    private final GeneratedColumnType type;
    @SerializedName(value = "es")
    private final String exprSql;
    @SerializedName(value = "e")
    private final Expr expr;

    /* e.g. a,b,c=a+b,d=c+1 -> a,b,c=a+b,d=a+b+1
     e.g. this is column d generated column info
     expr is c+1, expandExprForLoad is a+b+1
     expandExprForLoad is used in streamload, routineload, mysqlload, etc */
    @SerializedName(value = "efl")
    private Expr expandExprForLoad;

    /** constructor */
    public GeneratedColumnInfo(@Nullable String exprSql, Expr expr) {
        this(exprSql, expr, null);
    }

    public GeneratedColumnInfo(@Nullable String exprSql, Expr expr, Expr expandExprForLoad) {
        if (exprSql != null) {
            this.exprSql = exprSql;
        } else {
            this.exprSql = expr.toSqlWithoutTbl();
        }
        this.expr = expr;
        this.expandExprForLoad = expandExprForLoad;
        this.type = GeneratedColumnType.STORED;
    }

    public String getExprSql() {
        return exprSql;
    }

    public Expr getExpr() {
        return expr;
    }

    public Expr getExpandExprForLoad() {
        return expandExprForLoad;
    }

    public void setExpandExprForLoad(Expr expandExprForLoad) {
        this.expandExprForLoad = expandExprForLoad;
    }
}
