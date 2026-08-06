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

import java.util.Objects;

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

    /** constructor */
    public GeneratedColumnInfo(String exprSql, Expr expr) {
        Objects.requireNonNull(exprSql, "exprSql is null");
        this.exprSql = exprSql;
        this.expr = expr;
        this.type = GeneratedColumnType.STORED;
    }

    public String getExprSql() {
        return exprSql;
    }

    public Expr getExpr() {
        return expr;
    }
}
