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
import org.apache.doris.thrift.TExprNode;

import com.google.common.base.Preconditions;

public class AliasArgument extends Expr {

    private final String alias;

    private Expr expr;

    public AliasArgument(String alias, Expr expr) {
        Preconditions.checkNotNull(expr);
        this.alias = alias;
        this.expr = expr;
    }

    public AliasArgument(AliasArgument other) {
        this.alias = other.alias;
        this.expr = other.expr;
    }

    public String getAlias() {
        return alias;
    }

    public Expr getExpr() {
        return expr;
    }


    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        // require no analysis.
    }

    @Override
    protected String toSqlImpl() {
        return null; // currently no used
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // no operation
    }

    @Override
    public Expr clone() {
        return new AliasArgument(alias, expr);
    }
}
