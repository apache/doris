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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.thrift.TExprNode;

public final class MaxLiteral extends LiteralExpr {

    public static final MaxLiteral MAX_VALUE = new MaxLiteral();

    private MaxLiteral() {
    }

    @Override
    public Expr clone() {
        return MAX_VALUE;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof MaxLiteral) {
            return 0;
        }
        return 1;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // TODO: complete this type
    }

    @Override
    public String toSqlImpl() {
        return "MAXVALUE";
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        return "MAXVALUE";
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String getStringValue() {
        return null;
    }

    @Override
    protected String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return null;
    }
}
