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
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

public class DefaultExpr extends Expr {

    public DefaultExpr(Expr child) {
        super();
        this.children.add(child);
        this.type = child.getType();
        this.nullable = true;
    }

    public DefaultExpr(DefaultExpr other) {
        super(other);
    }

    @Override
    public Expr clone() {
        return new DefaultExpr(this);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.DEFAULT_EXPR;
    }

    @Override
    public String toSqlImpl() {
        return "DEFAULT(" + getChild(0).toSql() + ")";
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        return "DEFAULT(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        return obj instanceof DefaultExpr;
    }
}
