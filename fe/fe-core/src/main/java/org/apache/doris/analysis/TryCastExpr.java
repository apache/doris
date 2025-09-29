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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/CastExpr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;

public class TryCastExpr extends CastExpr {

    public TryCastExpr(Type targetType, Expr e) {
        super(targetType, e);
        opcode = TExprOpcode.TRY_CAST;
    }

    public TryCastExpr(Type targetType, Expr e, Void v) {
        super(targetType, e, v);
        opcode = TExprOpcode.TRY_CAST;
    }

    public TryCastExpr(TypeDef targetTypeDef, Expr e) {
        super(targetTypeDef, e);
        opcode = TExprOpcode.TRY_CAST;
    }

    protected TryCastExpr(TryCastExpr other) {
        super(other);
        opcode = TExprOpcode.TRY_CAST;
    }

    private static String getFnName(Type targetType) {
        return "tryCastTo" + targetType.getPrimitiveType().toString();
    }

    @Override
    public Expr clone() {
        return new TryCastExpr(this);
    }

    @Override
    public String toSqlImpl() {
        if (isAnalyzed) {
            return "TRYCAST(" + getChild(0).toSql() + " AS " + type.toSql() + ")";
        } else {
            return "TRYCAST(" + getChild(0).toSql() + " AS "
                    + (isImplicit ? type.toString() : targetTypeDef.toSql()) + ")";
        }
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        if (needExternalSql) {
            return getChild(0).toSql(disableTableName, needExternalSql, tableType, table);
        }
        if (isAnalyzed) {
            return "TRYCAST(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " AS "
                    + type.toSql() + ")";
        } else {
            return "TRYCAST(" + getChild(0).toSql(disableTableName, needExternalSql, tableType, table) + " AS "
                    + (isImplicit ? type.toString() : targetTypeDef.toSql()) + ")";
        }
    }

    @Override
    public String toDigestImpl() {
        boolean isVerbose = ConnectContext.get() != null
                && ConnectContext.get().getExecutor() != null
                && ConnectContext.get().getExecutor().getParsedStmt() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions() != null
                && ConnectContext.get().getExecutor().getParsedStmt().getExplainOptions().isVerbose();
        if (isImplicit && !isVerbose) {
            return getChild(0).toDigest();
        }
        if (isAnalyzed) {
            return "TRYCAST(" + getChild(0).toDigest() + " AS " + type.toString() + ")";
        } else {
            return "TRYCAST(" + getChild(0).toDigest() + " AS " + targetTypeDef.toString() + ")";
        }
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.TRY_CAST_EXPR;
        msg.setOpcode(opcode);
        if (type.isNativeType() && getChild(0).getType().isNativeType()) {
            msg.setChildType(getChild(0).getType().getPrimitiveType().toThrift());
        }
        originCastNullable.ifPresent(msg::setIsCastNullable);
    }

    public void analyze() throws AnalysisException {
        super.analyze();
        this.opcode = TExprOpcode.TRY_CAST;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        TryCastExpr expr = (TryCastExpr) obj;
        return this.opcode == expr.opcode;
    }

    @Override
    public boolean isNullable() {
        return true;
    }
}
