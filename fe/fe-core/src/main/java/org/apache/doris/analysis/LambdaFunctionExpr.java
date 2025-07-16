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
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public class LambdaFunctionExpr extends Expr {
    @SerializedName("ns")
    private ArrayList<String> names = new ArrayList<>();
    @SerializedName("ses")
    private ArrayList<Expr> slotExprs = new ArrayList<>();
    @SerializedName("ps")
    private ArrayList<Expr> params = new ArrayList<>();
    private int columnId = 0;

    private LambdaFunctionExpr() {
        // use for serde only
    }

    public LambdaFunctionExpr(Expr e, String arg, List<Expr> params) {
        this.names.add(arg);
        this.slotExprs.add(e);
        this.params.addAll(params);
        columnId = 0;
        this.setType(Type.LAMBDA_FUNCTION);
    }

    public LambdaFunctionExpr(Expr e, ArrayList<String> args, List<Expr> params) {
        this.names.addAll(args);
        this.slotExprs.add(e);
        this.params.addAll(params);
        columnId = 0;
        this.setType(Type.LAMBDA_FUNCTION);
    }

    // for Nereids
    public LambdaFunctionExpr(Expr lambdaBody, List<String> argNames, List<Expr> slotExprs) {
        this.slotExprs.add(lambdaBody);
        this.slotExprs.addAll(slotExprs);
        this.names.addAll(argNames);
        this.params.addAll(slotExprs);
        this.children.add(lambdaBody);
        this.setType(Type.LAMBDA_FUNCTION);
    }

    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
        this.names.addAll(rhs.names);
        this.slotExprs.addAll(rhs.slotExprs);
        this.params.addAll(rhs.params);
        this.columnId = rhs.columnId;
    }

    @Override
    protected String toSqlImpl() {
        String nameStr = "";
        Expr lambdaExpr = slotExprs.get(0);
        int exprSize = names.size();
        for (int i = 0; i < exprSize; ++i) {
            nameStr = nameStr + names.get(i);
            if (i != exprSize - 1) {
                nameStr = nameStr + ",";
            }
        }
        if (exprSize > 1) {
            nameStr = "(" + nameStr + ")";
        }
        String res = String.format("%s -> %s", nameStr, lambdaExpr.toSql());
        return res;
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        String nameStr = "";
        Expr lambdaExpr = slotExprs.get(0);
        int exprSize = names.size();
        for (int i = 0; i < exprSize; ++i) {
            nameStr = nameStr + names.get(i);
            if (i != exprSize - 1) {
                nameStr = nameStr + ",";
            }
        }
        if (exprSize > 1) {
            nameStr = "(" + nameStr + ")";
        }
        String res = String.format("%s -> %s", nameStr,
                lambdaExpr.toSql(disableTableName, needExternalSql, tableType, table));
        return res;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNodeType(TExprNodeType.LAMBDA_FUNCTION_EXPR);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    @Override
    public boolean isNullable() {
        for (int i = 1; i < slotExprs.size(); ++i) {
            if (slotExprs.get(i).isNullable()) {
                return true;
            }
        }
        return false;
    }
}
