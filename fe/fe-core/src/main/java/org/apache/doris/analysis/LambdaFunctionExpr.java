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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class LambdaFunctionExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(LambdaFunctionExpr.class);
    private ArrayList<String> names = new ArrayList<>();
    private ArrayList<Expr> slotExpr = new ArrayList<>();
    private ArrayList<Expr> params = new ArrayList<>();
    private int columnId = 0;

    public LambdaFunctionExpr(Expr e, String arg, List<Expr> params) {
        this.names.add(arg);
        this.slotExpr.add(e);
        this.params.addAll(params);
        columnId = 0;
        this.setType(Type.LAMBDA_FUNCTION);
    }

    public LambdaFunctionExpr(Expr e, ArrayList<String> args, List<Expr> params) {
        this.names.addAll(args);
        this.slotExpr.add(e);
        this.params.addAll(params);
        columnId = 0;
        this.setType(Type.LAMBDA_FUNCTION);
    }

    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
        this.names.addAll(rhs.names);
        this.slotExpr.addAll(rhs.slotExpr);
        this.params.addAll(rhs.params);
        this.columnId = rhs.columnId;
    }

    @Override
    protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (names.size() != params.size()) {
            throw new AnalysisException("Lambda argument size: is " + names.size() + " but input params size is "
                    + params.size());
        }
        if (this.children.size() == 0) {
            this.children.add(slotExpr.get(0));
        }
        HashSet<String> nameSet = new HashSet<>();
        // the first is lambda
        int size = slotExpr.size();
        for (int i = size - 1; i < names.size(); ++i) {
            if (nameSet.contains(names.get(i))) {
                throw new AnalysisException(
                        "The lambda function of params " + names.get(i) + " has already been repeated, "
                                + "you should give a unique name for every param.");
            } else {
                nameSet.add(names.get(i));
            }
            Expr param = params.get(i);
            if (!param.isAnalyzed()) {
                param.analyze(analyzer);
            }
            Type paramType = param.getType();
            if (!paramType.isArrayType()) {
                throw new AnalysisException(
                        "The lambda function of params must be array type, now the param of "
                                + param.toColumnLabel() + " is " + paramType.toString());
            }
            // this ColumnRefExpr record the unique columnId, which is used for BE
            // so could insert nested column by order.
            ColumnRefExpr column = new ColumnRefExpr();
            column.setName(names.get(i));
            column.setColumnId(columnId);
            column.setNullable(true);
            column.setType(((ArrayType) paramType).getItemType());
            columnId = columnId + 1;
            replaceExpr(names.get(i), column, slotExpr);
        }
        if (slotExpr.size() != params.size() + 1) {
            String msg = new String();
            for (Expr s : slotExpr) {
                msg = msg + s.debugString() + " ,";
            }
            throw new AnalysisException(
                    "Lambda columnref size: is " + (slotExpr.size() - 1) + " but input params size is "
                            + params.size() + ". the replaceExpr of columnref is " + msg);
        }
        this.children.get(0).analyze(analyzer);
    }

    @Override
    protected String toSqlImpl() {
        return String.format("%s -> %s", names.toString(), getChild(0).toSql());
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.setNodeType(TExprNodeType.LAMBDA_FUNCTION_EXPR);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }

    public ArrayList<String> getNames() {
        return names;
    }

    public ArrayList<Expr> getSlotExprs() {
        return slotExpr;
    }

    public boolean isNullable() {
        for (int i = 1; i < slotExpr.size(); ++i) {
            if (slotExpr.get(i).isNullable()) {
                return true;
            }
        }
        return false;
    }
}
