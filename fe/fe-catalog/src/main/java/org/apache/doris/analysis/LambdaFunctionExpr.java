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

import org.apache.doris.catalog.Type;

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

    private LambdaFunctionExpr() {
        // use for serde only
    }

    // for Nereids
    public LambdaFunctionExpr(Expr lambdaBody, List<String> argNames, List<Expr> slotExprs, boolean nullable) {
        this.slotExprs.add(lambdaBody);
        this.slotExprs.addAll(slotExprs);
        this.names.addAll(argNames);
        this.params.addAll(slotExprs);
        this.children.add(lambdaBody);
        this.setType(Type.LAMBDA_FUNCTION);
        this.nullable = nullable;
    }

    public LambdaFunctionExpr(LambdaFunctionExpr rhs) {
        super(rhs);
        this.names.addAll(rhs.names);
        this.slotExprs.addAll(rhs.slotExprs);
        this.params.addAll(rhs.params);
    }

    public ArrayList<String> getNames() {
        return names;
    }

    public ArrayList<Expr> getSlotExprs() {
        return slotExprs;
    }

    public ArrayList<Expr> getParams() {
        return params;
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitLambdaFunctionExpr(this, context);
    }

    @Override
    public Expr clone() {
        return new LambdaFunctionExpr(this);
    }
}
