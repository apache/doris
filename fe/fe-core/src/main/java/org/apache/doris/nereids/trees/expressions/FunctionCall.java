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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.analysis.FunctionParams;
import org.apache.doris.nereids.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Logical FunctionCall Expression.
 */
public class FunctionCall extends Expression {

    private FunctionName fnName;

    private FunctionParams fnParams;

    private DataType retType;

    // Used to construct output, this type may differ from above retType
    // when the intermediate type of aggregate function is not same
    // as its return type
    private DataType type;

    public FunctionCall(FunctionName functionName, FunctionParams functionParams) {
        super(NodeType.FUNCTIONCALL, functionParams.getExpression().toArray(new Expression[0]));
        this.fnName = functionName;
        this.fnParams = functionParams;
    }

    public FunctionCall(String functionName, Expression params) {
        this(new FunctionName(functionName), new FunctionParams(false, params));
    }

    public FunctionCall(String functionName, FunctionParams functionParams) {
        this(new FunctionName(functionName), functionParams);
    }

    public FunctionName getFnName() {
        return fnName;
    }

    public FunctionParams getFnParams() {
        return fnParams;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFunctionCall(this, context);
    }

    public DataType getRetType() {
        return retType;
    }

    public void setRetType(DataType retType) {
        this.retType = retType;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return super.getDataType();
    }

    public void setType(DataType type) {
        this.type = type;
    }

    @Override
    public Expression clone() {
        List<Expression> paramExprList = fnParams
                .getExpression()
                .stream()
                .map(Expression::clone)
                .collect(Collectors.toList());
       return new FunctionCall(fnName, new FunctionParams(fnParams.isDistinct(), paramExprList));
    }
}
