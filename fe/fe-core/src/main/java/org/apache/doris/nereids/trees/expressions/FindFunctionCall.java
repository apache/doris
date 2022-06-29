// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.expressions;

import java.util.ArrayList;
import java.util.List;

/**
 * Find all function call in the given expression tree.
 */
public class FindFunctionCall extends ExpressionVisitor<Void, List<FunctionCall>> {

    private static final FindFunctionCall functionCall = new FindFunctionCall();

    public static List<FunctionCall> find(Expression expression) {
        List<FunctionCall> functionCallList = new ArrayList<>();
        functionCall.visit(expression, functionCallList);
        return functionCallList;
    }

    @Override
    public Void visit(Expression expr, List<FunctionCall> context) {
        if (expr instanceof FunctionCall) {
            context.add((FunctionCall) expr);
            return null;
        }
        for (Expression child : expr.children()) {
            child.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitFunctionCall(FunctionCall function, List<FunctionCall> context) {
        context.add(function);
        return null;
    }
}
