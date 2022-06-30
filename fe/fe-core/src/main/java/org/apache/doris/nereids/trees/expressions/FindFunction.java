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

import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Find all function call in the given expression tree.
 */
public class FindFunction extends ExpressionVisitor<Void, List<BoundFunction>> {

    private static final FindFunction FIND_FUNCTION = new FindFunction();

    public static List<BoundFunction> find(Expression expression) {
        List<BoundFunction> functionList = new ArrayList<>();
        FIND_FUNCTION.visit(expression, functionList);
        return functionList;
    }

    @Override
    public Void visit(Expression expr, List<BoundFunction> context) {
        if (expr instanceof BoundFunction) {
            context.add((BoundFunction) expr);
            return null;
        }
        for (Expression child : expr.children()) {
            child.accept(this, context);
        }
        return null;
    }

    @Override
    public Void visitBoundFunction(BoundFunction function, List<BoundFunction> context) {
        context.add(function);
        return null;
    }
}
