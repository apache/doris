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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * ScalarFunction 'json_array'.
 */
public class JsonArray extends ScalarFunction implements CustomSignature, AlwaysNotNullable {

    /**
     * constructor with 0 or more arguments.
     */
    public JsonArray(Expression... varArgs) {
        super("json_array", ExpressionUtils.mergeArguments(varArgs));
    }

    /** constructor for withChildren and reuse signature */
    private JsonArray(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> arguments = new ArrayList<>();
        for (int i = 0; i < arity(); i++) {
            arguments.add(getArgumentType(i));
        }

        return FunctionSignature.of(JsonType.INSTANCE, arguments);
    }

    /**
     * withChildren.
     */
    @Override
    public JsonArray withChildren(List<Expression> children) {
        return new JsonArray(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJsonArray(this, context);
    }
}
