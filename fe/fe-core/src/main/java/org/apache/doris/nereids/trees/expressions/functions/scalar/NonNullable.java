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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * change nullable input col to non_nullable col
 */
public class NonNullable extends ScalarFunction implements UnaryExpression, CustomSignature, AlwaysNotNullable {

    public NonNullable(Expression expr) {
        super("non_nullable", expr);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType dataType = getArgument(0).getDataType();
        return FunctionSignature.ret(dataType).args(dataType);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1,
                "the child expression of NonNullable should be only one");
        return new NonNullable(children.get(0));
    }
}
