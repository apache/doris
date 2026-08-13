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
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Internal Paimon BinaryRow hash used only for external writer routing. */
public final class PaimonBinaryRowHash extends ScalarFunction
        implements CustomSignature, AlwaysNotNullable {

    public PaimonBinaryRowHash(List<? extends Expression> fields) {
        super("__paimon_binary_row_hash_v1", ImmutableList.copyOf(fields));
        Preconditions.checkArgument(!fields.isEmpty(), "Paimon hash fields must not be empty");
    }

    private PaimonBinaryRowHash(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public PaimonBinaryRowHash withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty(), "Paimon hash fields must not be empty");
        return new PaimonBinaryRowHash(getFunctionParams(children));
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.ret(IntegerType.INSTANCE)
                .args(children.stream()
                        .map(Expression::getDataType)
                        .toArray(DataType[]::new));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
