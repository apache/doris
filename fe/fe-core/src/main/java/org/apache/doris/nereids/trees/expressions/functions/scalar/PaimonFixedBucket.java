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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** Internal Paimon default bucket function used only for external writer routing. */
public final class PaimonFixedBucket extends ScalarFunction
        implements CustomSignature, AlwaysNotNullable {

    public PaimonFixedBucket(int numBuckets, List<? extends Expression> fields) {
        super("__paimon_fixed_bucket_v1", arguments(numBuckets, fields));
        Preconditions.checkArgument(numBuckets > 0, "Paimon bucket count must be positive");
        Preconditions.checkArgument(!fields.isEmpty(), "Paimon bucket fields must not be empty");
    }

    private PaimonFixedBucket(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    private static List<Expression> arguments(
            int numBuckets, List<? extends Expression> fields) {
        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        arguments.add(new IntegerLiteral(numBuckets));
        arguments.addAll(fields);
        return arguments.build();
    }

    @Override
    public PaimonFixedBucket withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() > 1,
                "Paimon bucket requires a count and at least one field");
        return new PaimonFixedBucket(getFunctionParams(children));
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
