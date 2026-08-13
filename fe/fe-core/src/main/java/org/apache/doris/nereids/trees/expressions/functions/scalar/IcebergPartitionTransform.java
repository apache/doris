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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.base.Preconditions;

import java.util.List;

/** Internal Iceberg partition transform used only to produce hidden writer-routing slots. */
public final class IcebergPartitionTransform extends ScalarFunction
        implements CustomSignature, PropagateNullable {

    /** Stateless Iceberg transforms whose results can be evaluated by the BE. */
    public enum Transform {
        YEAR("__iceberg_transform_year", false),
        MONTH("__iceberg_transform_month", false),
        DAY("__iceberg_transform_day", false),
        HOUR("__iceberg_transform_hour", false),
        BUCKET("__iceberg_transform_bucket", true),
        TRUNCATE("__iceberg_transform_truncate", true);

        private final String functionName;
        private final boolean parameterized;

        Transform(String functionName, boolean parameterized) {
            this.functionName = functionName;
            this.parameterized = parameterized;
        }
    }

    private final Transform transform;

    /** Create an unparameterized transform. */
    public IcebergPartitionTransform(Transform transform, Expression source) {
        super(transform.functionName, source);
        Preconditions.checkArgument(!transform.parameterized,
                "%s transform requires a width", transform);
        this.transform = transform;
    }

    /** Create a bucket or truncate transform. */
    public IcebergPartitionTransform(Transform transform, Expression source, int width) {
        super(transform.functionName, source, new IntegerLiteral(width));
        Preconditions.checkArgument(transform.parameterized,
                "%s transform does not accept a width", transform);
        Preconditions.checkArgument(width > 0, "Iceberg transform width must be positive");
        this.transform = transform;
    }

    private IcebergPartitionTransform(Transform transform, ScalarFunctionParams functionParams) {
        super(functionParams);
        this.transform = transform;
    }

    public Transform getTransform() {
        return transform;
    }

    @Override
    public IcebergPartitionTransform withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == (transform.parameterized ? 2 : 1));
        return new IcebergPartitionTransform(transform, getFunctionParams(children));
    }

    @Override
    public FunctionSignature customSignature() {
        DataType sourceType = getArgument(0).getDataType();
        DataType returnType = transform == Transform.TRUNCATE
                ? sourceType
                : IntegerType.INSTANCE;
        if (transform.parameterized) {
            return FunctionSignature.ret(returnType).args(sourceType, IntegerType.INSTANCE);
        }
        return FunctionSignature.ret(returnType).args(sourceType);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
