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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.GeographyType;
import org.apache.doris.nereids.types.GeometryType;

/** Helpers for preserving parameterized spatial types in function signatures. */
final class SpatialFunctionSignature {
    private SpatialFunctionSignature() {
    }

    static FunctionSignature unary(Expression argument, DataType returnType) {
        if (!isSpatial(argument)) {
            return null;
        }
        return FunctionSignature.ret(returnType).args(argument.getDataType());
    }

    static FunctionSignature binary(Expression left, Expression right, DataType returnType) {
        if (!isSpatial(left) || !isSpatial(right)) {
            return null;
        }
        return FunctionSignature.ret(returnType).args(left.getDataType(), right.getDataType());
    }

    private static boolean isSpatial(Expression expression) {
        return expression.getDataType() instanceof GeometryType
                || expression.getDataType() instanceof GeographyType;
    }
}
