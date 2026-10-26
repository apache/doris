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
import org.apache.doris.nereids.exceptions.AnalysisException;
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
        return spatialArguments(returnType, left, right);
    }

    static FunctionSignature ternary(Expression first, Expression second, Expression third, DataType returnType) {
        return spatialArguments(returnType, first, second, third);
    }

    private static FunctionSignature spatialArguments(DataType returnType, Expression... arguments) {
        DataType inferredType = null;
        for (Expression argument : arguments) {
            if (isSpatial(argument)) {
                inferredType = argument.getDataType();
                break;
            }
        }
        if (inferredType == null) {
            return null;
        }
        DataType[] inputTypes = new DataType[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            Expression argument = arguments[i];
            if (isSpatial(argument)) {
                inputTypes[i] = argument.getDataType();
            } else if (argument.getDataType().isNullType()) {
                inputTypes[i] = inferredType;
            } else {
                return null;
            }
        }
        return FunctionSignature.ret(returnType).args(inputTypes);
    }

    static void checkSpatialArguments(String function, Expression left, Expression right) {
        if ((!isSpatial(left) && !left.getDataType().isNullType())
                || (!isSpatial(right) && !right.getDataType().isNullType())) {
            throw new AnalysisException(function + " requires typed spatial arguments");
        }
    }

    private static boolean isSpatial(Expression expression) {
        return expression.getDataType() instanceof GeometryType
                || expression.getDataType() instanceof GeographyType;
    }
}
