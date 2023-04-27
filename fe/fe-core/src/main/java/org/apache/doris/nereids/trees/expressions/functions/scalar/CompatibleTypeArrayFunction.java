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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Joiner;

import java.util.List;

/** CompatibleTypeArrayFunction */
public abstract class CompatibleTypeArrayFunction extends ScalarFunction
        implements ExplicitlyCastableSignature {

    public CompatibleTypeArrayFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public CompatibleTypeArrayFunction(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        Type compatibleType = getArgumentType(0).toCatalogDataType();
        for (int i = 1; i < arity(); ++i) {
            compatibleType = Type.getAssignmentCompatibleType(
                    compatibleType, getArgumentType(i).toCatalogDataType(), true);
            if (compatibleType == Type.INVALID) {
                throw new AnalysisException(String.format(
                        "No matching function with signature: %s(%s).",
                        getName(), Joiner.on(", ").join(getArguments())));
            }
        }
        // Make sure BE doesn't see any TYPE_NULL exprs
        if (compatibleType.isNull()) {
            compatibleType = Type.BOOLEAN;
        }

        // do implicit type cast
        DataType compatibleDataType = DataType.fromCatalogType(compatibleType);
        signature = signature.withArgumentTypes(getArguments(), (sigType, argument) -> compatibleDataType);
        return super.computeSignature(signature);
    }
}
