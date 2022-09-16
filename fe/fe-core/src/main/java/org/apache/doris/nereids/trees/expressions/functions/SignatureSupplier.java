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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FuncSig;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * you can implement this interface to simply compute dataType and expectedInputTypes by the compatibility signature.
 */
@Developing
public interface SignatureSupplier extends ImplicitCastInputTypes {
    String getName();

    int arity();

    List<FuncSig> getSignatures();

    List<Expression> getArguments();

    @Override
    default List<AbstractDataType> expectedInputTypes() {
        int arity = arity();
        List<FuncSig> candidateFunctions = getSignatures()
                .stream()
                .filter(s -> (s.hasVarArgs && arity >= s.argumentsTypes.size())
                        || (!s.hasVarArgs && s.argumentsTypes.size() == arity))
                .collect(Collectors.toList());
        List<Expression> arguments = getArguments();
        if (candidateFunctions.isEmpty()) {
            throwCanNotFoundFunctionException(getName(), arguments);
        }

        for (FuncSig candidateFunction : candidateFunctions) {
            if (isAssignableFrom(candidateFunction.hasVarArgs, candidateFunction.argumentsTypes,
                    candidateFunction.argumentsTypes)) {
                return candidateFunction.argumentsTypes;
            }
        }
        throwCanNotFoundFunctionException(getName(), arguments);
        // never reach
        return null;
    }

    /**
     * find function's return data type by the signature.
     * @return DataType
     */
    default DataType getDataType() {
        List<Expression> arguments = getArguments();
        int arity = arguments.size();

        for (FuncSig sig : getSignatures()) {
            // check arity
            if (sig.hasVarArgs && sig.argumentsTypes.size() < arity) {
                continue;
            } else if (!sig.hasVarArgs && sig.argumentsTypes.size() != arity) {
                continue;
            }

            // check types
            List<AbstractDataType> argTypes = arguments.stream()
                    .map(Expression::getDataType)
                    .collect(Collectors.toList());
            if (isAssignableFrom(sig.hasVarArgs, sig.argumentsTypes, argTypes)) {
                return sig.returnType;
            }
        }
        throwCanNotFoundFunctionException(getName(), arguments);
        // never reach
        return null;
    }

    /**
     * Check whether argument's can assign to the expected types. Currently, we just check by the
     * dataType.equals(), should we support more complex type compatibility?
     *
     * @param isVarArgs is var args
     * @param expectTypes the arguments type of function
     * @param dataTypes the argument's data types
     * @return true if argument's can assign to the expected types
     */
    static boolean isAssignableFrom(boolean isVarArgs, List<AbstractDataType> expectTypes,
            List<AbstractDataType> dataTypes) {
        for (int i = 0; i < dataTypes.size(); i++) {
            AbstractDataType expectType = (isVarArgs && i >= expectTypes.size())
                    ? expectTypes.get(expectTypes.size() - 1)
                    : expectTypes.get(i);

            AbstractDataType argumentType = dataTypes.get(i);
            if (!expectType.isAssignableFrom(argumentType)) {
                return false;
            }
        }
        return true;
    }

    static void throwCanNotFoundFunctionException(String name, List<Expression> arguments) {
        String missingSignature = name + arguments.stream()
                .map(Expression::getDataType)
                .map(DataType::toSql)
                .collect(Collectors.joining(", ", "(", ")"));
        throw new AnalysisException("Can not find the compatibility function signature: " + missingSignature);
    }
}
