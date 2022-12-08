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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.types.DateTimeV2Type;

import java.util.List;

/**
 * TimeWithPrecision. fill precision to the return type.
 *
 * e.g. now(1) return datetime v2(1)
 */
public abstract class DateTimeWithPrecision extends ScalarFunction {

    public DateTimeWithPrecision(String name, Expression... arguments) {
        super(name, arguments);
    }

    public DateTimeWithPrecision(String name, List<Expression> arguments) {
        super(name, arguments);
    }

    @Override
    protected FunctionSignature computeSignature(FunctionSignature signature, List<Expression> arguments) {
        if (arity() == 1 && signature.returnType instanceof DateTimeV2Type) {
            // For functions in TIME_FUNCTIONS_WITH_PRECISION, we can't figure out which function should be use when
            // searching in FunctionSet. So we adjust the return type by hand here.
            if (arguments.get(0) instanceof IntegerLikeLiteral) {
                IntegerLikeLiteral integerLikeLiteral = (IntegerLikeLiteral) arguments.get(0);
                signature = signature.withReturnType(DateTimeV2Type.of(integerLikeLiteral.getIntValue()));
            } else {
                signature = signature.withReturnType(DateTimeV2Type.of(6));
            }
        }
        return super.computeSignature(signature, arguments);
    }
}
