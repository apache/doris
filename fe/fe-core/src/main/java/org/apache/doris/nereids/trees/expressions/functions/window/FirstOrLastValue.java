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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * parent class for first_value() and last_value()
 */
public abstract class FirstOrLastValue extends WindowFunction
        implements AlwaysNullable, ExplicitlyCastableSignature {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(AnyDataType.INSTANCE_WITHOUT_INDEX),
            FunctionSignature.retArgType(0).args(AnyDataType.INSTANCE_WITHOUT_INDEX, BooleanType.INSTANCE)
    );

    public FirstOrLastValue(String name, Expression child, Expression ignoreNullValue) {
        super(name, child, ignoreNullValue);
    }

    public FirstOrLastValue(String name, Expression child) {
        super(name, child);
    }

    public FirstOrLastValue(String name, List<Expression> children) {
        super(name, children);
    }

    public FirstOrLastValue reverse() {
        if (this instanceof FirstValue) {
            return new LastValue(children);
        } else {
            return new FirstValue(children);
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    /**check the second parameter must be true or false*/
    public static void checkSecondParameter(FirstOrLastValue firstOrLastValue) {
        if (1 == firstOrLastValue.arity()) {
            return;
        }
        if (!BooleanLiteral.TRUE.equals(firstOrLastValue.child(1))
                && !BooleanLiteral.FALSE.equals(firstOrLastValue.child(1))) {
            throw new AnalysisException("The second parameter of " + firstOrLastValue.getName()
                    + " must be a constant or a constant expression, and the result of "
                    + "the calculated constant or constant expression must be true or false.");
        }
    }
}
