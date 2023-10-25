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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** parent class for first_value() and last_value() */
public abstract class FirstOrLastValue extends WindowFunction
        implements UnaryExpression, PropagateNullable, ExplicitlyCastableSignature {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.retArgType(0).args(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    public FirstOrLastValue(String name, Expression child) {
        super(name, child);
    }

    public FirstOrLastValue reverse() {
        if (this instanceof FirstValue) {
            return new LastValue(child());
        } else {
            return new FirstValue(child());
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
