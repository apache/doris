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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.IdenticalSignature;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/** parent class for first_value() and last_value() */
public abstract class FirstOrLastValue extends WindowFunction
        implements UnaryExpression, AlwaysNullable, IdenticalSignature, RequireTrivialTypes {

    static {
        List<FunctionSignature> signatures = Lists.newArrayList();
        trivialTypes.forEach(t ->
                signatures.add(FunctionSignature.ret(t).args(t))
        );
        SIGNATURES = ImmutableList.copyOf(signatures);
    }

    private static final List<FunctionSignature> SIGNATURES;

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
