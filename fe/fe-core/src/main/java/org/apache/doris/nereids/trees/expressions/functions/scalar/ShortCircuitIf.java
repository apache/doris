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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysShortCircuit;

import com.google.common.base.Preconditions;

import java.util.List;

/** IF expression with statement-independent short-circuit semantics. */
public class ShortCircuitIf extends If implements AlwaysShortCircuit {
    public ShortCircuitIf(Expression condition, Expression trueValue, Expression falseValue) {
        super(condition, trueValue, falseValue);
    }

    @Override
    public ShortCircuitIf withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new ShortCircuitIf(children.get(0), children.get(1), children.get(2));
    }
}
