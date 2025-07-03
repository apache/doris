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
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ComputeSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import java.util.List;

/**
 * The function which consume zero or more arguments in a row and product one value.
 */
public abstract class ScalarFunction extends BoundFunction implements ComputeSignature {
    public ScalarFunction(String name, Expression... arguments) {
        super(name, arguments);
    }

    public ScalarFunction(String name, List<Expression> arguments) {
        this(name, arguments, false);
    }

    public ScalarFunction(String name, List<Expression> arguments, boolean inferred) {
        super(name, arguments, inferred);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarFunction(this, context);
    }
}
