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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunctionParams;

import org.jetbrains.annotations.Nullable;

import java.util.List;

/** AggregateFunctionParams */
public class AggregateFunctionParams extends ScalarFunctionParams {
    public final boolean isDistinct;
    public final boolean isSkew;

    public AggregateFunctionParams(
            String functionName, boolean isDistinct, boolean isSkew, List<Expression> arguments) {
        this(null, functionName, isDistinct, isSkew, arguments, false);
    }

    public AggregateFunctionParams(@Nullable AggregateFunction previousFunction, String functionName,
            boolean isDistinct, boolean isSkew, List<Expression> arguments, boolean inferred) {
        super(previousFunction, functionName, arguments, inferred);
        this.isDistinct = isDistinct;
        this.isSkew = isSkew;
    }
}
