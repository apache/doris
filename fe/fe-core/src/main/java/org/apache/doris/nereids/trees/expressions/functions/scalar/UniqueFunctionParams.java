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

import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;

import org.jetbrains.annotations.Nullable;

import java.util.List;

/** UniqueFunctionParams */
public class UniqueFunctionParams extends ScalarFunctionParams {

    public final ExprId uniqueId;
    public final boolean ignoreUniqueId;

    public UniqueFunctionParams(String functionName, ExprId uniqueId, boolean ignoreUniqueId,
            List<Expression> arguments) {
        this(null, functionName, uniqueId, ignoreUniqueId, arguments, false);
    }

    public UniqueFunctionParams(@Nullable UniqueFunction originFunction, String functionName, ExprId uniqueId,
            boolean ignoreUniqueId, List<Expression> arguments, boolean inferred) {
        super(originFunction, functionName, arguments, inferred);
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }
}
