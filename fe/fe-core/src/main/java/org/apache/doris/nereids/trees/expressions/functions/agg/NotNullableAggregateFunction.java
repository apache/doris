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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;

import java.util.List;

/**
 * base class for AlwaysNotNullable aggregate function
 */
public abstract class NotNullableAggregateFunction extends AggregateFunction implements AlwaysNotNullable {
    protected NotNullableAggregateFunction(String name, Expression ...expressions) {
        super(name, false, expressions);
    }

    protected NotNullableAggregateFunction(String name, List<Expression> expressions) {
        super(name, false, expressions);
    }

    protected NotNullableAggregateFunction(String name, boolean distinct, Expression ...expressions) {
        super(name, distinct, expressions);
    }

    protected NotNullableAggregateFunction(String name, boolean distinct, List<Expression> expressions) {
        super(name, distinct, expressions);
    }

    // return value of this function if the input data is empty.
    // for example, count(*) of empty table is 0;
    public abstract Expression resultForEmptyInput();
}
