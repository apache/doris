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
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;

import java.util.Objects;

/**
 * nullable aggregate function
 */
public abstract class NullableAggregateFunction extends AggregateFunction implements
        PropagateNullable, AlwaysNullable {
    protected final boolean isAlwaysNullable;

    protected NullableAggregateFunction(String name, boolean isAlwaysNullable,
            Expression ...expressions) {
        super(name, false, expressions);
        this.isAlwaysNullable = isAlwaysNullable;
    }

    protected NullableAggregateFunction(String name, boolean isAlwaysNullable, boolean isDistinct,
            Expression ...expressions) {
        super(name, isDistinct, expressions);
        this.isAlwaysNullable = isAlwaysNullable;
    }

    @Override
    public boolean nullable() {
        return isAlwaysNullable ? AlwaysNullable.super.nullable() : PropagateNullable.super.nullable();
    }

    public abstract NullableAggregateFunction withAlwaysNullable(boolean isAlwaysNullable);

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        NullableAggregateFunction that = (NullableAggregateFunction) o;
        return isAlwaysNullable == that.isAlwaysNullable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isAlwaysNullable);
    }
}
