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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * nullable aggregate function
 */
public abstract class NullableAggregateFunction extends AggregateFunction implements
        PropagateNullable, AlwaysNullable {

    protected final boolean alwaysNullable;

    protected NullableAggregateFunction(String name, boolean distinct, boolean alwaysNullable,
            Expression ...expressions) {
        this(name, distinct, alwaysNullable, false, Arrays.asList(expressions));
    }

    protected NullableAggregateFunction(String name, boolean distinct, boolean alwaysNullable,
            List<Expression> expressions) {
        this(name, distinct, alwaysNullable, false, expressions);
    }

    protected NullableAggregateFunction(String name, boolean distinct, boolean alwaysNullable, boolean isSkew,
            Expression ...expressions) {
        this(name, distinct, alwaysNullable, isSkew, Arrays.asList(expressions));
    }

    protected NullableAggregateFunction(String name, boolean distinct, boolean alwaysNullable, boolean isSkew,
            List<Expression> expressions) {
        super(name, distinct, isSkew, expressions);
        this.alwaysNullable = alwaysNullable;
    }

    /** constructor for withChildren and reuse signature */
    protected NullableAggregateFunction(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
        this.alwaysNullable = functionParams.alwaysNullable;
    }

    @Override
    public boolean nullable() {
        return alwaysNullable ? AlwaysNullable.super.nullable() : PropagateNullable.super.nullable();
    }

    @Override
    public NullableAggregateFunctionParams getFunctionParams(List<Expression> arguments) {
        return new NullableAggregateFunctionParams(
                this, getName(), isDistinct(), isSkew(), alwaysNullable, arguments, isInferred()
        );
    }

    @Override
    public NullableAggregateFunctionParams getFunctionParams(boolean isDistinct, List<Expression> arguments) {
        return new NullableAggregateFunctionParams(
                this, getName(), isDistinct, isSkew(), alwaysNullable, arguments, isInferred()
        );
    }

    @Override
    public NullableAggregateFunctionParams getFunctionParams(
            boolean isDistinct, boolean isSkew, List<Expression> arguments) {
        return new NullableAggregateFunctionParams(
                this, getName(), isDistinct, isSkew, alwaysNullable, arguments, isInferred()
        );
    }

    public NullableAggregateFunctionParams getAlwaysNullableFunctionParams(boolean alwaysNullable) {
        return new NullableAggregateFunctionParams(
                this, getName(), isDistinct(), isSkew(), alwaysNullable, children, isInferred()
        );
    }

    public boolean isAlwaysNullable() {
        return alwaysNullable;
    }

    public abstract NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable);

    /**
     * equalsIgnoreNullable
     */
    public boolean equalsIgnoreNullable(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return super.equals(o);
    }

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
        return alwaysNullable == that.alwaysNullable;
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), alwaysNullable);
    }

}
