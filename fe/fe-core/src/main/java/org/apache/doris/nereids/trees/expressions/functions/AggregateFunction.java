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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import java.util.Objects;

/**
 * The function which consume arguments in lots of rows and product one value.
 */
public abstract class AggregateFunction extends BoundFunction {

    private DataType intermediate;
    private final boolean isDistinct;

    public AggregateFunction(String name, Expression... arguments) {
        super(name, arguments);
        isDistinct = false;
    }

    public AggregateFunction(String name, boolean isDistinct, Expression... arguments) {
        super(name, arguments);
        this.isDistinct = isDistinct;
    }

    public abstract DataType getIntermediateType();

    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AggregateFunction that = (AggregateFunction) o;
        return Objects.equals(isDistinct, that.isDistinct) && Objects.equals(intermediate, that.intermediate)
                && Objects.equals(getName(), that.getName()) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isDistinct, intermediate, getName(), children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAggregateFunction(this, context);
    }

    public DataType getIntermediate() {
        return intermediate;
    }

    public void setIntermediate(DataType intermediate) {
        this.intermediate = intermediate;
    }
}
