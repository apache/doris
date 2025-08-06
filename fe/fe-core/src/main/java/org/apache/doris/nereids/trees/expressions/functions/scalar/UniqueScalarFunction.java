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

import java.util.List;

/**
 * unique had a unique id, and two equals only if they have the same unique id.
 *
 * e.g. random(), uuid(), etc.
 */
public abstract class UniqueScalarFunction extends ScalarFunction {

    protected final ExprId uniqueId;

    // when compare and bind unique id with group by expressions, should ignore the unique id
    protected final boolean ignoreUniqueId;

    public UniqueScalarFunction(String name, ExprId uniqueId, boolean ignoreUniqueId, Expression... arguments) {
        super(name, arguments);
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }

    public UniqueScalarFunction(String name, ExprId uniqueId, boolean ignoreUniqueId, List<Expression> arguments) {
        super(name, arguments);
        this.uniqueId = uniqueId;
        this.ignoreUniqueId = ignoreUniqueId;
    }

    public abstract UniqueScalarFunction withIgnoreUniqueId(boolean ignoreUniqueId);

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UniqueScalarFunction other = (UniqueScalarFunction) o;
        if (ignoreUniqueId || other.ignoreUniqueId) {
            return super.equals(other);
        }
        return uniqueId.equals(other.uniqueId);
    }

    // The contains method needs to use hashCode, so similar to equals, it only compares exprId
    @Override
    public int computeHashCode() {
        // direct return exprId to speed up
        if (ignoreUniqueId) {
            return super.computeHashCode();
        }
        return uniqueId.asInt();
    }
}
