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

package org.apache.doris.mtmv.ivm.agg;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.Objects;

/**
 * Identity of one aggregate column in the normalized aggregate output, keyed by
 * {@code (function kind, first argument)}.
 *
 * <p>Used by the unified column pool during IVM aggregate normalize: visible aggregate outputs and
 * hidden state columns are registered under this key, so two targets requiring the same state
 * expression (for example {@code SUM(x)} and {@code AVG(x)} both needing {@code Count(x)}) reuse
 * the same physical column instead of creating duplicates.
 *
 * <p>{@code COUNT(*)} carries a {@code null} argument to distinguish it from {@code COUNT(expr)}.
 */
public class IvmAggColumnKey {
    private final IvmAggFunctionKind functionKind;
    private final Expression argExpr;

    private IvmAggColumnKey(IvmAggFunctionKind functionKind, Expression argExpr) {
        this.functionKind = Objects.requireNonNull(functionKind, "functionKind can not be null");
        this.argExpr = argExpr;
    }

    /** Builds a key from a function kind and its argument expression. */
    public static IvmAggColumnKey of(IvmAggFunctionKind functionKind, Expression argExpr) {
        return new IvmAggColumnKey(functionKind, argExpr);
    }

    public IvmAggFunctionKind getFunctionKind() {
        return functionKind;
    }

    public Expression getArgExpr() {
        return argExpr;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof IvmAggColumnKey)) {
            return false;
        }
        IvmAggColumnKey other = (IvmAggColumnKey) o;
        return functionKind == other.functionKind && Objects.equals(argExpr, other.argExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionKind, argExpr);
    }

    @Override
    public String toString() {
        return "IvmAggColumnKey{" + functionKind + ", " + argExpr + "}";
    }
}
