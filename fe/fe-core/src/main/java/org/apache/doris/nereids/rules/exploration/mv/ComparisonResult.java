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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * comparison result of view and query
 */
public class ComparisonResult {
    public static final ComparisonResult INVALID =
            new ComparisonResult(ImmutableList.of(), ImmutableList.of(), ImmutableSet.of(), false);
    private final boolean valid;
    private final List<Expression> viewExpressions;
    private final List<Expression> queryExpressions;
    private final Set<Set<Slot>> viewNoNullableSlot;

    public ComparisonResult(List<Expression> queryExpressions, List<Expression> viewExpressions,
            Set<Set<Slot>> viewNoNullableSlot) {
        this(queryExpressions, viewExpressions, viewNoNullableSlot, true);
    }

    ComparisonResult(List<Expression> queryExpressions, List<Expression> viewExpressions,
            Set<Set<Slot>> viewNoNullableSlot, boolean valid) {
        this.viewExpressions = ImmutableList.copyOf(viewExpressions);
        this.queryExpressions = ImmutableList.copyOf(queryExpressions);
        this.viewNoNullableSlot = ImmutableSet.copyOf(viewNoNullableSlot);
        this.valid = valid;
    }

    public List<Expression> getViewExpressions() {
        return viewExpressions;
    }

    public List<Expression> getQueryExpressions() {
        return queryExpressions;
    }

    public Set<Set<Slot>> getViewNoNullableSlot() {
        return viewNoNullableSlot;
    }

    public boolean isInvalid() {
        return !valid;
    }

    /**
     * Builder
     */
    public static class Builder {
        ImmutableList.Builder<Expression> queryBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<Expression> viewBuilder = new ImmutableList.Builder<>();
        ImmutableSet.Builder<Set<Slot>> viewNoNullableSlotBuilder = new ImmutableSet.Builder<>();
        boolean valid = true;

        /**
         * add comparisonResult
         */
        public Builder addComparisonResult(ComparisonResult comparisonResult) {
            if (comparisonResult.isInvalid()) {
                valid = false;
                return this;
            }
            queryBuilder.addAll(comparisonResult.getQueryExpressions());
            viewBuilder.addAll(comparisonResult.getViewExpressions());
            return this;
        }

        public Builder addQueryExpressions(Collection<? extends Expression> expressions) {
            queryBuilder.addAll(expressions);
            return this;
        }

        public Builder addViewExpressions(Collection<? extends Expression> expressions) {
            viewBuilder.addAll(expressions);
            return this;
        }

        public Builder addViewNoNullableSlot(Set<Slot> viewNoNullableSlot) {
            viewNoNullableSlotBuilder.add(ImmutableSet.copyOf(viewNoNullableSlot));
            return this;
        }

        public boolean isInvalid() {
            return !valid;
        }

        public ComparisonResult build() {
            if (isInvalid()) {
                return ComparisonResult.INVALID;
            }
            return new ComparisonResult(queryBuilder.build(), viewBuilder.build(),
                    viewNoNullableSlotBuilder.build(), valid);
        }
    }

    @Override
    public String toString() {
        if (isInvalid()) {
            return "INVALID";
        }
        return String.format("viewExpressions: %s \n "
                + "queryExpressions :%s \n "
                + "viewNoNullableSlot :%s \n",
                viewExpressions, queryExpressions, viewNoNullableSlot);
    }
}
