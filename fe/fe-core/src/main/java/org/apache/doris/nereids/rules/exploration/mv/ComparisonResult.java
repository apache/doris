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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * comparison result of view and query
 */
public class ComparisonResult {
    private final boolean valid;
    private final List<Expression> viewExpressions;
    private final List<Expression> queryExpressions;
    private final List<Expression> queryAllPulledUpExpressions;
    private final Set<Set<Slot>> viewNoNullableSlot;
    private final String errorMessage;

    ComparisonResult(List<Expression> queryExpressions, List<Expression> queryAllPulledUpExpressions,
            List<Expression> viewExpressions, Set<Set<Slot>> viewNoNullableSlot, boolean valid, String message) {
        this.viewExpressions = ImmutableList.copyOf(viewExpressions);
        this.queryExpressions = ImmutableList.copyOf(queryExpressions);
        this.queryAllPulledUpExpressions = ImmutableList.copyOf(queryAllPulledUpExpressions);
        this.viewNoNullableSlot = ImmutableSet.copyOf(viewNoNullableSlot);
        this.valid = valid;
        this.errorMessage = message;
    }

    public static ComparisonResult newInvalidResWithErrorMessage(String errorMessage) {
        return new ComparisonResult(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                ImmutableSet.of(), false, errorMessage);
    }

    public List<Expression> getViewExpressions() {
        return viewExpressions;
    }

    public List<Expression> getQueryExpressions() {
        return queryExpressions;
    }

    public List<Expression> getQueryAllPulledUpExpressions() {
        return queryAllPulledUpExpressions;
    }

    public Set<Set<Slot>> getViewNoNullableSlot() {
        return viewNoNullableSlot;
    }

    public boolean isInvalid() {
        return !valid;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Builder
     */
    public static class Builder {
        ImmutableList.Builder<Expression> queryBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<Expression> viewBuilder = new ImmutableList.Builder<>();
        ImmutableSet.Builder<Set<Slot>> viewNoNullableSlotBuilder = new ImmutableSet.Builder<>();
        ImmutableList.Builder<Expression> queryAllPulledUpExpressionsBuilder = new ImmutableList.Builder<>();
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

        /**Add slots which should reject null slots in view*/
        public Builder addViewNoNullableSlot(Pair<Set<Slot>, Set<Slot>> viewNoNullableSlotsPair) {
            if (!viewNoNullableSlotsPair.first.isEmpty()) {
                viewNoNullableSlotBuilder.add(viewNoNullableSlotsPair.first);
            }
            if (!viewNoNullableSlotsPair.second.isEmpty()) {
                viewNoNullableSlotBuilder.add(viewNoNullableSlotsPair.second);
            }
            return this;
        }

        public Builder addQueryAllPulledUpExpressions(Collection<? extends Expression> expressions) {
            queryAllPulledUpExpressionsBuilder.addAll(expressions);
            return this;
        }

        public boolean isInvalid() {
            return !valid;
        }

        public ComparisonResult build() {
            Preconditions.checkArgument(valid, "Comparison result must be valid");
            return new ComparisonResult(queryBuilder.build(),
                    queryAllPulledUpExpressionsBuilder.build().stream()
                            .filter(expr -> !expr.isInferred()).collect(Collectors.toList()),
                    viewBuilder.build(), viewNoNullableSlotBuilder.build(), valid, "");
        }
    }

    @Override
    public String toString() {
        return String.format("valid: %s \n "
                        + "viewExpressions: %s \n "
                        + "queryExpressions :%s \n "
                        + "viewNoNullableSlot :%s \n"
                        + "queryAllPulledUpExpressions :%s \n", valid, viewExpressions, queryExpressions,
                viewNoNullableSlot, queryAllPulledUpExpressions);
    }
}
