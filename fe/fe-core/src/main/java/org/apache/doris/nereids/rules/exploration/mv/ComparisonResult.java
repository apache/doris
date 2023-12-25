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

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 * comparison result of view and query
 */
public class ComparisonResult {
    public static final ComparisonResult INVALID = new ComparisonResult(ImmutableList.of(), ImmutableList.of(), false);
    public static final ComparisonResult EMPTY = new ComparisonResult(ImmutableList.of(), ImmutableList.of(), true);
    private final boolean valid;
    private final List<Expression> viewExpressions;
    private final List<Expression> queryExpressions;

    public ComparisonResult(List<Expression> queryExpressions, List<Expression> viewExpressions) {
        this(queryExpressions, viewExpressions, true);
    }

    ComparisonResult(List<Expression> queryExpressions, List<Expression> viewExpressions, boolean valid) {
        this.viewExpressions = ImmutableList.copyOf(viewExpressions);
        this.queryExpressions = ImmutableList.copyOf(queryExpressions);
        this.valid = valid;
    }

    public List<Expression> getViewExpressions() {
        return viewExpressions;
    }

    public List<Expression> getQueryExpressions() {
        return queryExpressions;
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

        public Builder addQueryExpressions(Collection<Expression> expressions) {
            queryBuilder.addAll(expressions);
            return this;
        }

        public Builder addViewExpressions(Collection<Expression> expressions) {
            viewBuilder.addAll(expressions);
            return this;
        }

        public ComparisonResult build() {
            return new ComparisonResult(queryBuilder.build(), viewBuilder.build(), valid);
        }
    }
}
