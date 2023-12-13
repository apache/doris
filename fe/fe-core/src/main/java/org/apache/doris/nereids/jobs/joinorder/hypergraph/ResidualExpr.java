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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class ResidualExpr {
    public static final ResidualExpr INVALID_RESIDUAL_EXPR = new ResidualExpr(ImmutableSet.of(), ImmutableSet.of(), false);
    public static final ResidualExpr VALID_EMPTY_RESIDUAL_EXPR = new ResidualExpr(ImmutableSet.of(), ImmutableSet.of(), true);

    private final Set<Expression> queryPredicate;
    private final Set<Expression> viewPredicate;
    private final boolean isValid;

    ResidualExpr(Set<Expression> queryPredicate, Set<Expression> viewPredicate, boolean isValid) {
        this.queryPredicate = ImmutableSet.copyOf(queryPredicate);
        this.viewPredicate = ImmutableSet.copyOf(viewPredicate);
        this.isValid = isValid;
    }

    public boolean isValid() {
        return isValid;
    }

    public Set<Expression> getQueryPredicate() {
        return queryPredicate;
    }

    public Set<Expression> getViewPredicate() {
        return viewPredicate;
    }
}
