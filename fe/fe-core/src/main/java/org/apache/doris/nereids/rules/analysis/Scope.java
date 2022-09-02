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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The slot range required for expression analyze.
 *
 * slots: The symbols used at this level are stored in slots.
 * outerScope: The scope information corresponding to the parent is stored in outerScope.
 * ownerSubquery: The subquery corresponding to ownerSubquery.
 * subqueryToOuterCorrelatedSlots: The slots correlated in the subquery,
 *                                 only the slots that cannot be resolved at this level.
 *
 * eg:
 * t1(k1, v1) / t2(k2, v2)
 * select * from t1 where t1.k1 = (select sum(k2) from t2 where t1.v1 = t2.v2);
 *
 * When analyzing subquery:
 *
 * slots: k2, v2;
 * outerScope:
 *      slots: k1, v1;
 *      outerScope: Optional.empty();
 *      ownerSubquery: Optionsl.empty();
 *      subqueryToOuterCorrelatedSlots: empty();
 * ownerSubquery: subquery((select sum(k2) from t2 where t1.v1 = t2.v2));
 * subqueryToOuterCorrelatedSlots: (subquery, v1);
 */
public class Scope {
    private final Optional<Scope> outerScope;
    private final List<Slot> slots;

    private final Optional<SubqueryExpr> ownerSubquery;
    private List<Slot> correlatedSlots;

    public Scope(Optional<Scope> outerScope, List<Slot> slots, Optional<SubqueryExpr> subqueryExpr) {
        this.outerScope = outerScope;
        this.slots = slots;
        this.ownerSubquery = subqueryExpr;
        this.correlatedSlots = new ArrayList<>();
    }

    public Scope(List<Slot> slots) {
        this.outerScope = Optional.empty();
        this.slots = slots;
        this.ownerSubquery = Optional.empty();
        this.correlatedSlots = new ArrayList<>();
    }

    public List<Slot> getSlots() {
        return slots;
    }

    public Optional<Scope> getOuterScope() {
        return outerScope;
    }

    public Optional<SubqueryExpr> getSubquery() {
        return ownerSubquery;
    }

    public List<Slot> getCorrelatedSlots() {
        return correlatedSlots;
    }

    /**
     * generate scope link from inner to outer.
     * Used for multi-level subquery parsing.
     */
    public List<Scope> toScopeLink() {
        Scope scope = this;
        Builder<Scope> builder = ImmutableList.<Scope>builder().add(scope);
        while (scope.getOuterScope().isPresent()) {
            scope = scope.getOuterScope().get();
            builder.add(scope);
        }
        return builder.build();
    }
}
