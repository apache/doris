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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/BetweenPredicate.java
// and modified by Doris

package org.apache.doris.analysis;

import com.google.gson.annotations.SerializedName;

/**
 * Class describing between predicates. After successful analysis, we equal
 * the between predicate to a conjunctive/disjunctive compound predicate
 * to be handed to the backend.
 */
@Deprecated
public class BetweenPredicate extends Predicate {
    @SerializedName("inb")
    private boolean isNotBetween;

    private BetweenPredicate() {
        // use for serde only
    }

    protected BetweenPredicate(BetweenPredicate other) {
        super(other);
        isNotBetween = other.isNotBetween;
    }

    public boolean isNotBetween() {
        return isNotBetween;
    }

    @Override
    public Expr clone() {
        return new BetweenPredicate(this);
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public Expr clone(ExprSubstitutionMap sMap) {
        return new BetweenPredicate(this);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        BetweenPredicate that = (BetweenPredicate) o;
        return isNotBetween == that.isNotBetween;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Boolean.hashCode(isNotBetween);
    }
}
