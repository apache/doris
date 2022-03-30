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

package org.apache.doris.planner;

import java.util.List;
import java.util.Optional;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

/**
 * There are two kinds of predicates for a column: `is null` predicate and other predicates that
 * the value of a column is not null, e.g., col=1, col>2, col in (1,2,3), etc.
 *
 * This can represent both conjunctive and disjunctive predicates for a column.
 *
 * The meaning of the predicates is: `conjunctiveIsNull` AND (`rangeSet` OR `disjunctiveIsNull`)
 *
 * Notes about internal state:
 * 1. If `conjunctiveIsNull` and  `disjunctiveIsNull` are both false and `rangeSet` is null,
 * it means that there is no filter for the column. See {@link ColumnRange#hasFilter()}.
 * 2. If `rangeSet` is empty, it means that the `not null` predicates are folded to false literal,
 * i.e., col=1 and col=2.
 */
public class ColumnRange {
    private boolean hasConjunctiveIsNull;
    private boolean hasDisjunctiveIsNull;
    private RangeSet<ColumnBound> rangeSet;

    private ColumnRange() {
    }

    public void intersect(List<Range<ColumnBound>> disjunctiveRanges) {
        if (disjunctiveRanges != null && !disjunctiveRanges.isEmpty()) {
            if (rangeSet == null) {
                rangeSet = TreeRangeSet.create();
                disjunctiveRanges.forEach(rangeSet::add);
            } else {
                RangeSet<ColumnBound> merged = TreeRangeSet.create();
                disjunctiveRanges.forEach(range -> merged.addAll(rangeSet.subRangeSet(range)));
                rangeSet = merged;
            }
        }
    }

    public Optional<RangeSet<ColumnBound>> getRangeSet() {
        if (rangeSet == null) {
            return Optional.empty();
        } else {
            return Optional.of(rangeSet);
        }
    }

    public static ColumnRange create() {
        return new ColumnRange();
    }

    public boolean hasConjunctiveIsNull() {
        return hasConjunctiveIsNull;
    }

    public ColumnRange setHasConjunctiveIsNull(boolean hasConjunctiveIsNull) {
        this.hasConjunctiveIsNull = hasConjunctiveIsNull;
        return this;
    }

    public boolean hasDisjunctiveIsNull() {
        return hasDisjunctiveIsNull;
    }

    public ColumnRange setHasDisjunctiveIsNull(boolean hasDisjunctiveIsNull) {
        this.hasDisjunctiveIsNull = hasDisjunctiveIsNull;
        return this;
    }

    public boolean hasFilter() {
        return hasConjunctiveIsNull || hasDisjunctiveIsNull || rangeSet != null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("hasConjunctiveIsNull", hasConjunctiveIsNull)
            .add("hasDisjunctiveIsNull", hasDisjunctiveIsNull)
            .add("rangeSet", rangeSet)
            .toString();
    }
}

