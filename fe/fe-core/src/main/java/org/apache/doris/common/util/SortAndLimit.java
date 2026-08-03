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

package org.apache.doris.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility for the common "sort, then truncate to the first N rows" pattern used by several
 * SHOW/proc-dir style commands that operate on {@code List<List<Comparable>>} rows (see
 * {@link ListComparator} and {@link OrderByPair}).
 */
public class SortAndLimit {

    private SortAndLimit() {
    }

    /**
     * Sorts {@code rows} using {@code comparator} and returns a new list truncated to the first
     * {@code sizeLimit} elements.
     *
     * <p>This method does NOT mutate the {@code rows} list passed in: it first copies the input
     * into a new, mutable list and sorts that copy in place, so an immutable input list can be
     * passed safely and the caller's original list/order is left untouched.
     *
     * @param rows       the rows to sort; not modified by this call
     * @param comparator the comparator defining the sort order
     * @param sizeLimit      the maximum number of rows to keep, counted from the start of the sorted
     *                   result; {@link Optional#empty()} means "no sizeLimit" (return every row)
     * @return a new list, sorted by {@code comparator} and truncated to at most {@code sizeLimit}
     *         elements
     */
    public static List<List<Comparable>> sortAndLimit(List<List<Comparable>> rows,
            ListComparator<List<Comparable>> comparator, Optional<Integer> sizeLimit) {
        List<List<Comparable>> sorted = new ArrayList<>(rows);
        sorted.sort(comparator);

        int limit = sizeLimit.orElse(sorted.size());
        return new ArrayList<>(sorted.subList(0, Math.min(limit, sorted.size())));
    }
}
