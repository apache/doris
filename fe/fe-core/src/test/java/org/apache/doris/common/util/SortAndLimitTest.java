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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class SortAndLimitTest {

    private static final ListComparator<List<Comparable>> BY_FIRST_COLUMN = new ListComparator<>(0);

    private static List<List<Comparable>> rows(Comparable... firstColumnValues) {
        List<List<Comparable>> rows = Lists.newArrayList();
        for (Comparable value : firstColumnValues) {
            rows.add(Lists.newArrayList(value));
        }
        return rows;
    }

    private static List<Comparable> firstColumnOf(List<List<Comparable>> rows) {
        List<Comparable> values = Lists.newArrayList();
        for (List<Comparable> row : rows) {
            values.add(row.get(0));
        }
        return values;
    }

    @Test
    public void testEmptyLimitKeepsEveryRow() {
        List<List<Comparable>> sorted = SortAndLimit.sortAndLimit(rows(3L, 1L, 2L), BY_FIRST_COLUMN,
                Optional.empty());
        Assert.assertEquals(Lists.newArrayList(1L, 2L, 3L), firstColumnOf(sorted));
    }

    @Test
    public void testLimitAppliesToTheSortedResult() {
        // the two smallest values, not the first two rows of the input
        List<List<Comparable>> sorted = SortAndLimit.sortAndLimit(rows(3L, 1L, 2L), BY_FIRST_COLUMN,
                Optional.of(2));
        Assert.assertEquals(Lists.newArrayList(1L, 2L), firstColumnOf(sorted));
    }

    @Test
    public void testLimitLargerThanInputIsClamped() {
        List<List<Comparable>> sorted = SortAndLimit.sortAndLimit(rows(3L, 1L), BY_FIRST_COLUMN,
                Optional.of(100));
        Assert.assertEquals(Lists.newArrayList(1L, 3L), firstColumnOf(sorted));
    }

    @Test
    public void testInputIsNotModified() {
        List<List<Comparable>> input = ImmutableList.<List<Comparable>>of(
                ImmutableList.<Comparable>of(3L),
                ImmutableList.<Comparable>of(1L));
        List<List<Comparable>> sorted = SortAndLimit.sortAndLimit(input, BY_FIRST_COLUMN, Optional.of(1));
        Assert.assertEquals(Lists.newArrayList(1L), firstColumnOf(sorted));
        Assert.assertEquals(Lists.newArrayList(3L, 1L), firstColumnOf(input));
    }
}
