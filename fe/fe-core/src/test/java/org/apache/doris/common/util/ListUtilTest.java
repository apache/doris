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

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class ListUtilTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSplitBySizeNormal() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        int expectSize = 3;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assert.assertEquals(splitLists.size(), 3);
        Assert.assertEquals(splitLists.get(0).size(), 3);
        Assert.assertEquals(splitLists.get(1).size(), 2);
        Assert.assertEquals(splitLists.get(2).size(), 2);
    }

    @Test
    public void testSplitBySizeNormal2() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7);
        int expectSize = 1;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assert.assertEquals(splitLists.size(), 1);
        Assert.assertEquals(lists, splitLists.get(0));
    }

    @Test
    public void testSplitBySizeWithLargeExpectSize() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3);
        int expectSize = 10;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assert.assertEquals(splitLists.size(), lists.size());
        Assert.assertTrue( splitLists.get(0).get(0) == 1);
        Assert.assertTrue( splitLists.get(1).get(0) == 2);
        Assert.assertTrue( splitLists.get(2).get(0) == 3);
    }

    @Test
    public void testSplitBySizeWithEmptyList() {
        List<Integer> lists = Lists.newArrayList();
        int expectSize = 10;

        List<List<Integer>> splitLists = ListUtil.splitBySize(lists, expectSize);

        Assert.assertEquals(splitLists.size(), lists.size());
    }

    @Test
    public void testSplitBySizeWithNullList() {
        List<Integer> lists = null;
        int expectSize = 10;

        expectedEx.expect(NullPointerException.class);
        expectedEx.expectMessage("list must not be null");

        ListUtil.splitBySize(lists, expectSize);
    }

    @Test
    public void testSplitBySizeWithNegativeSize() {
        List<Integer> lists = Lists.newArrayList(1, 2, 3);
        int expectSize = -1;

        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("expectedSize must larger than 0");

        ListUtil.splitBySize(lists, expectSize);
    }
}
