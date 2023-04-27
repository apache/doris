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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ListUtilTest {

    private static PartitionKey pk1;
    private static PartitionKey pk2;
    private static PartitionKey pk3;
    private static List<PartitionKey> listA = new ArrayList<>();
    private static List<PartitionKey> listB = new ArrayList<>();
    private static List<PartitionKey> listC = new ArrayList<>();

    @BeforeClass
    public static void setUp() throws AnalysisException {
        Column charString = new Column("char", PrimitiveType.CHAR);
        Column varchar = new Column("varchar", PrimitiveType.VARCHAR);

        pk1 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("beijing")), Arrays.asList(charString));
        pk2 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("shanghai")), Arrays.asList(varchar));
        pk3 = PartitionKey.createPartitionKey(Arrays.asList(new PartitionValue("tianjin")), Arrays.asList(varchar));

        listA.add(pk1);

        listB.add(pk1);
        listB.add(pk2);

        listC.add(pk1);
        listC.add(pk2);
        listC.add(pk3);
    }

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
        Assert.assertEquals(1, (int) splitLists.get(0).get(0));
        Assert.assertEquals(2, (int) splitLists.get(1).get(0));
        Assert.assertEquals(3, (int) splitLists.get(2).get(0));
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

    @Test
    public void testListsMatchNormal() throws DdlException {

        List<PartitionItem> list1 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listB));
        List<PartitionItem> list2 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listB));

        ListUtil.checkPartitionKeyListsMatch(list1, list2);

    }

    @Test(expected = DdlException.class)
    public void testListsMatchSameSize() throws DdlException {
        List<PartitionItem> list1 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listB));
        List<PartitionItem> list2 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listC));

        ListUtil.checkPartitionKeyListsMatch(list1, list2);
    }

    @Test(expected = DdlException.class)
    public void testListMatchDiffSize() throws DdlException {
        List<PartitionItem> list1 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listB));
        List<PartitionItem> list2 = Arrays.asList(new ListPartitionItem(listA), new ListPartitionItem(listB),
                new ListPartitionItem(listC));

        ListUtil.checkPartitionKeyListsMatch(list1, list2);
    }
}
