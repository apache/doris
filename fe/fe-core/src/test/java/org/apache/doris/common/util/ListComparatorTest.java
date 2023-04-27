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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ListComparatorTest {

    List<List<Comparable>> listCollection;

    @Before
    public void setUp() {
        listCollection = new LinkedList<List<Comparable>>();
    }

    private void printCollection() {
        System.out.println("LIST:");
        for (List<Comparable> list : listCollection) {
            for (Comparable comparable : list) {
                System.out.print(comparable + " | ");
            }
            System.out.println();
        }
        System.out.println("END LIST\n");
    }

    @Test
    public void test_1() {
        // 1, 200, "bcd", 2000
        // 1, 200, "abc"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(new Long(1));
        list1.add(new Long(200));
        list1.add("bcd");
        list1.add(new Long(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(new Long(1));
        list2.add(new Long(200));
        list2.add("abc");
        listCollection.add(list2);

        printCollection();

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                                                                           new OrderByPair(2, false));
        Collections.sort(listCollection, comparator);
        printCollection();

        Assert.assertEquals(list2, listCollection.get(0));
    }

    @Test
    public void test_2() {
        // 1, 200, "abc", 1000
        // 1, 200, "abc"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(new Long(1));
        list1.add(new Long(200));
        list1.add("abc");
        list1.add(new Long(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(new Long(1));
        list2.add(new Long(200));
        list2.add("abc");
        listCollection.add(list2);

        printCollection();

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                                                                           new OrderByPair(2, false));
        Collections.sort(listCollection, comparator);
        printCollection();
        Assert.assertEquals(list2, listCollection.get(0));
    }

    @Test(expected = ClassCastException.class)
    public void test_3() {
        // 1, 200, "abc", 2000
        // 1, 200, "abc", "bcd"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(new Long(1));
        list1.add(new Long(200));
        list1.add("abc");
        list1.add(new Long(2000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(new Long(1));
        list2.add(new Long(200));
        list2.add("abc");
        list2.add("bcd");
        listCollection.add(list2);

        printCollection();

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(1, false),
                                                                           new OrderByPair(3, false));
        Collections.sort(listCollection, comparator);
        Assert.fail();
    }

    @Test
    public void test_4() {
        // 1, 200, "bb", 2000
        // 1, 300, "aa"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(new Long(1));
        list1.add(new Long(200));
        list1.add("bb");
        list1.add(new Long(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(new Long(1));
        list2.add(new Long(300));
        list2.add("aa");
        listCollection.add(list2);

        printCollection();

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(2, false),
                                                                           new OrderByPair(1, false));
        Collections.sort(listCollection, comparator);
        printCollection();
        Assert.assertEquals(list2, listCollection.get(0));
    }

    @Test
    public void test_5() {
        // 1, 200, "bb", 2000
        // 1, 100, "aa"
        // 1, 300, "aa"
        List<Comparable> list1 = new LinkedList<Comparable>();
        list1.add(new Long(1));
        list1.add(new Long(200));
        list1.add("bb");
        list1.add(new Long(1000));
        listCollection.add(list1);

        List<Comparable> list2 = new LinkedList<Comparable>();
        list2.add(new Long(1));
        list2.add(new Long(100));
        list2.add("aa");
        listCollection.add(list2);

        List<Comparable> list3 = new LinkedList<Comparable>();
        list3.add(new Long(1));
        list3.add(new Long(300));
        list3.add("aa");
        listCollection.add(list3);

        printCollection();

        ListComparator<List<Comparable>> comparator = new ListComparator<>(new OrderByPair(2, false),
                                                                           new OrderByPair(1, true));
        Collections.sort(listCollection, comparator);
        printCollection();
        Assert.assertEquals(list3, listCollection.get(0));
    }

}
