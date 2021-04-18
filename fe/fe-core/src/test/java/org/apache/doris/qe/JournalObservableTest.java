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

package org.apache.doris.qe;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.Test;

public class JournalObservableTest {
    @Test
    public void testUpperBound() {
        Multiset<JournalObserver> elements = TreeMultiset.create();
        JournalObserver observer2 = new JournalObserver(2L);
        JournalObserver observer4 = new JournalObserver(4L);
        JournalObserver observer41 = new JournalObserver(4L);
        JournalObserver observer42 = new JournalObserver(4L);
        JournalObserver observer6 = new JournalObserver(6L);

        // empty
        {
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), 0, 1L));
        }

        // one element
        {
            elements.add(observer2);
            int size = elements.size();
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 3L));
        }

        // same element
        {
            elements.clear();
            elements.add(observer2);
            elements.add(observer6);
            elements.add(observer4);
            elements.add(observer41);
            elements.add(observer42);

            for (JournalObserver journalObserver : elements) {
                System.out.println(journalObserver);
            }

            int size = elements.size();
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assert.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 4L));
            elements.remove(observer41);
            Assert.assertEquals(3, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(observer4);
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(observer42);
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
        }

        // same element 2
        {
            elements.clear();
            elements.add(observer4);
            elements.add(observer41);

            int size = elements.size();
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 4L));
            elements.remove(observer41);
            Assert.assertEquals(1, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
            elements.remove(observer4);
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), elements.size(), 4L));
        }

        // odd elements
        {
            elements.clear();
            elements.add(observer2);
            elements.add(observer2);
            elements.add(observer4);
            elements.add(observer4);
            elements.add(observer6);
            elements.add(observer6);
            int size = elements.size();
//            System.out.println("size=" + size);
//            for(int i = 0; i < size; i ++) {
//                System.out.println("array " + i + " = " + ((MasterOpExecutor)elements.get(i)).getTargetJournalId());
//            }
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assert.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 4L));
            Assert.assertEquals(4, JournalObservable.upperBound(elements.toArray(), size, 5L));
            Assert.assertEquals(6, JournalObservable.upperBound(elements.toArray(), size, 6L));
            Assert.assertEquals(6, JournalObservable.upperBound(elements.toArray(), size, 7L));
        }
        // even elements
        {
            elements.clear();
            elements.add(observer2);
            elements.add(observer2);
            elements.add(observer4);
            elements.add(observer4);
            elements.add(observer4);
            elements.add(observer6);
            elements.add(observer6);
            int size = elements.size();
            Assert.assertEquals(0, JournalObservable.upperBound(elements.toArray(), size, 1L));
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 2L));
            Assert.assertEquals(2, JournalObservable.upperBound(elements.toArray(), size, 3L));
            Assert.assertEquals(5, JournalObservable.upperBound(elements.toArray(), size, 4L));
            Assert.assertEquals(5, JournalObservable.upperBound(elements.toArray(), size, 5L));
            Assert.assertEquals(7, JournalObservable.upperBound(elements.toArray(), size, 6L));
            Assert.assertEquals(7, JournalObservable.upperBound(elements.toArray(), size, 7L));
        }
        {
            CountDownLatch latch = new CountDownLatch(1);
            System.out.println(latch.getCount());

            latch.countDown();
            System.out.println(latch.getCount());

            latch.countDown();
            System.out.println(latch.getCount());

            latch.countDown();
            System.out.println(latch.getCount());
        }
        System.out.println("success");
    }
}

