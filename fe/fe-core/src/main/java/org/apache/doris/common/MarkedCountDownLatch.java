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

package org.apache.doris.common;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MarkedCountDownLatch<K, V>  {

    private final Object lock = new Object();

    private Multimap<K, V> marks;
    private Multimap<K, V> failedMarks;
    private Status st = Status.OK;
    private int markCount;
    private CountDownLatch downLatch;


    public MarkedCountDownLatch() {
        marks = HashMultimap.create();
        failedMarks = HashMultimap.create();
        markCount = 0;
        downLatch = null;
    }

    public int getMarkCount() {
        return markCount;
    }

    public long getCount() {
        synchronized (lock) {
            if (downLatch == null) {
                throw new IllegalStateException("downLatch is not initialize checkout usage is valid.");
            }
        }
        return downLatch.getCount();
    }

    public void countDown() {
        synchronized (lock) {
            if (downLatch == null) {
                throw new IllegalStateException("downLatch is not initialize checkout usage is valid.");
            }
        }
        downLatch.countDown();
    }

    public void addMark(K key, V value) {
        synchronized (lock) {
            marks.put(key, value);
            markCount++;
        }
    }

    public boolean markedCountDown(K key, V value) {
        synchronized (lock) {
            if (downLatch == null) {
                throw new IllegalStateException("downLatch is not initialize checkout usage is valid.");
            }
            if (marks.remove(key, value)) {
                downLatch.countDown();
                return true;
            }
            return false;
        }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (lock) {
            if (downLatch == null) {
                this.downLatch = new CountDownLatch(markCount);
            }
        }
        return downLatch.await(timeout, unit);
    }

    public void await() throws InterruptedException {
        synchronized (lock) {
            if (downLatch == null) {
                this.downLatch = new CountDownLatch(markCount);
            }
        }
        downLatch.await();
    }

    public boolean markedCountDownWithStatus(K key, V value, Status status) {
        // update status first before countDown.
        // so that the waiting thread will get the correct status.
        synchronized (lock) {
            if (downLatch == null) {
                throw new IllegalStateException("downLatch is not initialize checkout usage is valid.");
            }

            if (st.ok()) {
                st = status;
            }

            // Since marks are used to determine whether a task is completed, we should not remove
            // a mark if the task has failed rather than finished. To maintain the idempotency of
            // this method, we store failed marks in a separate map.
            //
            // Search `getLeftMarks` for details.
            if (!failedMarks.containsEntry(key, value)) {
                failedMarks.put(key, value);
                downLatch.countDown();
                return true;
            }
            return false;
        }
    }

    public List<Entry<K, V>> getLeftMarks() {
        synchronized (lock) {
            return Lists.newArrayList(marks.entries());
        }
    }

    public Status getStatus() {
        synchronized (lock) {
            return st;
        }
    }

    public void countDownToZero(Status status) {
        synchronized (lock) {
            if (downLatch == null) {
                throw new IllegalStateException("downLatch is not initialize checkout usage is valid.");
            }
            // update status first before countDown.
            // so that the waiting thread will get the correct status.
            if (st.ok()) {
                st = status;
            }
            while (downLatch.getCount() > 0) {
                downLatch.countDown();
            }
        }
    }
}
