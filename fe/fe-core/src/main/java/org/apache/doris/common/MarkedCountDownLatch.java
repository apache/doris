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

    private static final long DEFAULT_INIT_TIMEOUT = 2 * 60 * 1000L;

    private final Object lock = new Object();
    private final Object signal = new Object();

    private Multimap<K, V> marks;
    private Multimap<K, V> failedMarks;
    private Status st = Status.OK;
    private int markCount;
    private long initTimeout;
    private volatile CountDownLatch downLatch;


    public MarkedCountDownLatch() {
        marks = HashMultimap.create();
        failedMarks = HashMultimap.create();
        markCount = 0;
        downLatch = null;
        initTimeout = DEFAULT_INIT_TIMEOUT;
    }

    public void setInitTimeout(long initTimeout) {
        this.initTimeout = initTimeout;
    }

    public void addMark(K key, V value) {
        if (downLatch != null) {
            throw new IllegalStateException("downLatch must initialize after mark.");
        }
        synchronized (lock) {
            marks.put(key, value);
            markCount++;
        }
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (signal) {
            if (downLatch == null) {
                this.downLatch = new CountDownLatch(markCount);
                signal.notifyAll();
            }
        }
        return downLatch.await(timeout, unit);
    }

    public void await() throws InterruptedException {
        synchronized (signal) {
            if (downLatch == null) {
                this.downLatch = new CountDownLatch(markCount);
                signal.notifyAll();
            }
        }
        downLatch.await();
    }

    public boolean markedCountDown(K key, V value) {
        // check and wait util downLatch is initialized
        if (downLatch == null) {
            synchronized (signal) {
                long startTime = System.currentTimeMillis();
                while (downLatch == null) {
                    if (System.currentTimeMillis() - startTime > initTimeout) {
                        throw new RuntimeException("MarkedCountDownLatch init timeout.");
                    }
                    try {
                        signal.wait(initTimeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        //remove mark and countDown
        synchronized (lock) {
            if (marks.remove(key, value)) {
                markCount--;
                downLatch.countDown();
                return true;
            }
            return false;
        }
    }

    public boolean markedCountDownWithStatus(K key, V value, Status status)  {
        // check and wait util downLatch is initialized
        if (downLatch == null) {
            synchronized (signal) {
                long startTime = System.currentTimeMillis();
                while (downLatch == null) {
                    if (System.currentTimeMillis() - startTime > initTimeout) {
                        throw new RuntimeException("MarkedCountDownLatch init timeout.");
                    }
                    try {
                        signal.wait(initTimeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        // update status first before countDown.
        // so that the waiting thread will get the correct status.
        synchronized (lock) {

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
                marks.remove(key, value);
                markCount--;
                downLatch.countDown();
                return true;
            }
            return false;
        }
    }

    public void countDownToZero(Status status)  {
        // check and wait util downLatch is initialized
        if (downLatch == null) {
            synchronized (signal) {
                long startTime = System.currentTimeMillis();
                while (downLatch == null) {
                    if (System.currentTimeMillis() - startTime > initTimeout) {
                        throw new RuntimeException("MarkedCountDownLatch init timeout.");
                    }
                    try {
                        signal.wait(initTimeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        // update status first before countDown.
        // so that the waiting thread will get the correct status.
        synchronized (lock) {
            if (st.ok()) {
                st = status;
            }
            while (downLatch.getCount() > 0) {
                markCount--;
                downLatch.countDown();
            }

            //clear up the marks list
            marks.clear();
        }
    }

    public int getMarkCount() {
        synchronized (lock) {
            return markCount;
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


}
