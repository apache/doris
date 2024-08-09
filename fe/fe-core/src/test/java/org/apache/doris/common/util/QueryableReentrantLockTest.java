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

import org.apache.doris.common.lock.MonitoredReentrantLock;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class QueryableReentrantLockTest {

    private MonitoredReentrantLock lock = new MonitoredReentrantLock(true);

    @Test
    public void test() throws InterruptedException {

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } finally {
                    lock.unlock();
                }
            }
        }, "thread1");

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                try {
                    if (!lock.tryLock(1000, TimeUnit.MILLISECONDS)) {
                        Thread owner = lock.getOwner();
                        Assert.assertEquals("thread1", owner.getName());

                        System.out.println(Util.dumpThread(owner, 10));

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }, "thread2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }

}
