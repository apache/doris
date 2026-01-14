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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class MarkedCountDownLatchTest {


    @Test
    public void testNormal() throws Exception {

        final MarkedCountDownLatch<String, String> markedCountDownLatch = new MarkedCountDownLatch<>();
        markedCountDownLatch.addMark("k1", "v1");
        markedCountDownLatch.addMark("k2", "v2");
        markedCountDownLatch.addMark("k3", "v3");
        Assert.assertEquals(3, markedCountDownLatch.getMarkCount());


        Thread t = new Thread(() -> {
            int i = 1;
            while (i < 4) {
                try {
                    markedCountDownLatch.markedCountDown("k" + i, "v" + i);
                } catch (Exception e) {
                    if (e instanceof IllegalStateException) {
                        continue;
                    }
                    throw e;
                }
                Assert.assertEquals(3 - i, markedCountDownLatch.getMarkCount());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                i++;
            }
        });
        t.start();

        long startTime = System.currentTimeMillis();
        markedCountDownLatch.await(10, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(0, markedCountDownLatch.getMarkCount());
        Assert.assertTrue(endTime - startTime < 10 * 1000L);
    }

    @Test
    public void testCountWithStatus() throws Exception {
        final MarkedCountDownLatch<String, String> markedCountDownLatch = new MarkedCountDownLatch<>();
        markedCountDownLatch.addMark("k1", "v1");
        markedCountDownLatch.addMark("k2", "v2");
        markedCountDownLatch.addMark("k3", "v3");
        Assert.assertEquals(3, markedCountDownLatch.getMarkCount());

        Thread t = new Thread(() -> {
            int i = 1;
            while (i < 4) {
                try {
                    markedCountDownLatch.markedCountDownWithStatus("k" + i, "v" + i, Status.FINISHED);
                } catch (Exception e) {
                    if (e instanceof IllegalStateException) {
                        continue;
                    }
                    throw e;
                }
                Assert.assertEquals(3 - i, markedCountDownLatch.getMarkCount());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                i++;
            }
        });
        t.start();

        long startTime = System.currentTimeMillis();
        markedCountDownLatch.await(10, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(0, markedCountDownLatch.getMarkCount());
        Assert.assertTrue(endTime - startTime < 10 * 1000L);
        Assert.assertEquals(Status.FINISHED, markedCountDownLatch.getStatus());
    }

    @Test
    public void testTimeout() throws Exception {

        final MarkedCountDownLatch<String, String> markedCountDownLatch = new MarkedCountDownLatch<>();
        markedCountDownLatch.addMark("k1", "v1");
        markedCountDownLatch.addMark("k2", "v2");
        markedCountDownLatch.addMark("k3", "v3");
        Assert.assertEquals(3, markedCountDownLatch.getMarkCount());

        Thread t = new Thread(() -> {
            int i = 1;
            while (i < 3) {
                try {
                    markedCountDownLatch.markedCountDown("k" + i, "v" + i);
                } catch (Exception e) {
                    if (e instanceof IllegalStateException) {
                        continue;
                    }
                    throw e;
                }
                Assert.assertEquals(3 - i, markedCountDownLatch.getMarkCount());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                i++;
            }
        });
        t.start();

        long startTime = System.currentTimeMillis();
        markedCountDownLatch.await(10, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(1, markedCountDownLatch.getMarkCount());
        Assert.assertTrue(endTime - startTime >= 10 * 1000L);
    }

    @Test(expected = IllegalStateException.class)
    public void testNotWaitException()  {

        final MarkedCountDownLatch<String, String> markedCountDownLatch = new MarkedCountDownLatch<>();
        markedCountDownLatch.addMark("k1", "v1");
        markedCountDownLatch.addMark("k2", "v2");
        markedCountDownLatch.addMark("k3", "v3");
        Assert.assertEquals(3, markedCountDownLatch.getMarkCount());

        //call markedCountDown before wait method will throw  IllegalStateException
        markedCountDownLatch.markedCountDown("k1", "v1");
    }


    @Test
    public void testCountDownToZero() throws Exception {
        final MarkedCountDownLatch<String, String> markedCountDownLatch = new MarkedCountDownLatch<>();
        markedCountDownLatch.addMark("k1", "v1");
        markedCountDownLatch.addMark("k2", "v2");
        markedCountDownLatch.addMark("k3", "v3");
        Assert.assertEquals(3, markedCountDownLatch.getMarkCount());

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    markedCountDownLatch.countDownToZero(Status.CANCELLED);
                } catch (Exception e) {
                    if (e instanceof IllegalStateException) {
                        continue;
                    }
                    throw e;
                }
                Assert.assertEquals(0, markedCountDownLatch.getMarkCount());
                break;
            }

        });
        t.start();

        long startTime = System.currentTimeMillis();
        markedCountDownLatch.await(10, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        Assert.assertEquals(0, markedCountDownLatch.getMarkCount());
        Assert.assertTrue(endTime - startTime < 10 * 1000L);
        Assert.assertEquals(Status.CANCELLED, markedCountDownLatch.getStatus());
    }


}
