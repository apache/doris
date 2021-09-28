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

package org.apache.doris.task;

import org.apache.doris.load.sync.SyncChannelCallback;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SerialExecutorServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(MasterTaskExecutorTest.class);
    private static final int NUM_OF_SLOTS = 10;
    private static final int THREAD_NUM = 10;

    private static SerialExecutorService taskPool;
    // thread signature -> tasks submit serial
    private static Map<Long, List<Integer>> submitSerial;
    // thread signature -> tasks execute serial
    private static Map<Long, List<Integer>> execSerial;

    @Before
    public void setUp() {
        taskPool = new SerialExecutorService(NUM_OF_SLOTS);
        submitSerial = new ConcurrentHashMap<>();
        execSerial = new ConcurrentHashMap<>();
    }

    @After
    public void tearDown() {
        if (taskPool != null) {
            taskPool.close();
        }
    }

    @Test
    public void testSubmit() {
        for (long i = 0; i < THREAD_NUM; i++) {
            if (!submitSerial.containsKey(i)) {
                submitSerial.put(i, new ArrayList<>());
            }
            SubmitThread thread = new SubmitThread("Thread-" + i, i, submitSerial.get(i));
            thread.start();
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }

        // The submission order of the same signature should be equal to the execution order
        Assert.assertEquals(submitSerial.size(), THREAD_NUM);
        Assert.assertEquals(submitSerial.size(), execSerial.size());
        for (long i = 0; i < THREAD_NUM; i++) {
            Assert.assertTrue(submitSerial.containsKey(i));
            Assert.assertTrue(execSerial.containsKey(i));
            List<Integer> submitSerialList = submitSerial.get(i);
            List<Integer> execSerialList = execSerial.get(i);
            Assert.assertEquals(submitSerialList.size(), execSerialList.size());
            for (int j = 0; j < submitSerialList.size(); j++) {
                Assert.assertEquals(submitSerialList.get(j), execSerialList.get(j));
            }
        }
    }

    private static class TestSyncTask extends SyncTask {
        public int serial;

        public TestSyncTask(long signature, int index, int serial, SyncChannelCallback callback) {
            super(signature, index, callback);
            this.serial = serial;
        }

        @Override
        protected void exec() {
            LOG.info("run exec. signature: {}, index: {}, serial: {}", signature, index, serial);
            if (!execSerial.containsKey(signature)) {
                execSerial.put(signature, new ArrayList<>());
            }
            execSerial.get(signature).add(serial);
        }
    }

    private static class SubmitThread extends Thread {
        private int index = SyncTaskPool.getNextIndex();
        private long signature;
        private List<Integer> submitSerialList;

        public SubmitThread(String name, long signature, List<Integer> submitSerialList) {
            super(name);
            this.signature = signature;
            this.submitSerialList = submitSerialList;
        }

        public void run() {
            for (int i = 0; i < 100; i++) {
                TestSyncTask task = new TestSyncTask(signature, index, i, new SyncChannelCallback() {
                    @Override
                    public void onFinished(long channelId) {
                    }
                    @Override
                    public void onFailed(String errMsg) {
                    }
                });
                submitSerialList.add(i);
                taskPool.submit(task);
            }
        }
    }
}
