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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MasterTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(MasterTaskExecutorTest.class);
    private static final int THREAD_NUM = 1;
    private static final long SLEEP_MS = 10L;

    private MasterTaskExecutor executor;

    @Before
    public void setUp() {
        executor = new MasterTaskExecutor("master_task_executor_test", THREAD_NUM, false);
        executor.start();
    }
    
    @After
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    public void testSubmit() {
        // submit task
        MasterTask task1 = new TestMasterTask(1L);
        Assert.assertTrue(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());
        // submit same running task error
        Assert.assertFalse(executor.submit(task1));
        Assert.assertEquals(1, executor.getTaskNum());
        
        // submit another task
        MasterTask task2 = new TestMasterTask(2L);
        Assert.assertTrue(executor.submit(task2));
        Assert.assertEquals(2, executor.getTaskNum());
        
        // wait for tasks run to end
        try {
            // checker thread interval is 1s
            // sleep 3s
            Thread.sleep(SLEEP_MS * 300);
            Assert.assertEquals(0, executor.getTaskNum());
        } catch (InterruptedException e) {
            LOG.error("error", e);
        }
    }
    
    private class TestMasterTask extends MasterTask {
        
        public TestMasterTask(long signature) {
            this.signature = signature;
        }

        @Override
        protected void exec() {
            LOG.info("run exec. signature: {}", signature);
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                LOG.error("error", e);
            }
        }
        
    }
}
