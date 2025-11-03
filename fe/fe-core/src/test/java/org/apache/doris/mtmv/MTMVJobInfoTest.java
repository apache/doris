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

package org.apache.doris.mtmv;

import org.apache.doris.common.Config;
import org.apache.doris.job.extensions.mtmv.MTMVTask;

import org.junit.Assert;
import org.junit.Test;

public class MTMVJobInfoTest {

    @Test
    public void testAddHistoryTask() {
        int originalCount = Config.max_persistence_task_count;
        Config.max_persistence_task_count = 0;
        MTMVJobInfo jobInfo = new MTMVJobInfo("dummyJob");
        jobInfo.addHistoryTask(new MTMVTask());
        Assert.assertEquals(0, jobInfo.getHistoryTasks().size());
        Config.max_persistence_task_count = 2;
        for (int i = 0; i < 3; i++) {
            jobInfo.addHistoryTask(new MTMVTask());
        }
        Assert.assertEquals(2, jobInfo.getHistoryTasks().size());
        Config.max_persistence_task_count = 1;
        jobInfo.addHistoryTask(new MTMVTask());
        Assert.assertEquals(1, jobInfo.getHistoryTasks().size());
        Config.max_persistence_task_count = originalCount;
    }
}
