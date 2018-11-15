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

package org.apache.doris.load.routineload;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.system.SystemInfoService;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Catalog.class})
public class RoutineLoadSchedulerTest {

    @Test
    public void testNormalRunOneCycle() throws LoadException, MetaNotFoundException {
        int taskNum = 1;
        List<RoutineLoadTask> routineLoadTaskList = new ArrayList<>();
        KafkaRoutineLoadTask kafkaRoutineLoadTask = EasyMock.createNiceMock(KafkaRoutineLoadTask.class);
        EasyMock.expect(kafkaRoutineLoadTask.getSignature()).andReturn(1L).anyTimes();
        EasyMock.replay(kafkaRoutineLoadTask);
        routineLoadTaskList.add(kafkaRoutineLoadTask);

        KafkaRoutineLoadJob routineLoadJob = EasyMock.createNiceMock(KafkaRoutineLoadJob.class);
        EasyMock.expect(routineLoadJob.calculateCurrentConcurrentTaskNum()).andReturn(taskNum).anyTimes();
        EasyMock.expect(routineLoadJob.divideRoutineLoadJob(taskNum)).andReturn(routineLoadTaskList).anyTimes();
        EasyMock.expect(routineLoadJob.getState()).andReturn(RoutineLoadJob.JobState.NEED_SCHEDULER).anyTimes();
        EasyMock.replay(routineLoadJob);

        SystemInfoService systemInfoService = EasyMock.createNiceMock(SystemInfoService.class);
        List<Long> beIds = Arrays.asList(1L, 2L, 3L);
        EasyMock.expect(systemInfoService.getBackendIds(true)).andReturn(beIds).anyTimes();
        EasyMock.replay(systemInfoService);

        Catalog catalog = EasyMock.createNiceMock(Catalog.class);
        EditLog editLog = EasyMock.createNiceMock(EditLog.class);
        PowerMock.mockStatic(Catalog.class);
        EasyMock.expect(Catalog.getCurrentSystemInfo()).andReturn(systemInfoService).anyTimes();
        EasyMock.expect(Catalog.getInstance()).andReturn(catalog).anyTimes();
        PowerMock.replay(Catalog.class);


        RoutineLoad routineLoad = new RoutineLoad();
        EasyMock.expect(catalog.getEditLog()).andReturn(editLog).anyTimes();
        EasyMock.expect(catalog.getRoutineLoadInstance()).andReturn(routineLoad).anyTimes();
        EasyMock.replay(catalog);

        routineLoad.addRoutineLoadJob(routineLoadJob);
        routineLoad.updateRoutineLoadJobState(routineLoadJob, RoutineLoadJob.JobState.NEED_SCHEDULER);

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();
        routineLoadScheduler.runOneCycle();

        Assert.assertEquals(1, routineLoad.getIdToRoutineLoadTask().size());
        Assert.assertEquals(1, routineLoad.getIdToNeedSchedulerRoutineLoadTasks().size());
        Assert.assertEquals(1, routineLoad.getRoutineLoadJobByState(RoutineLoadJob.JobState.RUNNING).size());
        Assert.assertEquals(0, routineLoad.getRoutineLoadJobByState(RoutineLoadJob.JobState.NEED_SCHEDULER).size());

    }
}
