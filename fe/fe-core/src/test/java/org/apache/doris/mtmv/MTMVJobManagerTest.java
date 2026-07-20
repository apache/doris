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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.job.extensions.mtmv.MTMVJob;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.job.manager.JobManager;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class MTMVJobManagerTest {

    @Test
    public void testRefreshMTMVPassesCurrentComputeGroupToTaskContext() throws Exception {
        String originCloudUniqueId = Config.cloud_unique_id;
        ConnectContext previousContext = ConnectContext.get();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Config.cloud_unique_id = "test_cloud";
            Env env = Mockito.mock(Env.class);
            InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
            Database db = Mockito.mock(Database.class);
            MTMV mtmv = Mockito.mock(MTMV.class);
            MTMVJob job = Mockito.mock(MTMVJob.class);
            JobManager jobManager = Mockito.mock(JobManager.class);

            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(internalCatalog);
            Mockito.when(internalCatalog.getDbOrDdlException("db1")).thenReturn(db);
            Mockito.when(db.getTableOrMetaException(Mockito.eq("mv1"), Mockito.eq(TableType.MATERIALIZED_VIEW)))
                    .thenReturn(mtmv);
            Mockito.when(env.getJobManager()).thenReturn(jobManager);
            Mockito.when(jobManager.getJob(mtmv.getId())).thenReturn(job);
            Mockito.when(job.getJobId()).thenReturn(100L);

            ConnectContext ctx = new ConnectContext();
            ctx.setCloudCluster("cg1");
            ctx.setThreadLocalInfo();

            RefreshMTMVInfo info = new RefreshMTMVInfo(new TableNameInfo("db1", "mv1"),
                    Lists.newArrayList("p1"), false);
            new MTMVJobManager().refreshMTMV(info);

            ArgumentCaptor<MTMVTaskContext> captor = ArgumentCaptor.forClass(MTMVTaskContext.class);
            Mockito.verify(jobManager).triggerJob(Mockito.eq(100L), captor.capture());
            MTMVTaskContext taskContext = captor.getValue();
            Assert.assertEquals(MTMVTaskTriggerMode.MANUAL, taskContext.getTriggerMode());
            Assert.assertEquals(Lists.newArrayList("p1"), taskContext.getPartitions());
            Assert.assertFalse(taskContext.isComplete());
            Assert.assertEquals("cg1", taskContext.getComputeGroup());
        } finally {
            Config.cloud_unique_id = originCloudUniqueId;
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }
}
