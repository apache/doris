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

package org.apache.doris.service;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cloud.CacheHotspotManager;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionLikeOp;
import org.apache.doris.thrift.TAddOrDropPartitionsRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosRequest;
import org.apache.doris.thrift.TGetTabletReplicaInfosResult;
import org.apache.doris.thrift.TStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Collections;

public class FrontendServiceImplCloudTest {

    // Regression test for FrontendServiceImpl.getTabletReplicaInfos NPE:
    // When a warm-up job has been removed from
    // CacheHotspotManager.cloudWarmUpJobs (past
    // history_cloud_warm_up_job_keep_max_second), getCloudWarmUpJob
    // returns null. The previous code called job.getJobId() inside the
    // log message, throwing NPE which bubbled up to BE as
    // "Internal error processing getTabletReplicaInfos".
    @Test
    public void testGetTabletReplicaInfosNullJobReturnsCancelledWithoutNpe() {
        String originalCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "gettabletreplicainfostest";

        CloudEnv cloudEnv = Mockito.mock(CloudEnv.class);
        CacheHotspotManager cacheHotspotManager = Mockito.mock(CacheHotspotManager.class);
        Mockito.when(cloudEnv.getCacheHotspotMgr()).thenReturn(cacheHotspotManager);
        // Simulate job already removed from cloudWarmUpJobs.
        Mockito.when(cacheHotspotManager.getCloudWarmUpJob(123456L)).thenReturn(null);

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(cloudEnv);

            FrontendServiceImpl frontendService = new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class));
            TGetTabletReplicaInfosRequest request = new TGetTabletReplicaInfosRequest();
            request.setTabletIds(Collections.singletonList(789L));
            request.setWarmUpJobId(123456L);

            TGetTabletReplicaInfosResult result;
            try {
                result = frontendService.getTabletReplicaInfos(request);
            } catch (NullPointerException e) {
                throw new AssertionError("getTabletReplicaInfos must not NPE when the "
                        + "warm-up job has been removed from CacheHotspotManager", e);
            }

            Assert.assertNotNull("result.status must be set", result.getStatus());
            Assert.assertEquals("BE must be told to cancel its stale warm-up job entry",
                    TStatusCode.CANCELLED, result.getStatus().getStatusCode());
        } finally {
            Config.cloud_unique_id = originalCloudUniqueId;
        }
    }

    @Test
    public void testAddPartitionForRemoteInsertOverwriteUsesDefaultSchema() throws Exception {
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(catalog.getDbNullable("db")).thenReturn(database);
        Mockito.when(database.getTableNullable("table")).thenReturn(table);
        Mockito.when(table.writeLockIfExist()).thenReturn(true);
        Mockito.when(table.getName()).thenReturn("table");

        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);

            invokeAddOrDropPartitions(newAddPartitionRequest());

            Mockito.verify(env).addPartitionLike(Mockito.same(database), Mockito.eq("table"),
                    Mockito.any(AddPartitionLikeOp.class));
            Mockito.verify(table).writeUnlock();
        }
    }

    private TAddOrDropPartitionsRequest newAddPartitionRequest() {
        TAddOrDropPartitionsRequest request = new TAddOrDropPartitionsRequest();
        request.setDb("db");
        request.setTbl("table");
        request.setPartitionNames(Collections.singletonList("source_partition"));
        request.setTempPartitionNames(Collections.singletonList("temp_partition"));
        request.setIsDrop(false);
        request.setIsTemp(true);
        return request;
    }

    private void invokeAddOrDropPartitions(TAddOrDropPartitionsRequest request) throws Exception {
        Method method = FrontendServiceImpl.class.getDeclaredMethod("addOrDropPartitionsImpl",
                TAddOrDropPartitionsRequest.class);
        method.setAccessible(true);
        method.invoke(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)), request);
    }
}
