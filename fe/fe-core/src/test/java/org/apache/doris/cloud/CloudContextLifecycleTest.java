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

package org.apache.doris.cloud;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.FeConstants;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class CloudContextLifecycleTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWarmUpDoesNotCreateContext(boolean hasCaller) {
        boolean previousUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        ConnectContext parent = hasCaller ? new ConnectContext() : null;
        CloudEnv env = Mockito.mock(CloudEnv.class);
        CacheHotspotManager manager = Mockito.mock(CacheHotspotManager.class);
        Mockito.when(env.getCacheHotspotMgr()).thenReturn(manager);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        Mockito.when(manager.tryRegisterRunningJob(Mockito.any())).thenReturn(true);
        ConnectContext.remove();
        try (AutoCloseConnectContext scope = hasCaller ? new AutoCloseConnectContext(parent) : null;
                MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(env);
            CloudWarmUpJob job = new CloudWarmUpJob(1L, "source", "destination", Collections.emptyMap(),
                    CloudWarmUpJob.JobType.CLUSTER);
            job.run();
            Assertions.assertEquals(CloudWarmUpJob.JobState.RUNNING, job.getJobState());
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(parent, ConnectContext.get());
            if (hasCaller) {
                Assertions.assertFalse(parent.isClosingOrClosed());
            }
        } finally {
            FeConstants.runningUnitTest = previousUnitTest;
        }
    }

    @Test
    public void testClusterLookupWithoutCallerDoesNotCreateContext() {
        ConnectContext.remove();
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            CloudSystemInfoService service = new CloudSystemInfoService();
            Assertions.assertNull(service.getClusterNameAutoStart(null));
            Assertions.assertEquals("cluster", service.getClusterNameAutoStart("cluster"));
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertNull(ConnectContext.get());
        }
    }

    @Test
    public void testClusterLookupDoesNotCloseBorrowedContext() {
        ConnectContext parent = Mockito.spy(new ConnectContext());
        Mockito.doReturn(new ConnectContext.CloudClusterResult("cluster",
                ConnectContext.CloudClusterResult.Comment.FOUND_BY_DEFAULT_CLUSTER))
                .when(parent).getCloudClusterByPolicy();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent)) {
            Assertions.assertEquals("cluster", new CloudSystemInfoService().getClusterNameAutoStart(null));
            Assertions.assertSame(parent, ConnectContext.get());
            Mockito.verify(parent, Mockito.never()).requestClose(Mockito.any());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }
}
