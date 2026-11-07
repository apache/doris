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

import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class CloudContextLifecycleTest {
    @Test
    public void testWarmUpClosesOwnedContextAndRestoresCaller() {
        boolean previousUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent);
                MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            config.when(Config::isCloudMode).thenReturn(true);
            CloudWarmUpJob job = new CloudWarmUpJob(1L, "source", "destination", Collections.emptyMap(),
                    CloudWarmUpJob.JobType.CLUSTER);
            job.setJobState(CloudWarmUpJob.JobState.FINISHED);
            job.run();
            Assertions.assertEquals(1, contexts.constructed().size());
            Mockito.verify(contexts.constructed().get(0))
                    .requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        } finally {
            FeConstants.runningUnitTest = previousUnitTest;
        }
    }

    @Test
    public void testClusterLookupClosesPrivateContextOnEarlyReturnAndFailure() {
        ConnectContext.remove();
        for (boolean fail : new boolean[] {false, true}) {
            try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class,
                    (context, construction) -> {
                        if (fail) {
                            Mockito.when(context.getCloudClusterByPolicy())
                                    .thenThrow(new IllegalStateException("cluster lookup failed"));
                        }
                    })) {
                CloudSystemInfoService service = new CloudSystemInfoService();
                if (fail) {
                    Assertions.assertThrows(IllegalStateException.class, () -> service.getClusterNameAutoStart(null));
                } else {
                    Assertions.assertNull(service.getClusterNameAutoStart(null));
                }
                Assertions.assertEquals(1, contexts.constructed().size());
                Mockito.verify(contexts.constructed().get(0))
                        .requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
                Assertions.assertNull(ConnectContext.get());
            }
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
