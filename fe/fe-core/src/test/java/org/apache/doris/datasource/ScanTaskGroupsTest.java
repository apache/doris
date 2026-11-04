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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.FairTaskExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

class ScanTaskGroupsTest {
    @Test
    void scansShareQueryAdmissionAndDeferredLeaseOutlivesStatementOwner() throws Exception {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr manager = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        try (FairTaskExecutor pool = new FairTaskExecutor(1, task -> new Thread(task));
                MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            Mockito.when(manager.getScanTaskExecutor(Mockito.anyBoolean())).thenReturn(pool);
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            ScanTaskGroups groups = new ScanTaskGroups();
            ScanTaskGroups.Lease first = groups.acquire();
            ScanTaskGroups.Lease flight = groups.acquire();
            FairTaskExecutor.Group split = first.group(false);
            Assertions.assertSame(split, flight.group(false));
            Assertions.assertNotSame(split, flight.group(true));
            first.close();
            groups.close();
            Assertions.assertFalse(split.isClosed());
            Assertions.assertThrows(CancellationException.class, groups::acquire);
            AtomicInteger cleanups = new AtomicInteger();
            flight.onClose(cleanups::incrementAndGet);
            flight.close();
            flight.close();
            Assertions.assertEquals(1, cleanups.get());
            flight.onClose(cleanups::incrementAndGet);
            Assertions.assertEquals(2, cleanups.get());
            Assertions.assertTrue(split.isClosed());
            Assertions.assertThrows(CancellationException.class, () -> flight.group(false));
        }
    }
}
