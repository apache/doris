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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.property.metastore.IcebergRestProperties;

import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

class IcebergCatalogResourceTrackerTest {

    @Test
    void restClientCloseWaitsForRetainedCatalogGeneration() throws Exception {
        IcebergRestExternalCatalog catalog = new IcebergRestExternalCatalog(
                1L, "rest", null, Collections.emptyMap(), "");
        IcebergRestProperties properties = Mockito.mock(IcebergRestProperties.class);
        RESTSessionCatalog retiredClient = Mockito.mock(RESTSessionCatalog.class);
        RESTSessionCatalog replacementClient = Mockito.mock(RESTSessionCatalog.class);
        Mockito.when(properties.getRestSessionCatalog()).thenReturn(retiredClient);
        Deencapsulation.setField(catalog, "msProperties", properties);
        IcebergCatalogResourceTracker tracker = Deencapsulation.getField(catalog, "resourceTracker");
        IcebergCatalogResourceTracker.ResourceLease lease;
        try (IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad()) {
            lease = guard.promote();
        }
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            catalog.onClose();
            Mockito.verify(retiredClient, Mockito.never()).close();
            Mockito.when(properties.getRestSessionCatalog()).thenReturn(replacementClient);
            lease.close();
            Mockito.verify(retiredClient).close();
            Mockito.verify(replacementClient, Mockito.never()).close();
            lease.close();
            Mockito.verify(retiredClient).close();
        }
    }

    @Test
    void catalogRetirementWaitsForLoadedTableOwner() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        IcebergCatalogResourceTracker.ResourceLease lease = guard.promote();
        guard.close();
        AtomicInteger closeCalls = new AtomicInteger();

        tracker.retireCurrent(closeCalls::incrementAndGet);
        Assertions.assertEquals(0, closeCalls.get());

        lease.close();
        Assertions.assertEquals(1, closeCalls.get());
        lease.close();
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    void failedLoadReleasesRetiredGeneration() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        AtomicInteger closeCalls = new AtomicInteger();

        tracker.retireCurrent(closeCalls::incrementAndGet);
        Assertions.assertEquals(0, closeCalls.get());

        guard.close();
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    void consecutiveCatalogGenerationsRetireIndependently() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard first = tracker.beginLoad();
        IcebergCatalogResourceTracker.ResourceLease firstLease = first.promote();
        first.close();
        AtomicInteger firstCloseCalls = new AtomicInteger();
        tracker.retireCurrent(firstCloseCalls::incrementAndGet);

        IcebergCatalogResourceTracker.LoadGuard second = tracker.beginLoad();
        IcebergCatalogResourceTracker.ResourceLease secondLease = second.promote();
        second.close();
        AtomicInteger secondCloseCalls = new AtomicInteger();
        tracker.retireCurrent(secondCloseCalls::incrementAndGet);

        secondLease.close();
        Assertions.assertEquals(0, firstCloseCalls.get());
        Assertions.assertEquals(1, secondCloseCalls.get());

        firstLease.close();
        Assertions.assertEquals(1, firstCloseCalls.get());
    }
}
