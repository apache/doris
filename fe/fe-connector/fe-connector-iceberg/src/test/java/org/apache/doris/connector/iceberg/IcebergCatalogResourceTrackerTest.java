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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class IcebergCatalogResourceTrackerTest {

    @Test
    void connectorCloseWaitsForLoadedTableOwner() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        IcebergCatalogResourceTracker.ResourceLease lease = guard.promote();
        guard.close();
        AtomicInteger closeCalls = new AtomicInteger();

        tracker.close(closeCalls::incrementAndGet);
        Assertions.assertEquals(0, closeCalls.get());

        lease.close();
        Assertions.assertEquals(1, closeCalls.get());
        lease.close();
        Assertions.assertEquals(1, closeCalls.get());
    }

    @Test
    void loadSpanningRestReplacementPinsBothGenerations() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        AtomicInteger oldCloseCalls = new AtomicInteger();
        AtomicInteger currentCloseCalls = new AtomicInteger();
        AtomicInteger publications = new AtomicInteger();

        tracker.rotate(oldCloseCalls::incrementAndGet, publications::incrementAndGet);
        IcebergCatalogResourceTracker.ResourceLease lease = guard.promote();
        guard.close();
        tracker.close(currentCloseCalls::incrementAndGet);

        Assertions.assertEquals(1, publications.get());
        Assertions.assertEquals(0, oldCloseCalls.get());
        Assertions.assertEquals(0, currentCloseCalls.get());

        lease.close();
        Assertions.assertEquals(1, oldCloseCalls.get());
        Assertions.assertEquals(1, currentCloseCalls.get());
    }

    @Test
    void failedLoadReleasesEveryGenerationItCrossed() {
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        IcebergCatalogResourceTracker.LoadGuard guard = tracker.beginLoad();
        AtomicInteger oldCloseCalls = new AtomicInteger();
        AtomicInteger currentCloseCalls = new AtomicInteger();

        tracker.rotate(oldCloseCalls::incrementAndGet, () -> { });
        guard.close();
        tracker.close(currentCloseCalls::incrementAndGet);

        Assertions.assertEquals(1, oldCloseCalls.get());
        Assertions.assertEquals(1, currentCloseCalls.get());
    }
}
