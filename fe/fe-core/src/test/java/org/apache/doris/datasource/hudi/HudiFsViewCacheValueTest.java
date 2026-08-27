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

package org.apache.doris.datasource.hudi;

import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HudiFsViewCacheValueTest {

    @Test
    public void testEvictionClosesAfterExactLeaseRelease() {
        HoodieTableFileSystemView view = Mockito.mock(HoodieTableFileSystemView.class);
        HudiFsViewCacheValue value = new HudiFsViewCacheValue(view);
        HudiFsViewCacheValue.Lease lease = value.tryAcquire();

        Assert.assertNotNull(lease);
        Assert.assertSame(view, lease.get());
        Mockito.verify(view).sync();
        value.evict();
        Mockito.verify(view, Mockito.never()).close();
        Assert.assertNull(value.tryAcquire());

        lease.close();
        Mockito.verify(view).close();
        lease.close();
        Mockito.verify(view, Mockito.times(1)).close();
    }

    @Test
    public void testEvictionBeforeLoaderReferenceHandoff() {
        HoodieTableFileSystemView view = Mockito.mock(HoodieTableFileSystemView.class);
        HudiFsViewCacheValue value = new HudiFsViewCacheValue(view);

        value.evict();

        Mockito.verify(view).close();
        Assert.assertNull(value.tryAcquire());
    }

    @Test
    public void testLeaseSynchronizesHotCachedView() {
        HoodieTableFileSystemView view = Mockito.mock(HoodieTableFileSystemView.class);
        HudiFsViewCacheValue value = new HudiFsViewCacheValue(view);
        HudiFsViewCacheValue.Lease firstLease = value.tryAcquire();

        Assert.assertNotNull(firstLease);
        firstLease.close();
        HudiFsViewCacheValue.Lease secondLease = value.tryAcquire();
        Assert.assertNotNull(secondLease);
        secondLease.close();

        Mockito.verify(view, Mockito.times(2)).sync();
    }

    @Test
    public void testEvictionDoesNotWaitForBlockedSync() throws Exception {
        HoodieTableFileSystemView view = Mockito.mock(HoodieTableFileSystemView.class);
        HudiFsViewCacheValue value = new HudiFsViewCacheValue(view);
        CountDownLatch syncStarted = new CountDownLatch(1);
        CountDownLatch allowSync = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            syncStarted.countDown();
            Assert.assertTrue(allowSync.await(3L, TimeUnit.SECONDS));
            return null;
        }).when(view).sync();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<HudiFsViewCacheValue.Lease> acquisition = executor.submit(value::tryAcquire);
            Assert.assertTrue(syncStarted.await(3L, TimeUnit.SECONDS));

            Future<?> eviction = executor.submit(value::evict);
            eviction.get(3L, TimeUnit.SECONDS);
            Mockito.verify(view, Mockito.never()).close();

            allowSync.countDown();
            HudiFsViewCacheValue.Lease lease = acquisition.get(3L, TimeUnit.SECONDS);
            Mockito.verify(view, Mockito.never()).close();
            lease.close();
            Mockito.verify(view).close();
        } finally {
            allowSync.countDown();
            executor.shutdownNow();
        }
    }
}
