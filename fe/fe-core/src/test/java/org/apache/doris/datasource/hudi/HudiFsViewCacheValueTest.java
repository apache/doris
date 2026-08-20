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

        Mockito.verify(view, Mockito.never()).close();
        HudiFsViewCacheValue.Lease lease = value.tryAcquire();
        Assert.assertNotNull(lease);
        Mockito.verify(view).sync();
        lease.close();
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
}
