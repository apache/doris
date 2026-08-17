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

package org.apache.doris.transaction;

import org.apache.doris.thrift.TTabletLocation;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AutoPartitionCacheManagerTest {
    @Test
    public void testGetOrSetAutoPartitionInfoReturnsCachedLoadTabletIdx() {
        AutoPartitionCacheManager cacheManager = new AutoPartitionCacheManager();
        List<TTabletLocation> firstTablets = new ArrayList<>();
        firstTablets.add(new TTabletLocation(10001L, Arrays.asList(1L)));
        List<TTabletLocation> firstSlaveTablets = new ArrayList<>();

        long storedLoadTabletIdx = cacheManager.getOrSetAutoPartitionInfo(
                10L, 20L, firstTablets, firstSlaveTablets, 3);
        Assert.assertEquals(3, storedLoadTabletIdx);

        List<TTabletLocation> secondTablets = new ArrayList<>();
        secondTablets.add(new TTabletLocation(20001L, Arrays.asList(2L)));
        List<TTabletLocation> secondSlaveTablets = new ArrayList<>();

        long cachedLoadTabletIdx = cacheManager.getOrSetAutoPartitionInfo(
                10L, 20L, secondTablets, secondSlaveTablets, 5);
        Assert.assertEquals(3, cachedLoadTabletIdx);
        Assert.assertEquals(1, secondTablets.size());
        Assert.assertEquals(10001L, secondTablets.get(0).getTabletId());

        List<TTabletLocation> cachedTablets = new ArrayList<>();
        List<TTabletLocation> cachedSlaveTablets = new ArrayList<>();
        AtomicLong readLoadTabletIdx = new AtomicLong(-1);
        Assert.assertTrue(cacheManager.getAutoPartitionInfo(
                10L, 20L, cachedTablets, cachedSlaveTablets, readLoadTabletIdx));
        Assert.assertEquals(3, readLoadTabletIdx.get());
        Assert.assertEquals(1, cachedTablets.size());
        Assert.assertEquals(10001L, cachedTablets.get(0).getTabletId());
    }
}
