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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        long storedLoadTabletIdx = cacheManager.getOrSetAutoPartitionInfo(
                10L, 20L, firstTablets, 3);
        Assertions.assertEquals(3, storedLoadTabletIdx);

        List<TTabletLocation> secondTablets = new ArrayList<>();
        secondTablets.add(new TTabletLocation(20001L, Arrays.asList(2L)));
        long cachedLoadTabletIdx = cacheManager.getOrSetAutoPartitionInfo(
                10L, 20L, secondTablets, 5);
        Assertions.assertEquals(3, cachedLoadTabletIdx);
        Assertions.assertEquals(1, secondTablets.size());
        Assertions.assertEquals(10001L, secondTablets.get(0).getTabletId());

        List<TTabletLocation> cachedTablets = new ArrayList<>();
        AtomicLong readLoadTabletIdx = new AtomicLong(-1);
        Assertions.assertTrue(cacheManager.getAutoPartitionInfo(
                10L, 20L, cachedTablets, readLoadTabletIdx));
        Assertions.assertEquals(3, readLoadTabletIdx.get());
        Assertions.assertEquals(1, cachedTablets.size());
        Assertions.assertEquals(10001L, cachedTablets.get(0).getTabletId());
    }
}
