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

package org.apache.doris.clone;

import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class RootPathLoadStatisticTest {

    @Test
    public void test() {
        RootPathLoadStatistic usageLow = new RootPathLoadStatistic(0L, "/home/disk1", 12345L, TStorageMedium.HDD, 4096L,
                1024L, DiskState.ONLINE);
        RootPathLoadStatistic usageHigh = new RootPathLoadStatistic(0L, "/home/disk2", 67890L, TStorageMedium.HDD,
                4096L, 2048L, DiskState.ONLINE);

        List<RootPathLoadStatistic> list = Lists.newArrayList();
        list.add(usageLow);
        list.add(usageHigh);

        // low usage should be ahead
        Collections.sort(list);
        Assert.assertTrue(list.get(0).getPathHash() == usageLow.getPathHash());
    }

}
