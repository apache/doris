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

import org.apache.doris.clone.TabletScheduler.PathSlot;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

class PathSlotTest {

    @Test
    public void test() {
        Config.balance_slot_num_per_path = 2;
        Map<Long, TStorageMedium> paths = Maps.newHashMap();
        List<Long> availPathHashs = Lists.newArrayList();
        List<Long> expectPathHashs = Lists.newArrayList();
        List<Long> gotPathHashs = Lists.newArrayList();
        TStorageMedium medium = TStorageMedium.HDD;
        long startPath = 10001L;
        long endPath = 10006L;
        for (long pathHash = startPath; pathHash < endPath; pathHash++) {
            paths.put(pathHash, medium);
            availPathHashs.add(pathHash);
            expectPathHashs.add(pathHash);
        }
        for (long pathHash = startPath; pathHash < endPath; pathHash++) {
            expectPathHashs.add(pathHash);
        }
        for (long pathHash = startPath; pathHash < endPath; pathHash++) {
            expectPathHashs.add(-1L);
        }

        PathSlot ps = new PathSlot(paths, 1L);
        for (int i = 0; i < expectPathHashs.size(); i++) {
            Collections.shuffle(availPathHashs);
            gotPathHashs.add(ps.takeAnAvailBalanceSlotFrom(availPathHashs, medium));
        }
        Assert.assertEquals(expectPathHashs, gotPathHashs);
    }

}
