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

import org.apache.doris.catalog.DiskInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MetaIdGenerator.IdGeneratorBuffer;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Replica.ReplicaState;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.resource.Tag;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RoundRobinCreateTabletTest {
    private Backend backend1;
    private Backend backend2;
    private Backend backend3;
    private Backend backend4;

    @Before
    public void setUp() {
        backend1 = new Backend(1L, "192.168.1.1", 9050);
        backend2 = new Backend(2L, "192.168.1.2", 9050);
        backend3 = new Backend(3L, "192.168.1.3", 9050);
        backend4 = new Backend(4L, "192.168.1.4", 9050);

        DiskInfo diskInfo1 = new DiskInfo("/disk1");
        ImmutableMap<String, DiskInfo> diskRefs = ImmutableMap.of("disk1", diskInfo1);
        backend1.setDisks(diskRefs);
        backend2.setDisks(diskRefs);
        backend3.setDisks(diskRefs);
        backend4.setDisks(diskRefs);

        backend1.setAlive(true);
        backend2.setAlive(true);
        backend3.setAlive(true);
        backend4.setAlive(true);

        Map<String, String> tagMap = new HashMap<>();
        tagMap.put(Tag.TYPE_LOCATION, Tag.VALUE_DEFAULT_TAG);

        backend1.setTagMap(tagMap);
        backend2.setTagMap(tagMap);
        backend3.setTagMap(tagMap);
        backend4.setTagMap(tagMap);

        Env.getCurrentSystemInfo().addBackend(backend1);
        Env.getCurrentSystemInfo().addBackend(backend2);
        Env.getCurrentSystemInfo().addBackend(backend3);
        Env.getCurrentSystemInfo().addBackend(backend4);
    }

    @After
    public void tearDown() {
        Config.enable_round_robin_create_tablet = true;
        Config.disable_storage_medium_check = true;

        try {
            Env.getCurrentSystemInfo().dropBackend(1L);
            Env.getCurrentSystemInfo().dropBackend(2L);
            Env.getCurrentSystemInfo().dropBackend(3L);
            Env.getCurrentSystemInfo().dropBackend(4L);
        } catch (Exception e) {
            System.out.println("failed to drop backend " + e.getMessage());
        }
    }

    @Test
    public void testCreateTablets() {
        MaterializedIndex index = new MaterializedIndex();
        HashDistributionInfo distributionInfo = new HashDistributionInfo(48, null);
        ReplicaAllocation replicaAlloc = new ReplicaAllocation((short) 3);
        TabletMeta tabletMeta = new TabletMeta(1L, 2L, 3L, 4L, 5, TStorageMedium.HDD);
        IdGeneratorBuffer idGeneratorBuffer = Env.getCurrentEnv().getIdGeneratorBuffer(1000);
        Set<Long> tabletIdSet = new HashSet<>();

        Config.enable_round_robin_create_tablet = true;
        Config.disable_storage_medium_check = true;

        try {
            Env.getCurrentEnv().getInternalCatalog().createTablets(index, ReplicaState.NORMAL,
                    distributionInfo, 0, replicaAlloc, tabletMeta,
                    tabletIdSet, idGeneratorBuffer, false);
        } catch (Exception e) {
            System.out.println("failed to create tablets " + e.getMessage());
        }

        int i = 0;
        int beNum = 4;
        for (Tablet tablet : index.getTablets()) {
            for (Replica replica : tablet.getReplicas()) {
                Assert.assertEquals((i++ % beNum) + 1, replica.getBackendId());
            }
        }
    }
}
