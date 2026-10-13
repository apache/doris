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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TTabletType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class PartitionInfoTest {
    private ListPartitionInfo partitionInfo;

    @BeforeEach
    public void setUp() {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionInfo = new ListPartitionInfo(Lists.newArrayList(k1));
    }

    private void addPartition(long partitionId, String value, boolean isTemp)
            throws AnalysisException, DdlException {
        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue(value)));
        SinglePartitionDesc desc = new SinglePartitionDesc(false, "p" + partitionId,
                PartitionKeyDesc.createIn(inValues), null);
        desc.analyze(1, null);
        partitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
    }

    @Test
    public void testDropPartitionRemovesAllPartitionScopedEntries() throws Exception {
        addPartition(1L, "1", false);
        partitionInfo.setTabletType(1L, TTabletType.TABLET_TYPE_DISK);
        Assertions.assertTrue(partitionInfo.idToStoragePolicy.containsKey(1L));
        Assertions.assertTrue(partitionInfo.idToTabletType.containsKey(1L));

        partitionInfo.dropPartition(1L);

        Assertions.assertTrue(partitionInfo.idToDataProperty.isEmpty());
        Assertions.assertTrue(partitionInfo.idToStoragePolicy.isEmpty());
        Assertions.assertTrue(partitionInfo.idToInvertedIndexFileStorageFormat.isEmpty());
        Assertions.assertTrue(partitionInfo.idToReplicaAllocation.isEmpty());
        Assertions.assertTrue(partitionInfo.idToInMemory.isEmpty());
        Assertions.assertTrue(partitionInfo.idToTabletType.isEmpty());
        Assertions.assertTrue(partitionInfo.idToItem.isEmpty());
    }

    @Test
    public void testRepeatedOverwriteDoesNotAccumulateStoragePolicy() throws Exception {
        long formalId = 1L;
        addPartition(formalId, "1", false);
        long nextId = 100L;
        for (int i = 0; i < 1000; i++) {
            long tempId = nextId++;
            addPartition(tempId, "1", true);
            partitionInfo.dropPartition(formalId);
            partitionInfo.moveFromTempToFormal(tempId);
            formalId = tempId;
        }

        Assertions.assertEquals(Sets.newHashSet(formalId), partitionInfo.idToStoragePolicy.keySet());
        Assertions.assertEquals(Sets.newHashSet(formalId), partitionInfo.idToDataProperty.keySet());
    }

    @Test
    public void testGsonPostProcessRemovesStaleStoragePolicy() throws Exception {
        addPartition(1L, "1", false);
        addPartition(2L, "2", false);
        partitionInfo.setStoragePolicy(2L, "policy_a");
        partitionInfo.idToStoragePolicy.put(1000L, "");
        partitionInfo.idToStoragePolicy.put(1001L, "policy_b");

        String json = GsonUtils.GSON.toJson(partitionInfo, PartitionInfo.class);
        PartitionInfo restored = GsonUtils.GSON.fromJson(json, PartitionInfo.class);

        Assertions.assertTrue(restored instanceof ListPartitionInfo);
        Assertions.assertEquals(Sets.newHashSet(1L, 2L), restored.idToStoragePolicy.keySet());
        Assertions.assertEquals("", restored.getStoragePolicy(1L));
        Assertions.assertEquals("policy_a", restored.getStoragePolicy(2L));
        Assertions.assertEquals(Sets.newHashSet(1L, 2L), restored.idToDataProperty.keySet());
    }
}
