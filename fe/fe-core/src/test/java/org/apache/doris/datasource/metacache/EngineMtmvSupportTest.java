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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HiveEngineCache;
import org.apache.doris.datasource.hudi.source.HudiEngineCache;
import org.apache.doris.mtmv.MTMVMaxTimestampSnapshot;
import org.apache.doris.mtmv.MTMVSnapshotIf;
import org.apache.doris.mtmv.MTMVTimestampSnapshot;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class EngineMtmvSupportTest {

    @Test
    public void testGetTableSnapshotForHive() throws Exception {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        UnifiedMetaCacheMgr unifiedMetaCacheMgr = Mockito.mock(UnifiedMetaCacheMgr.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HiveEngineCache hiveEngineCache = Mockito.mock(HiveEngineCache.class);

        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        Mockito.when(metaCacheMgr.getUnifiedMetaCacheMgr()).thenReturn(unifiedMetaCacheMgr);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(unifiedMetaCacheMgr.getOrCreateEngineMetaCache(table)).thenReturn(hiveEngineCache);
        Mockito.when(hiveEngineCache.getSnapshot(table, Optional.empty()))
                .thenReturn(new HiveEngineCache.HiveSnapshotMeta("p=1", 123L));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            MTMVSnapshotIf snapshot = EngineMtmvSupport.getTableSnapshot(table, Optional.empty());
            Assert.assertTrue(snapshot instanceof MTMVMaxTimestampSnapshot);
            Assert.assertEquals(123L, snapshot.getSnapshotVersion());
        }
    }

    @Test
    public void testGetPartitionSnapshotForHudiFallbackToTableTimestamp() throws Exception {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr metaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        UnifiedMetaCacheMgr unifiedMetaCacheMgr = Mockito.mock(UnifiedMetaCacheMgr.class);
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        HMSExternalTable table = Mockito.mock(HMSExternalTable.class);
        HudiEngineCache hudiEngineCache = Mockito.mock(HudiEngineCache.class);

        TablePartitionValues partitionValues = new TablePartitionValues();
        partitionValues.getPartitionNameToIdMap().put("p=1", 1L);
        partitionValues.getPartitionIdToNameMap().put(1L, "p=1");
        partitionValues.getIdToPartitionItem().put(1L, Mockito.mock(PartitionItem.class));
        partitionValues.getPartitionNameToLastModifiedMap().put("p=1", 0L);

        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(metaCacheMgr);
        Mockito.when(metaCacheMgr.getUnifiedMetaCacheMgr()).thenReturn(unifiedMetaCacheMgr);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getDlaType()).thenReturn(DLAType.HUDI);
        Mockito.when(unifiedMetaCacheMgr.getOrCreateEngineMetaCache(table)).thenReturn(hudiEngineCache);
        Mockito.when(hudiEngineCache.getPartitionInfo(table, Optional.empty()))
                .thenReturn(new HudiEngineCache.HudiPartition(partitionValues));
        Mockito.when(hudiEngineCache.getSnapshot(table, Optional.empty()))
                .thenReturn(new HudiEngineCache.HudiSnapshotMeta(456L));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            MTMVSnapshotIf snapshot = EngineMtmvSupport.getPartitionSnapshot(table, "p=1", Optional.empty());
            Assert.assertTrue(snapshot instanceof MTMVTimestampSnapshot);
            Assert.assertEquals(456L, snapshot.getSnapshotVersion());
        }
    }
}
