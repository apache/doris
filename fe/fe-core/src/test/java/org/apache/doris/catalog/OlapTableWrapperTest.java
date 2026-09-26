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

import org.apache.doris.binlog.BinlogTestUtils;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class OlapTableWrapperTest {

    private static OlapTable newTestTable(BinlogConfig binlogConfig) {
        long baseIndexId = 1L;
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v1", PrimitiveType.INT);
        value.setIsKey(false);
        List<Column> baseSchema = Lists.newArrayList(key, value);

        // Use non-null partition/distribution info so wrapper delegation can be asserted meaningfully.
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.PRIMARY_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(32));
        table.setBaseIndexId(baseIndexId);
        MaterializedIndexMeta baseIndexMeta = new MaterializedIndexMeta(baseIndexId, baseSchema, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.PRIMARY_KEYS, null);
        table.addIndexIdToMetaForUnitTest(baseIndexId, baseIndexMeta);
        table.addIndexNameToIdForUnitTest("base", baseIndexId);
        table.setBinlogConfig(binlogConfig);

        if (binlogConfig.isEnableForStreaming()) {
            long rowBinlogIndexId = 2L;
            List<Column> rowBinlogSchema = table.generateTableRowBinlogSchema();
            MaterializedIndexMeta rowBinlogMeta = new MaterializedIndexMeta(rowBinlogIndexId, rowBinlogSchema, 1, 1,
                    (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS, null, null, null, null);
            rowBinlogMeta.initSchemaColumnUniqueId();
            table.setRowBinlogMeta(rowBinlogMeta, "row_binlog");
        }
        return table;
    }

    @Test
    public void testOlapTableWrapper() {
        OlapTable table = newTestTable(BinlogTestUtils.newTestRowBinlogConfig(false, false));
        OlapTableWrapper wrapper = new OlapTableWrapper(table);

        Assertions.assertEquals(table, wrapper.getOriginTable());

        // base index id & schema delegation
        Assertions.assertEquals(table.getBaseIndexId(), wrapper.getBaseIndexId());
        Assertions.assertEquals(table.getIndexNameById(table.getBaseIndexId()), wrapper.getIndexNameById(table.getBaseIndexId()));
        Assertions.assertEquals(table.getIndexMetaByIndexId(table.getBaseIndexId()),
                wrapper.getIndexMetaByIndexId(table.getBaseIndexId()));
        Assertions.assertEquals(table.getSchemaByIndexId(table.getBaseIndexId()), wrapper.getSchemaByIndexId(table.getBaseIndexId()));
        Assertions.assertEquals(table.getIndexSchemaVersion(table.getBaseIndexId()),
                wrapper.getIndexSchemaVersion(table.getBaseIndexId()));

        // lock delegation should not throw
        wrapper.readLock();
        try {
            Assertions.assertNotNull(table.getPartitionInfo());
            Assertions.assertEquals(table.getPartitionInfo(), wrapper.getPartitionInfo());
        } finally {
            wrapper.readUnlock();
        }

        // tryReadLock delegation should not throw
        boolean locked = wrapper.tryReadLock(1, TimeUnit.SECONDS);
        if (locked) {
            wrapper.readUnlock();
        }
    }

    @Test
    public void testRowBinlogTableWrapper() {
        BinlogConfig binlogConfig = BinlogTestUtils.newTestRowBinlogConfig(true, true);
        binlogConfig.setTtlSeconds(10);
        OlapTable table = newTestTable(binlogConfig);
        Assertions.assertTrue(table.needRowBinlog());
        MaterializedIndexMeta rowBinlogMeta = table.getRowBinlogMeta();
        Assertions.assertNotNull(rowBinlogMeta);

        RowBinlogTableWrapper wrapper = new RowBinlogTableWrapper(table);

        // test getOriginTable
        Assertions.assertEquals(table, wrapper.getOriginTable());

        // test wrapped baseIndexId
        Assertions.assertEquals(rowBinlogMeta.getIndexId(), wrapper.getBaseIndexId());

        // test index name & id delegations
        Assertions.assertEquals(table.getIndexNameById(table.getBaseIndexId()), wrapper.getIndexNameById(table.getBaseIndexId()));
        Assertions.assertEquals(table.getIndexMetaByIndexId(table.getBaseIndexId()), wrapper.getIndexMetaByIndexId(table.getBaseIndexId()));

        // row binlog index meta should also be reachable through delegation
        Assertions.assertEquals(table.getIndexMetaByIndexId(rowBinlogMeta.getIndexId()),
                wrapper.getIndexMetaByIndexId(rowBinlogMeta.getIndexId()));

        // test schema delegation
        Assertions.assertEquals(table.getSchemaByIndexId(table.getBaseIndexId()), wrapper.getSchemaByIndexId(table.getBaseIndexId()));
        Assertions.assertEquals(table.getIndexSchemaVersion(table.getBaseIndexId()), wrapper.getIndexSchemaVersion(table.getBaseIndexId()));
    }

    @Test
    public void testCloudRowBinlogWrapperRefreshesCachedEmptyPartitions() {
        long stalePartitionId = 100L;
        long cachedNonEmptyPartitionId = 101L;
        List<Long> partitionIds = ImmutableList.of(stalePartitionId, cachedNonEmptyPartitionId);

        CloudPartition stalePartition = Mockito.mock(CloudPartition.class);
        Mockito.when(stalePartition.getId()).thenReturn(stalePartitionId);
        // Simulate another query refreshing the shared cache after the first cached-state check.
        Mockito.when(stalePartition.hasDataCached()).thenReturn(false, true);
        CloudPartition cachedNonEmptyPartition = Mockito.mock(CloudPartition.class);
        Mockito.when(cachedNonEmptyPartition.getId()).thenReturn(cachedNonEmptyPartitionId);
        Mockito.when(cachedNonEmptyPartition.hasDataCached()).thenReturn(true);

        OlapTable table = Mockito.spy(newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, true)));
        Mockito.doReturn(stalePartition).when(table).getPartition(stalePartitionId);
        Mockito.doReturn(cachedNonEmptyPartition).when(table).getPartition(cachedNonEmptyPartitionId);
        RowBinlogTableWrapper wrapper = new RowBinlogTableWrapper(table);

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(
                        CloudPartition.class, Mockito.CALLS_REAL_METHODS)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);
            mockedPartition.when(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    ImmutableList.of(stalePartition), false)).thenReturn(ImmutableList.of(2L));
            mockedPartition.clearInvocations();

            Assertions.assertEquals(ImmutableList.of(cachedNonEmptyPartitionId, stalePartitionId),
                    wrapper.selectNonEmptyPartitionIds(partitionIds, Optional.empty()));
            mockedPartition.verify(() -> CloudPartition.getSnapshotVisibleVersionFromMs(
                    ImmutableList.of(stalePartition), false));
            Mockito.verify(stalePartition).hasDataCached();
            Mockito.verify(cachedNonEmptyPartition).hasDataCached();
        }
    }

    @Test
    public void testCloudRowBinlogWrapperWithFixedVisibleVersionsUsesOriginTable() {
        long emptyPartitionId = 100L;
        long nonEmptyPartitionId = 101L;
        List<Long> partitionIds = ImmutableList.of(emptyPartitionId, nonEmptyPartitionId);

        OlapTable table = Mockito.spy(newTestTable(BinlogTestUtils.newTestRowBinlogConfig(true, true)));
        Mockito.doReturn(ImmutableList.of(nonEmptyPartitionId)).when(table)
                .selectNonEmptyPartitionIds(partitionIds, Optional.empty());
        RowBinlogTableWrapper wrapper = new RowBinlogTableWrapper(table, Collections.emptyMap(),
                ImmutableMap.of(emptyPartitionId, Partition.PARTITION_INIT_VERSION, nonEmptyPartitionId, 2L));

        try (MockedStatic<Config> mockedConfig = Mockito.mockStatic(Config.class);
                MockedStatic<CloudPartition> mockedPartition = Mockito.mockStatic(CloudPartition.class)) {
            mockedConfig.when(Config::isCloudMode).thenReturn(true);

            Assertions.assertEquals(ImmutableList.of(nonEmptyPartitionId),
                    wrapper.selectNonEmptyPartitionIds(partitionIds, Optional.empty()));
            Mockito.verify(table).selectNonEmptyPartitionIds(partitionIds, Optional.empty());
            mockedPartition.verifyNoInteractions();
        }
    }
}
