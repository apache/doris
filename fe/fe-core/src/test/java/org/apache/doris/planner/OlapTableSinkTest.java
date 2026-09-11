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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.doris.RemoteDorisExternalCatalog;
import org.apache.doris.datasource.doris.RemoteOlapTable;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.planner.OlapTableSink.AdaptiveBucketAssignment;
import org.apache.doris.planner.OlapTableSink.AdaptiveIndexBucketAssignment;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTableSchemaParam;
import org.apache.doris.thrift.TRowBinlogWriteColumnMapping;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletLocation;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OlapTableSinkTest {
    @Test
    public void testWriteSchemaCapturesTransactionSnapshot() throws Exception {
        checkWriteSchemaSnapshot(100L);
    }

    @Test
    public void testRoutineLoadPlansWithActualTransactionBeforeCapturingSnapshot() throws Exception {
        for (RoutineLoadJob job : Arrays.asList(new KafkaRoutineLoadJob(), new KinesisRoutineLoadJob())) {
            long planningTxnId = job.toNereidsRoutineLoadTaskInfo(100L).getTxnId();
            Assertions.assertDoesNotThrow(() -> checkWriteSchemaSnapshot(planningTxnId));
        }
    }

    private void checkWriteSchemaSnapshot(long planningTxnId) throws Exception {
        checkWriteSchemaSnapshot(planningTxnId, false);
    }

    @Test
    public void testRemoteWriteSchemaDoesNotCaptureInLocalTransaction() throws Exception {
        checkWriteSchemaSnapshot(100L, true);
    }

    private void checkWriteSchemaSnapshot(long planningTxnId, boolean remote) throws Exception {
        MaterializedIndexMeta source = indexMeta(10L, Arrays.asList(
                column("k1", 1, true, true), column("v1", 2, false, true)));
        source.setRowBinlogIndexId(20L);
        MaterializedIndexMeta binlog = indexMeta(20L, Arrays.asList(
                column("k1", 11, true, true), column("v1", 12, false, true),
                column(Column.generateBeforeColName("v1"), 22, false, true)));
        OlapTable table = remote ? Mockito.mock(RemoteOlapTable.class) : Mockito.mock(OlapTable.class);
        Mockito.when(table.needRowBinlog()).thenReturn(true);
        Mockito.when(table.getBaseIndexId()).thenReturn(10L);
        Mockito.when(table.getIndexMetaByIndexId(10L)).thenReturn(source);
        Mockito.when(table.getIndexIdToMeta()).thenReturn(ImmutableMap.of(10L, source));
        Mockito.when(table.getRowBinlogMeta()).thenReturn(binlog);
        BinlogConfig config = Mockito.mock(BinlogConfig.class);
        Mockito.when(config.getNeedHistoricalValue()).thenReturn(true);
        Mockito.when(table.getBinlogConfig()).thenReturn(config);
        TransactionState state = new TransactionState();
        GlobalTransactionMgrIface manager = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(manager.getTransactionState(1L, 100L)).thenReturn(state);
        try (MockedStatic<Env> env = Mockito.mockStatic(Env.class)) {
            env.when(Env::getCurrentGlobalTransactionMgr).thenReturn(manager);
            OlapTableSink sink;
            if (remote) {
                RemoteDorisExternalCatalog catalog = Mockito.mock(RemoteDorisExternalCatalog.class,
                        Mockito.RETURNS_DEEP_STUBS);
                Mockito.when(((RemoteOlapTable) table).getCatalog()).thenReturn(catalog);
                sink = new RemoteOlapTableSink((RemoteOlapTable) table, new TupleDescriptor(new TupleId(0)),
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
            } else {
                sink = new OlapTableSink(table, new TupleDescriptor(new TupleId(0)), Collections.emptyList());
            }
            Deencapsulation.setField(sink, "txnId", planningTxnId);
            TOlapTableSchemaParam schema = Deencapsulation.invoke(sink, "createSchema", 1L, table);
            if (remote) {
                Assertions.assertEquals(2, schema.getIndexes().get(0).getRowBinlogColumnMappingsSize());
                Mockito.verifyNoInteractions(manager);
                Assertions.assertTrue(state.getRowBinlogColumnMappings(100L).isEmpty());
                return;
            }
        }
        String expected = "{\"100\":{\"10\":{\"historical\":true,\"columns\":["
                + "{\"source\":1,\"current\":11},{\"source\":2,\"current\":12,\"before\":22}]}}}";
        Assertions.assertEquals(JsonParser.parseString(expected),
                JsonParser.parseString(GsonUtils.GSON.toJson(state)).getAsJsonObject().get("rowBinlogMappings"));
    }

    @Test
    public void testCreateHistoricalRowBinlogColumnMappings() throws Exception {
        MaterializedIndexMeta sourceMeta = indexMeta(1L, Arrays.asList(
                column("K1", 1, true, true),
                column("hidden_value", 2, false, false),
                column(Column.SHADOW_NAME_PREFIX + "v1", 3, false, true),
                column("HIDDEN_KEY", 4, true, false)));
        MaterializedIndexMeta rowBinlogMeta = indexMeta(2L, Arrays.asList(
                column("k1", 11, true, true),
                column("V1", 13, false, true),
                column("hidden_key", 14, true, false),
                column(Column.generateBeforeColName("v1"), 23, false, true)));

        List<TRowBinlogWriteColumnMapping> mappings = OlapTableSink.createRowBinlogColumnMappings(
                sourceMeta, rowBinlogMeta, true);

        Assertions.assertEquals(3, mappings.size());
        assertMapping(mappings.get(0), 1, 11, null);
        assertMapping(mappings.get(1), 3, 13, 23);
        assertMapping(mappings.get(2), 4, 14, null);
    }

    @Test
    public void testCreateNonHistoricalRowBinlogColumnMappings() throws Exception {
        MaterializedIndexMeta sourceMeta = indexMeta(1L, Arrays.asList(
                column("k1", 1, true, true), column("v1", 2, false, true)));
        MaterializedIndexMeta rowBinlogMeta = indexMeta(2L, Arrays.asList(
                column("k1", 11, true, true), column("v1", 12, false, true)));

        List<TRowBinlogWriteColumnMapping> mappings = OlapTableSink.createRowBinlogColumnMappings(
                sourceMeta, rowBinlogMeta, false);

        Assertions.assertEquals(2, mappings.size());
        assertMapping(mappings.get(0), 1, 11, null);
        assertMapping(mappings.get(1), 2, 12, null);
    }

    @Test
    public void testCreateDummyLocationUsesLoadAvailableBackendInCurrentComputeGroup() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        Backend loadDisabledBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(loadDisabledBackend.getId()).thenReturn(2L);
        Mockito.when(loadDisabledBackend.isLoadAvailable()).thenReturn(false);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend, 2L, loadDisabledBackend));
        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(Collections.singletonList(3L));
        Mockito.when(table.getIndexNumber()).thenReturn(1);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList());
            TOlapTableLocationParam location = sink.createDummyLocation(table);

            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(0).getNodeIds());
            Mockito.verify(systemInfoService, Mockito.never()).getAllBackendIds(true);
            Mockito.verify(systemInfoService).getBackendsByCurrentCluster();
        }
    }

    @Test
    public void testCreateDummyLocationDoesNotShareBackendCandidatesAcrossIndexes() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend));
        Mockito.when(table.getIndexNumber()).thenReturn(2);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList());
            TOlapTableLocationParam location = sink.createDummyLocation(table);

            Assertions.assertEquals(2, location.getTabletsSize());
            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(0).getNodeIds());
            Assertions.assertEquals(Collections.singletonList(1L),
                    location.getTablets().get(1).getNodeIds());
        }
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsPerIndex() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1000L);
        partition.setNumBuckets(2);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(20L)),
                new TTabletLocation(201L, Arrays.asList(10L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L), Arrays.asList(partition), locations, 2);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1000L);
        Assertions.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assertions.assertEquals(10L, be10Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 1L, 10L, Arrays.asList(0));
        assertIndexAssignment(be10Assignment, 2L, 20L, Arrays.asList(0));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1000L);
        Assertions.assertEquals(1, be20Assignment.getLoadTabletIdx());
        Assertions.assertEquals(20L, be20Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(1), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 1L, 20L, Arrays.asList(1));
        assertIndexAssignment(be20Assignment, 2L, 10L, Arrays.asList(1));

        OlapTableSink.applyAdaptiveRandomBucketAssignments(Arrays.asList(partition), assignments.get(10L));
        Assertions.assertEquals(10L, partition.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getLocalBucketSeqs());
        Assertions.assertEquals(10L, partition.getIndexes().get(0).getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getIndexes().get(0).getLocalBucketSeqs());
        Assertions.assertEquals(20L, partition.getIndexes().get(1).getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0), partition.getIndexes().get(1).getLocalBucketSeqs());
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsSharedByReceiverPartition() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1001L);
        partition.setNumBuckets(4);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L, 102L, 103L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L, 202L, 203L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(10L)),
                new TTabletLocation(102L, Arrays.asList(20L)),
                new TTabletLocation(103L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(30L)),
                new TTabletLocation(201L, Arrays.asList(30L)),
                new TTabletLocation(202L, Arrays.asList(30L)),
                new TTabletLocation(203L, Arrays.asList(30L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L, 30L, 40L), Arrays.asList(partition), locations, 4);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1001L);
        Assertions.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assertions.assertEquals(10L, be10Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(0, 1), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1001L);
        Assertions.assertEquals(2, be20Assignment.getLoadTabletIdx());
        Assertions.assertEquals(20L, be20Assignment.getBucketBeId());
        Assertions.assertEquals(Arrays.asList(2, 3), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be30Assignment = assignments.get(30L).get(1001L);
        Assertions.assertEquals(be10Assignment.getLoadTabletIdx(), be30Assignment.getLoadTabletIdx());
        Assertions.assertEquals(be10Assignment.getBucketBeId(), be30Assignment.getBucketBeId());
        Assertions.assertEquals(be10Assignment.getLocalBucketSeqs(), be30Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be30Assignment, 1L, 10L, Arrays.asList(0, 1));
        assertIndexAssignment(be30Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be40Assignment = assignments.get(40L).get(1001L);
        Assertions.assertEquals(be20Assignment.getLoadTabletIdx(), be40Assignment.getLoadTabletIdx());
        Assertions.assertEquals(be20Assignment.getBucketBeId(), be40Assignment.getBucketBeId());
        Assertions.assertEquals(be20Assignment.getLocalBucketSeqs(), be40Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be40Assignment, 1L, 20L, Arrays.asList(2, 3));
        assertIndexAssignment(be40Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));
    }

    private void assertIndexAssignment(AdaptiveBucketAssignment assignment, long indexId, long bucketBeId,
            List<Integer> localBucketSeqs) {
        AdaptiveIndexBucketAssignment indexAssignment = assignment.getIndexAssignments().get(indexId);
        Assertions.assertNotNull(indexAssignment);
        Assertions.assertEquals(indexId, indexAssignment.getIndexId());
        Assertions.assertEquals(bucketBeId, indexAssignment.getBucketBeId());
        Assertions.assertEquals(localBucketSeqs, indexAssignment.getLocalBucketSeqs());
    }

    private static Column column(String name, int uniqueId, boolean isKey, boolean isVisible) {
        Column column = new Column(name, PrimitiveType.INT);
        column.setUniqueId(uniqueId);
        column.setIsKey(isKey);
        column.setIsVisible(isVisible);
        return column;
    }

    private static MaterializedIndexMeta indexMeta(long indexId, List<Column> columns) {
        return new MaterializedIndexMeta(indexId, columns, 1, 1, (short) 1, TStorageType.COLUMN,
                KeysType.PRIMARY_KEYS, null);
    }

    private static void assertMapping(TRowBinlogWriteColumnMapping mapping, int sourceUniqueId,
            int currentUniqueId, Integer beforeUniqueId) {
        Assertions.assertEquals(sourceUniqueId, mapping.getSourceColumnUniqueId());
        Assertions.assertEquals(currentUniqueId, mapping.getCurrentColumnUniqueId());
        Assertions.assertEquals(beforeUniqueId != null, mapping.isSetBeforeColumnUniqueId());
        if (beforeUniqueId != null) {
            Assertions.assertEquals(beforeUniqueId.intValue(), mapping.getBeforeColumnUniqueId());
        }
    }
}
