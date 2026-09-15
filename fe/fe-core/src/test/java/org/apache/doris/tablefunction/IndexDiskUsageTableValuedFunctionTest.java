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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TIndexDiskUsageMetadataParams;
import org.apache.doris.thrift.TIndexDiskUsageTablet;
import org.apache.doris.thrift.TMetaScanRange;
import org.apache.doris.thrift.TMetadataType;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IndexDiskUsageTableValuedFunctionTest {
    private static final long BASE_INDEX_ID = 1000L;

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedContext;
    private AccessControllerManager accessManager;
    private Database db;
    private SessionVariable sessionVariable;

    @BeforeEach
    public void setUp() throws Exception {
        sessionVariable = new SessionVariable();
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(ctx.getQualifiedUser()).thenReturn("u1");
        Mockito.when(ctx.getRemoteIP()).thenReturn("127.0.0.1");

        Env env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        allowShow(true);

        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        db = Mockito.mock(Database.class);
        Mockito.when(catalog.getDbOrAnalysisException("db")).thenReturn(db);
        OlapTable table = mockOlapTable();
        Mockito.when(db.getTableOrAnalysisException("logs")).thenReturn(table);

        mockedContext = Mockito.mockStatic(ConnectContext.class);
        mockedContext.when(ConnectContext::get).thenReturn(ctx);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
    }

    @AfterEach
    public void tearDown() {
        mockedEnv.close();
        mockedContext.close();
    }

    @Test
    public void testRejectsUnknownProperty() {
        assertAnalysisError("'foo' is invalid property", params("foo", "bar"));
    }

    @Test
    public void testRequiresDatabaseAndTable() {
        assertAnalysisError("'database' and 'table' are required for index_disk_usage",
                ImmutableMap.of("table", "logs"));
    }

    @Test
    public void testRejectsUnsupportedLevel() {
        assertAnalysisError("Unsupported level 'partition' for index_disk_usage, expected tablet, rowset or segment",
                params("level", "partition"));
    }

    @Test
    public void testRejectsInvalidPositionDetail() {
        assertAnalysisError("Invalid position_detail 'abc', expected true or false",
                params("position_detail", "abc"));
    }

    @Test
    public void testRejectsNonOlapTable() throws Exception {
        View view = Mockito.mock(View.class);
        Mockito.when(db.getTableOrAnalysisException("v1")).thenReturn(view);
        assertAnalysisError("index_disk_usage only supports OLAP table", params("table", "v1"));
    }

    @Test
    public void testRejectsUnknownPartition() {
        assertAnalysisError("Unknown partition 'p9' in table db.logs", params("partitions", "p1,p9"));
    }

    @Test
    public void testRejectsIndexFilterWithoutNames() {
        // An empty index list would reach BE as "all indexes" and turn a narrowed query into a full scan.
        assertAnalysisError("'indexes' must list at least one name", params("indexes", " , "));
    }

    @Test
    public void testRejectsPartitionFilterWithoutNames() {
        assertAnalysisError("'partitions' must list at least one name", params("partitions", ","));
    }

    @Test
    public void testRejectsUnknownIndex() {
        assertAnalysisError("Unknown index 'idx9' in table db.logs", params("indexes", "idx9"));
    }

    @Test
    public void testRejectsNonInvertedIndex() {
        assertAnalysisError("Index 'idx_bf' in table db.logs is not an inverted or ANN index",
                params("indexes", "idx_bf"));
    }

    @Test
    public void testDeniesWithoutShowPrivilege() {
        allowShow(false);
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new IndexDiskUsageTableValuedFunction(params()));
        Assertions.assertTrue(e.getMessage().contains("denied"), e.getMessage());
    }

    @Test
    public void testSchema() {
        List<Column> columns = new IndexDiskUsageTableValuedFunction(params()).getTableColumns();
        Assertions.assertEquals(Arrays.asList("PARTITION_NAME", "MATERIALIZED_INDEX_NAME", "TABLET_ID",
                "BACKEND_ID", "ROWSET_ID", "SEGMENT_ID", "INDEX_ID", "INDEX_NAME", "INDEX_TYPE", "COLUMN_NAME",
                "INDEX_SUFFIX", "STRUCTURE", "STORAGE_FORMAT", "SEGMENT_COUNT", "ROW_COUNT", "TOTAL_BYTES",
                "DICT_BYTES", "POSTING_BYTES", "POSITION_BYTES", "STATS_BYTES", "OTHER_BYTES", "STATS_SOURCE"),
                columns.stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertEquals(Arrays.asList(PrimitiveType.VARCHAR, PrimitiveType.VARCHAR,
                PrimitiveType.BIGINT, PrimitiveType.BIGINT, PrimitiveType.VARCHAR, PrimitiveType.INT,
                PrimitiveType.BIGINT, PrimitiveType.VARCHAR, PrimitiveType.VARCHAR, PrimitiveType.VARCHAR,
                PrimitiveType.VARCHAR, PrimitiveType.VARCHAR, PrimitiveType.VARCHAR, PrimitiveType.BIGINT,
                PrimitiveType.BIGINT, PrimitiveType.BIGINT, PrimitiveType.BIGINT, PrimitiveType.BIGINT,
                PrimitiveType.BIGINT, PrimitiveType.BIGINT, PrimitiveType.BIGINT, PrimitiveType.VARCHAR),
                columns.stream().map(c -> c.getType().getPrimitiveType()).collect(Collectors.toList()));
        Assertions.assertTrue(columns.stream().allMatch(Column::isAllowNull));
    }

    @Test
    public void testMetaScanRangeDefaults() {
        IndexDiskUsageTableValuedFunction tvf = new IndexDiskUsageTableValuedFunction(params());
        Assertions.assertEquals(TMetadataType.INDEX_DISK_USAGE, tvf.getMetadataType());
        TMetaScanRange range = tvf.getMetaScanRange(Lists.newArrayList());
        Assertions.assertEquals(TMetadataType.INDEX_DISK_USAGE, range.getMetadataType());
        TIndexDiskUsageMetadataParams p = range.getIndexDiskUsageParams();
        Assertions.assertEquals("tablet", p.getLevel());
        Assertions.assertFalse(p.isPositionDetail());
        Assertions.assertTrue(p.getIndexIds().isEmpty());
        Assertions.assertEquals(ImmutableMap.of(10L, "p1", 11L, "p2"), p.getPartitionNames());
        Assertions.assertEquals(ImmutableMap.of(BASE_INDEX_ID, "logs"), p.getMaterializedIndexNames());
        Assertions.assertEquals(Arrays.asList(tablet(101, 10, 5), tablet(102, 10, 5), tablet(103, 11, 7)),
                p.getTablets());
    }

    @Test
    public void testMetaScanRangeWithFilters() {
        IndexDiskUsageTableValuedFunction tvf = new IndexDiskUsageTableValuedFunction(
                params("partitions", "p2", "indexes", "idx_ts", "level", "SEGMENT", "position_detail", "true"));
        TIndexDiskUsageMetadataParams p = tvf.getMetaScanRange(Lists.newArrayList()).getIndexDiskUsageParams();
        Assertions.assertEquals("segment", p.getLevel());
        Assertions.assertTrue(p.isPositionDetail());
        Assertions.assertEquals(Arrays.asList(1002L), p.getIndexIds());
        Assertions.assertEquals(ImmutableMap.of(11L, "p2"), p.getPartitionNames());
        Assertions.assertEquals(Arrays.asList(tablet(103, 11, 7)), p.getTablets());
    }

    @Test
    public void testPositionDetailTabletLimit() {
        sessionVariable.indexDiskUsagePositionDetailMaxTablets = 2;
        assertAnalysisError("position_detail covers 3 tablets, exceeding "
                + "index_disk_usage_position_detail_max_tablets=2; narrow partitions or indexes",
                params("position_detail", "true"));
    }

    @Test
    public void testTabletTargets() {
        List<IndexDiskUsageTableValuedFunction.TabletTarget> targets =
                new IndexDiskUsageTableValuedFunction(params("partitions", "p1")).getTabletTargets();
        Assertions.assertEquals(2, targets.size());
        Assertions.assertEquals(101L, targets.get(0).getTabletId());
        Assertions.assertEquals(10L, targets.get(0).getPartitionId());
        Assertions.assertEquals(5L, targets.get(0).getVersion());
        Assertions.assertEquals(101L, targets.get(0).getTablet().getId());
        Assertions.assertEquals(BASE_INDEX_ID, targets.get(0).getMaterializedIndexId());
        Assertions.assertEquals(102L, targets.get(1).getTabletId());
    }

    @Test
    public void testLocalVisibleVersionIsReadUnderTableLock() throws Exception {
        OlapTable table = mockOlapTable();
        Mockito.when(db.getTableOrAnalysisException("locked")).thenReturn(table);
        new IndexDiskUsageTableValuedFunction(params("table", "locked"));
        // Local replica choice filters replicas by this version, so it is read together with the
        // tablets under the table lock.
        Partition p1 = table.getPartition("p1", false);
        InOrder inOrder = Mockito.inOrder(table, p1);
        inOrder.verify(table).readLock();
        inOrder.verify(p1).getVisibleVersion();
        inOrder.verify(table).readUnlock();
    }

    @Test
    public void testCollectsVisibleRollupTablets() throws Exception {
        OlapTable table = mockOlapTable();
        Partition p1 = table.getPartition("p1", false);
        MaterializedIndex baseIndex = p1.getBaseIndex();
        Tablet rollupTablet = Mockito.mock(Tablet.class);
        Mockito.when(rollupTablet.getId()).thenReturn(201L);
        MaterializedIndex rollup = Mockito.mock(MaterializedIndex.class);
        Mockito.when(rollup.getId()).thenReturn(2000L);
        Mockito.when(rollup.getTablets()).thenReturn(Arrays.asList(rollupTablet));
        // A light ADD INDEX installs the index on rollups too, so their tablets hold index files.
        Mockito.when(p1.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(Arrays.asList(baseIndex, rollup));
        Mockito.when(table.getIndexNameById(2000L)).thenReturn("r_msg");
        Mockito.when(db.getTableOrAnalysisException("with_rollup")).thenReturn(table);

        IndexDiskUsageTableValuedFunction tvf = new IndexDiskUsageTableValuedFunction(
                params("table", "with_rollup", "partitions", "p1"));
        List<IndexDiskUsageTableValuedFunction.TabletTarget> targets = tvf.getTabletTargets();
        Assertions.assertEquals(Arrays.asList(101L, 102L, 201L), targets.stream()
                .map(IndexDiskUsageTableValuedFunction.TabletTarget::getTabletId).collect(Collectors.toList()));
        Assertions.assertEquals(2000L, targets.get(2).getMaterializedIndexId());
        Assertions.assertEquals(5L, targets.get(2).getVersion());
        Assertions.assertEquals(ImmutableMap.of(BASE_INDEX_ID, "logs", 2000L, "r_msg"),
                tvf.getMetaScanRange(Lists.newArrayList()).getIndexDiskUsageParams().getMaterializedIndexNames());
    }

    private void allowShow(boolean allowed) {
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class))).thenReturn(allowed);
    }

    private static void assertAnalysisError(String expected, Map<String, String> params) {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new IndexDiskUsageTableValuedFunction(params));
        Assertions.assertTrue(e.getMessage().contains(expected), e.getMessage());
    }

    private static Map<String, String> params(String... overrides) {
        Map<String, String> params = new HashMap<>();
        params.put("database", "db");
        params.put("table", "logs");
        for (int i = 0; i + 1 < overrides.length; i += 2) {
            params.put(overrides[i], overrides[i + 1]);
        }
        return params;
    }

    private static TIndexDiskUsageTablet tablet(long tabletId, long partitionId, long version) {
        TIndexDiskUsageTablet tablet = new TIndexDiskUsageTablet();
        tablet.setTabletId(tabletId);
        tablet.setPartitionId(partitionId);
        tablet.setMaterializedIndexId(BASE_INDEX_ID);
        tablet.setVersion(version);
        return tablet;
    }

    private static OlapTable mockOlapTable() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("logs");
        Mockito.when(table.getIndexNameById(BASE_INDEX_ID)).thenReturn("logs");
        Partition p1 = mockPartition(10L, "p1", 5L, 101L, 102L);
        Partition p2 = mockPartition(11L, "p2", 7L, 103L);
        Mockito.when(table.getPartitions()).thenReturn(Arrays.asList(p1, p2));
        Mockito.when(table.getPartition("p1", false)).thenReturn(p1);
        Mockito.when(table.getPartition("p2", false)).thenReturn(p2);
        List<Index> indexes = Arrays.asList(
                mockIndex(1001L, "idx_msg", IndexType.INVERTED),
                mockIndex(1002L, "idx_ts", IndexType.INVERTED),
                mockIndex(1003L, "idx_bf", IndexType.NGRAM_BF));
        Mockito.when(table.getIndexes()).thenReturn(indexes);
        return table;
    }

    private static Partition mockPartition(long id, String name, long version, Long... tabletIds) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(id);
        Mockito.when(partition.getName()).thenReturn(name);
        Mockito.when(partition.getVisibleVersion()).thenReturn(version);
        List<Tablet> tablets = Lists.newArrayList();
        for (Long tabletId : tabletIds) {
            Tablet tablet = Mockito.mock(Tablet.class);
            Mockito.when(tablet.getId()).thenReturn(tabletId);
            tablets.add(tablet);
        }
        MaterializedIndex baseIndex = Mockito.mock(MaterializedIndex.class);
        Mockito.when(baseIndex.getId()).thenReturn(BASE_INDEX_ID);
        Mockito.when(baseIndex.getTablets()).thenReturn(tablets);
        Mockito.when(partition.getBaseIndex()).thenReturn(baseIndex);
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE))
                .thenReturn(Arrays.asList(baseIndex));
        return partition;
    }

    private static Index mockIndex(long id, String name, IndexType type) {
        Index index = Mockito.mock(Index.class);
        Mockito.when(index.getIndexId()).thenReturn(id);
        Mockito.when(index.getIndexName()).thenReturn(name);
        Mockito.when(index.getIndexType()).thenReturn(type);
        return index;
    }
}
