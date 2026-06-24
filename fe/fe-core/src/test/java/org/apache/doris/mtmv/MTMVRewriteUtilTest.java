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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.TableIndexes;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MTMVRewriteUtilTest {
    private MTMV mtmv = Mockito.mock(MTMV.class);
    private ConnectContext ctx = Mockito.mock(ConnectContext.class);
    private SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
    private Partition p1 = Mockito.mock(Partition.class);
    private MTMVRelation relation = Mockito.mock(MTMVRelation.class);
    private MTMVStatus status = Mockito.mock(MTMVStatus.class);
    private MTMVPartitionInfo mvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
    private MockedStatic<MTMVPartitionUtil> mtmvPartitionUtilStatic;
    private MockedStatic<MTMVUtil> mtmvUtilStatic;
    private long currentTimeMills = 3L;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        mtmvPartitionUtilStatic = Mockito.mockStatic(MTMVPartitionUtil.class);
        mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);

        Mockito.when(mtmv.getPartitions()).thenReturn(Lists.newArrayList(p1));

        Mockito.when(mtmv.getPartitionNames()).thenReturn(Sets.newHashSet("p1"));

        Mockito.when(p1.getName()).thenReturn("p1");

        Mockito.when(p1.getVisibleVersionTime()).thenReturn(1L);

        Mockito.when(mtmv.getGracePeriod()).thenReturn(0L);

        Mockito.when(mtmv.getRelation()).thenReturn(relation);

        Mockito.when(mtmv.getStatus()).thenReturn(status);

        Mockito.when(status.getState()).thenReturn(MTMVState.NORMAL);

        Mockito.when(status.getRefreshState()).thenReturn(MTMVRefreshState.SUCCESS);

        Mockito.when(ctx.getSessionVariable()).thenReturn(sessionVariable);

        Mockito.when(sessionVariable.isEnableMaterializedViewRewrite()).thenReturn(true);

        Mockito.when(sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness()).thenReturn(true);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVPartitionSync(
                Mockito.any(MTMVRefreshContext.class), Mockito.anyString(),
                Mockito.any(Set.class),
                Mockito.any(Set.class))).thenReturn(true);

        mtmvUtilStatic.when(() -> MTMVUtil.mtmvContainsExternalTable(
                Mockito.any(MTMV.class))).thenReturn(false);

        Mockito.when(mtmv.getMvPartitionInfo()).thenReturn(mvPartitionInfo);

        Mockito.when(mvPartitionInfo.getPartitionType()).thenReturn(MTMVPartitionType.SELF_MANAGE);

        Mockito.when(mvPartitionInfo.getPctTables()).thenReturn(Sets.newHashSet());

        Mockito.when(mtmv.canBeCandidate()).thenReturn(true);
    }

    @After
    public void tearDown() {
        mtmvPartitionUtilStatic.close();
        mtmvUtilStatic.close();
    }

    @Test
    public void testGetMTMVCanRewritePartitionsForceConsistent() throws AnalysisException {
        Mockito.when(mtmv.getGracePeriod()).thenReturn(2L);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVPartitionSync(
                Mockito.any(MTMVRefreshContext.class), Mockito.anyString(),
                Mockito.any(Set.class),
                Mockito.any(Set.class))).thenReturn(false);

        // currentTimeMills is 3, grace period is 2, and partition getVisibleVersionTime is 1
        // if forceConsistent this should get 0 partitions which mtmv can use.
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, true, null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNormal() {
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsInGracePeriod() throws AnalysisException {
        Mockito.when(mtmv.getGracePeriod()).thenReturn(2L);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVPartitionSync(
                Mockito.any(MTMVRefreshContext.class), Mockito.anyString(),
                Mockito.any(Set.class),
                Mockito.any(Set.class))).thenReturn(false);

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotInGracePeriod() throws AnalysisException {
        Mockito.when(mtmv.getGracePeriod()).thenReturn(1L);

        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVPartitionSync(
                Mockito.any(MTMVRefreshContext.class), Mockito.anyString(),
                Mockito.any(Set.class),
                Mockito.any(Set.class))).thenReturn(false);

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableMaterializedViewRewrite() {
        Mockito.when(sessionVariable.isEnableMaterializedViewRewrite()).thenReturn(false);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotSync() throws AnalysisException {
        mtmvPartitionUtilStatic.when(() -> MTMVPartitionUtil.isMTMVPartitionSync(
                Mockito.any(MTMVRefreshContext.class), Mockito.anyString(),
                Mockito.any(Set.class),
                Mockito.any(Set.class))).thenReturn(false);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsEnableContainExternalTable() {
        mtmvUtilStatic.when(() -> MTMVUtil.mtmvContainsExternalTable(
                Mockito.any(MTMV.class))).thenReturn(true);

        Mockito.when(sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness()).thenReturn(true);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableContainExternalTable() {
        mtmvUtilStatic.when(() -> MTMVUtil.mtmvContainsExternalTable(
                Mockito.any(MTMV.class))).thenReturn(true);

        Mockito.when(sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness()).thenReturn(false);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsStateAbnormal() {
        Mockito.when(mtmv.canBeCandidate()).thenReturn(false);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateAbnormal() {
        Mockito.when(status.getRefreshState()).thenReturn(MTMVRefreshState.FAIL);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateInit() {
        Mockito.when(mtmv.canBeCandidate()).thenReturn(false);
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testPctToMv() {
        TestMTMVRelatedTable t1 = new TestMTMVRelatedTable("t1");
        TestMTMVRelatedTable t2 = new TestMTMVRelatedTable("t2");
        Map<String, Map<MTMVRelatedTableIf, Set<String>>> partitionMappings = Maps.newHashMap();
        partitionMappings.put("mv_p1",
                ImmutableMap.of(t1, Sets.newHashSet("t1_p1", "t1_p2"), t2, Sets.newHashSet("t2_p1")));
        partitionMappings.put("mv_p2", ImmutableMap.of(t2, Sets.newHashSet("t2_p2")));
        Map<Pair<MTMVRelatedTableIf, String>, String> pctToMv = MTMVRewriteUtil.getPctToMv(partitionMappings);
        Assert.assertEquals("mv_p1", pctToMv.get(Pair.of(t1, "t1_p1")));
        Assert.assertEquals("mv_p1", pctToMv.get(Pair.of(t1, "t1_p2")));
        Assert.assertEquals("mv_p1", pctToMv.get(Pair.of(t2, "t2_p1")));
        Assert.assertEquals("mv_p2", pctToMv.get(Pair.of(t2, "t2_p2")));
    }

    private static class TestMTMVRelatedTable implements MTMVRelatedTableIf {

        private String name;

        public TestMTMVRelatedTable(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestMTMVRelatedTable that = (TestMTMVRelatedTable) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public Map<String, PartitionItem> getAndCopyPartitionItems(Optional<MvccSnapshot> snapshot)
                throws AnalysisException {
            return null;
        }

        @Override
        public PartitionType getPartitionType(Optional<MvccSnapshot> snapshot) {
            return null;
        }

        @Override
        public Set<String> getPartitionColumnNames(Optional<MvccSnapshot> snapshot) throws DdlException {
            return null;
        }

        @Override
        public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
            return null;
        }

        @Override
        public MTMVSnapshotIf getPartitionSnapshot(String partitionName, MTMVRefreshContext context,
                Optional<MvccSnapshot> snapshot) throws AnalysisException {
            return null;
        }

        @Override
        public MTMVSnapshotIf getTableSnapshot(MTMVRefreshContext context, Optional<MvccSnapshot> snapshot)
                throws AnalysisException {
            return null;
        }

        @Override
        public MTMVSnapshotIf getTableSnapshot(Optional<MvccSnapshot> snapshot) throws AnalysisException {
            return null;
        }

        @Override
        public long getNewestUpdateVersionOrTime() {
            return 0;
        }

        @Override
        public boolean isPartitionColumnAllowNull() {
            return false;
        }

        @Override
        public long getId() {
            return 0;
        }

        @Override
        public String getName() {
            return "";
        }

        @Override
        public TableType getType() {
            return null;
        }

        @Override
        public List<Column> getFullSchema() {
            return null;
        }

        @Override
        public List<Column> getBaseSchema() {
            return null;
        }

        @Override
        public List<Column> getBaseSchema(boolean full) {
            return null;
        }

        @Override
        public void setNewFullSchema(List<Column> newSchema) {

        }

        @Override
        public Column getColumn(String name) {
            return null;
        }

        @Override
        public String getMysqlType() {
            return "";
        }

        @Override
        public String getEngine() {
            return "";
        }

        @Override
        public String getComment() {
            return "";
        }

        @Override
        public long getCreateTime() {
            return 0;
        }

        @Override
        public long getUpdateTime() {
            return 0;
        }

        @Override
        public long getRowCount() {
            return 0;
        }

        @Override
        public long getCachedRowCount() {
            return 0;
        }

        @Override
        public long fetchRowCount() {
            return 0;
        }

        @Override
        public long getDataLength() {
            return 0;
        }

        @Override
        public long getAvgRowLength() {
            return 0;
        }

        @Override
        public long getIndexLength() {
            return 0;
        }

        @Override
        public long getLastCheckTime() {
            return 0;
        }

        @Override
        public String getComment(boolean escapeQuota) {
            return "";
        }

        @Override
        public TTableDescriptor toThrift() {
            return null;
        }

        @Override
        public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
            return null;
        }

        @Override
        public DatabaseIf getDatabase() {
            return null;
        }

        @Override
        public Optional<ColumnStatistic> getColumnStatistic(String colName) {
            return Optional.empty();
        }

        @Override
        public Set<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
            return null;
        }

        @Override
        public List<Long> getChunkSizes() {
            return null;
        }

        @Override
        public void write(DataOutput out) throws IOException {

        }

        @Override
        public boolean autoAnalyzeEnabled() {
            return false;
        }

        @Override
        public TableIndexes getTableIndexes() {
            return null;
        }
    }
}
