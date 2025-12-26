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
import org.apache.doris.info.TableNameInfo;
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
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MTMVRewriteUtilTest {
    @Mocked
    private MTMV mtmv;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private SessionVariable sessionVariable;
    @Mocked
    private Partition p1;
    @Mocked
    private MTMVRelation relation;
    @Mocked
    private MTMVStatus status;
    @Mocked
    private MTMVPartitionUtil mtmvPartitionUtil;
    @Mocked
    private MTMVUtil mtmvUtil;
    private long currentTimeMills = 3L;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {

        new Expectations() {
            {
                mtmv.getPartitions();
                minTimes = 0;
                result = Lists.newArrayList(p1);

                mtmv.getPartitionNames();
                minTimes = 0;
                result = Sets.newHashSet("p1");

                p1.getName();
                minTimes = 0;
                result = "p1";

                p1.getVisibleVersionTime();
                minTimes = 0;
                result = 1L;

                mtmv.getGracePeriod();
                minTimes = 0;
                result = 0L;

                mtmv.getRelation();
                minTimes = 0;
                result = relation;

                mtmv.getStatus();
                minTimes = 0;
                result = status;

                mtmv.getGracePeriod();
                minTimes = 0;
                result = 0L;

                status.getState();
                minTimes = 0;
                result = MTMVState.NORMAL;

                status.getRefreshState();
                minTimes = 0;
                result = MTMVRefreshState.SUCCESS;

                ctx.getSessionVariable();
                minTimes = 0;
                result = sessionVariable;

                sessionVariable.isEnableMaterializedViewRewrite();
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = true;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<TableNameInfo>) any);
                minTimes = 0;
                result = true;

                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = false;

                mtmv.canBeCandidate();
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testGetMTMVCanRewritePartitionsForceConsistent() throws AnalysisException {
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 2L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<TableNameInfo>) any);
                minTimes = 0;
                result = false;
            }
        };

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
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 2L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<TableNameInfo>) any);
                minTimes = 0;
                result = false;
            }
        };

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotInGracePeriod() throws AnalysisException {
        new Expectations() {
            {
                mtmv.getGracePeriod();
                minTimes = 0;
                result = 1L;

                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<TableNameInfo>) any);
                minTimes = 0;
                result = false;
            }
        };

        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableMaterializedViewRewrite() {
        new Expectations() {
            {
                sessionVariable.isEnableMaterializedViewRewrite();
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsNotSync() throws AnalysisException {
        new Expectations() {
            {
                MTMVPartitionUtil.isMTMVPartitionSync((MTMVRefreshContext) any, anyString,
                        (Set<BaseTableInfo>) any,
                        (Set<TableNameInfo>) any);
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsEnableContainExternalTable() {
        new Expectations() {
            {
                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = true;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsDisableContainExternalTable() {
        new Expectations() {
            {
                MTMVUtil.mtmvContainsExternalTable((MTMV) any);
                minTimes = 0;
                result = true;

                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness();
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        // getMTMVCanRewritePartitions only check the partition is valid or not, it doesn't care the
        // isEnableMaterializedViewRewriteWhenBaseTableUnawareness
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsStateAbnormal() {
        new Expectations() {
            {
                mtmv.canBeCandidate();
                minTimes = 0;
                result = false;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(0, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateAbnormal() {
        new Expectations() {
            {
                status.getRefreshState();
                minTimes = 0;
                result = MTMVRefreshState.FAIL;
            }
        };
        Collection<Partition> mtmvCanRewritePartitions = MTMVRewriteUtil
                .getMTMVCanRewritePartitions(mtmv, ctx, currentTimeMills, false,
                        null);
        Assert.assertEquals(1, mtmvCanRewritePartitions.size());
    }

    @Test
    public void testGetMTMVCanRewritePartitionsRefreshStateInit() {
        new Expectations() {
            {
                mtmv.canBeCandidate();
                minTimes = 0;
                result = false;
            }
        };
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
