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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.NormalizeOlapTableStreamScan;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class IvmFullRefreshMTMVTest extends TestWithFeService {
    private static final String DB_NAME = "test_ivm_full_refresh_mtmv";
    private static final String BASE_TABLE_NAME = "base_tbl";
    private static final long MV_ID = 10L;

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        Config.enable_feature_binlog = true;

        createDatabaseAndUse(DB_NAME);
        createTable("CREATE TABLE " + BASE_TABLE_NAME + " (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"100\"),\n"
                + " PARTITION p2 VALUES LESS THAN (\"200\"))\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',"
                + " 'binlog.enable' = 'true', 'binlog.need_historical_value' = 'true',"
                + " 'binlog.format' = 'ROW')");
        createTable("CREATE STREAM `" + streamName(baseTable())
                + "` ON TABLE " + BASE_TABLE_NAME);
        createTable("CREATE TABLE wrong_base_tbl (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true')");
    }

    @Test
    public void testReplaceOlapScanWithResetStreamScanAndKeepExprIds() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        LogicalOlapScan scan = newOlapScan(baseTable, Lists.newArrayList(partitionId));
        Set<Long> resetPartitionIds = resetPartitionIds(Lists.newArrayList(partitionId));

        Plan result = rewrite(scan, baseTable, Optional.of(resetPartitionIds));

        Assertions.assertTrue(result instanceof LogicalProject);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertTrue(project.child() instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) project.child();
        Assertions.assertTrue(streamScan.isReset());
        Assertions.assertFalse(streamScan.isSnapshot());
        Assertions.assertSame(baseTable, streamScan.getTable().getBaseTable());
        Assertions.assertEquals(Lists.newArrayList(partitionId), streamScan.getSelectedPartitionIds());
        assertProjectKeepsOriginSlots(scan, project);
    }

    @Test
    public void testReplaceOlapScanWithSnapshotStreamScan() throws Exception {
        OlapTable baseTable = baseTable();
        List<Long> partitionIds = baseTable.getPartitionIds();
        LogicalOlapScan scan = newOlapScan(baseTable, partitionIds);

        Plan result = rewrite(scan, baseTable, Optional.empty());

        Assertions.assertTrue(result instanceof LogicalProject);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertTrue(project.child() instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) project.child();
        Assertions.assertTrue(streamScan.isSnapshot());
        Assertions.assertFalse(streamScan.isReset());
        Assertions.assertSame(baseTable, streamScan.getTable().getBaseTable());
        Assertions.assertEquals(partitionIds.stream().sorted().collect(java.util.stream.Collectors.toList()),
                streamScan.getSelectedPartitionIds());
        assertProjectKeepsOriginSlots(scan, project);
    }

    @Test
    public void testResetUsesLiveBaseTableScan() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        LogicalOlapScan scan = newOlapScan(baseTable, Lists.newArrayList(partitionId));

        Plan reset = rewrite(scan, baseTable,
                Optional.of(resetPartitionIds(Lists.newArrayList(partitionId))));

        assertHasLiveBaseScan(normalizeStreamScan(reset), baseTable, Lists.newArrayList(partitionId));
    }

    @Test
    public void testResetScansSelectedPartitions() throws Exception {
        OlapTable baseTable = baseTable();
        Long firstPartitionId = baseTable.getPartition("p1").getId();
        Long secondPartitionId = baseTable.getPartition("p2").getId();
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException(streamName(baseTable));
        OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(stream, baseTable,
                Lists.newArrayList(firstPartitionId, secondPartitionId));
        LogicalOlapTableStreamScan streamScan = new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(), wrapper,
                ImmutableList.of(DB_NAME, stream.getName()),
                Lists.newArrayList(firstPartitionId, secondPartitionId), ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of()).withReadMode(StreamReadMode.RESET);

        Plan normalized = normalizeStreamScan(streamScan);

        List<LogicalOlapScan> baseScans = normalized.collectToList(LogicalOlapScan.class::isInstance).stream()
                .map(LogicalOlapScan.class::cast)
                .collect(java.util.stream.Collectors.toList());
        Assertions.assertEquals(1, baseScans.size());
        Assertions.assertSame(baseTable, baseScans.get(0).getTable());
        Assertions.assertEquals(Lists.newArrayList(firstPartitionId, secondPartitionId),
                baseScans.get(0).getSelectedPartitionIds());
    }

    @Test
    public void testNormalizeDoesNotRewriteFullRefreshScan() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        LogicalOlapScan scan = newOlapScan(baseTable, Lists.newArrayList(partitionId));
        IvmRewriteContext rewriteContext = fullRefreshContext(
                baseTable, Optional.of(resetPartitionIds(Lists.newArrayList(partitionId))));
        JobContext jobContext = newJobContext(scan, rewriteContext);

        Plan result = new IvmNormalizeMTMV().rewriteRoot(scan, jobContext);
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().orElseThrow();
        Plan secondResult = new IvmNormalizeMTMV().rewriteRoot(result, jobContext);
        Plan fullRefreshResult = rewrite(result, rewriteContext);

        Assertions.assertTrue(rewriteResult.isNormalizeRewritten());
        Assertions.assertSame(result, rewriteResult.getNormalizedPlan());
        Assertions.assertFalse(result.collectToList(LogicalOlapScan.class::isInstance).isEmpty());
        Assertions.assertTrue(result.collectToList(LogicalOlapTableStreamScan.class::isInstance).isEmpty());
        Assertions.assertFalse(fullRefreshResult
                .collectToList(LogicalOlapTableStreamScan.class::isInstance).isEmpty());
        Assertions.assertSame(result, secondResult);
    }

    @Test
    public void testFullContextWithoutStreamScansDoesNotRewrite() throws Exception {
        OlapTable baseTable = baseTable();
        LogicalOlapScan scan = newOlapScan(baseTable, baseTable.getPartitionIds());
        Plan result = rewrite(scan, IvmRewriteContext.full(mtmv()));

        Assertions.assertSame(scan, result);
    }

    @Test
    public void testIvmStreamValidationFailsClosed() throws Exception {
        OlapTable baseTable = baseTable();
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable wrongBaseTable = (OlapTable) db.getTableOrMetaException("wrong_base_tbl");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException(streamName(baseTable));

        Assertions.assertThrows(IvmException.class,
                () -> IvmUtil.getIvmStream(mtmv(MV_ID + 1, "missing_mv"), baseTable));
        Assertions.assertThrows(IvmException.class,
                () -> IvmUtil.getIvmStream(mtmv(), wrongBaseTable));

        stream.setDisabled(true);
        try {
            Assertions.assertThrows(IvmException.class,
                    () -> IvmUtil.getIvmStream(mtmv(), baseTable));
        } finally {
            stream.setDisabled(false);
        }
        stream.setStale(true);
        try {
            Assertions.assertThrows(IvmException.class,
                    () -> IvmUtil.getIvmStream(mtmv(), baseTable));
        } finally {
            stream.setStale(false);
        }
    }

    @Test
    public void testResolveStreamAfterMtmvRename() throws Exception {
        OlapTable baseTable = baseTable();
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException(streamName(baseTable));

        Assertions.assertSame(stream, IvmUtil.getIvmStream(mtmv(MV_ID, "renamed_mv"), baseTable));
    }

    private Plan rewrite(LogicalOlapScan scan, OlapTable baseTable,
            Optional<Set<Long>> resetPartitionIds) throws Exception {
        return rewrite(scan, fullRefreshContext(baseTable, resetPartitionIds));
    }

    private Plan rewrite(Plan plan, IvmRewriteContext rewriteContext) throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(DB_NAME);
        PlanChecker planChecker = PlanChecker.from(ctx, plan);
        planChecker.getCascadesContext().getStatementContext()
                .setIvmRewriteContext(Optional.of(rewriteContext));
        return planChecker.applyBottomUp(new IvmFullRefreshMTMV())
                .getCascadesContext().getRewritePlan();
    }

    private JobContext newJobContext(Plan plan, IvmRewriteContext rewriteContext) throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(DB_NAME);
        StatementContext statementContext = new StatementContext(ctx, null);
        statementContext.setIvmRewriteContext(Optional.of(rewriteContext));
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, plan, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }

    private IvmRewriteContext fullRefreshContext(OlapTable baseTable,
            Optional<Set<Long>> resetPartitionIds) {
        MTMV mtmv = mtmv();
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        Map<BaseTableInfo, Set<Long>> resetScopes = resetPartitionIds
                .map(partitionIds -> ImmutableMap.of(baseTableInfo, partitionIds))
                .orElseGet(ImmutableMap::of);
        StreamReadMode nonPctReadMode = resetPartitionIds.isPresent()
                ? StreamReadMode.RESET : StreamReadMode.SNAPSHOT;
        return IvmRewriteContext.full(mtmv, resetScopes, nonPctReadMode);
    }

    private Plan normalizeStreamScan(Plan plan) throws Exception {
        return PlanChecker.from(createDefaultCtx(), plan)
                .applyTopDown(new NormalizeOlapTableStreamScan())
                .getCascadesContext().getRewritePlan();
    }

    private void assertHasLiveBaseScan(Plan plan, OlapTable baseTable, List<Long> partitionIds) {
        boolean hasLiveBaseScan = plan.collectToList(LogicalOlapScan.class::isInstance).stream()
                .map(LogicalOlapScan.class::cast)
                .anyMatch(scan -> scan.getTable() == baseTable
                        && scan.getSelectedPartitionIds().equals(partitionIds));
        Assertions.assertTrue(hasLiveBaseScan);
    }

    private LogicalOlapScan newOlapScan(OlapTable table, List<Long> partitionIds) throws Exception {
        StatementScopeIdGenerator.clear();
        return new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of(DB_NAME, table.getName()), partitionIds, ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of());
    }

    private void assertProjectKeepsOriginSlots(LogicalOlapScan scan, LogicalProject<?> project) {
        Assertions.assertEquals(scan.getOutput().size(), project.getOutput().size());
        for (int i = 0; i < scan.getOutput().size(); i++) {
            Assertions.assertEquals(scan.getOutput().get(i).getName(), project.getOutput().get(i).getName());
            Assertions.assertEquals(scan.getOutput().get(i).getExprId(), project.getOutput().get(i).getExprId());
            Assertions.assertEquals(scan.getOutput().get(i).getQualifier(), project.getOutput().get(i).getQualifier());
            Assertions.assertEquals(scan.getOutput().get(i).nullable(), project.getOutput().get(i).nullable(),
                    "nullable mismatch for " + scan.getOutput().get(i).getName());
        }
    }

    private Set<Long> resetPartitionIds(List<Long> partitionIds) {
        return new HashSet<>(partitionIds);
    }

    private String streamName(OlapTable baseTable) {
        return IvmUtil.streamName(MV_ID, baseTable.getFullQualifiers());
    }

    private MTMV mtmv() {
        return mtmv(MV_ID, "test_mv");
    }

    private MTMV mtmv(long id, String name) {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getId()).thenReturn(id);
        Mockito.when(mtmv.getName()).thenReturn(name);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn(DB_NAME);
        Mockito.when(mtmv.getDatabase()).thenReturn(Env.getCurrentInternalCatalog().getDbNullable(DB_NAME));
        Mockito.when(mtmv.getMvPartitionInfo())
                .thenReturn(new MTMVPartitionInfo(MTMVPartitionType.SELF_MANAGE));
        Mockito.when(mtmv.getExcludedTriggerTables()).thenReturn(ImmutableSet.of());
        return mtmv;
    }

    private OlapTable baseTable() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        return (OlapTable) db.getTableOrMetaException(BASE_TABLE_NAME);
    }

}
