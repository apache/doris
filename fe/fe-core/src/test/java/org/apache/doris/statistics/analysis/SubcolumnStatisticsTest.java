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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.ColumnAccessPathType;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.stats.FilterEstimation;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.AlterColumnStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.AnalyzeTableCommand;
import org.apache.doris.nereids.trees.plans.commands.DropStatsCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowColumnStatsCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.analysis.AnalysisInfo.JobType;
import org.apache.doris.statistics.cache.StatisticsCache;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.ColumnStatisticBuilder;
import org.apache.doris.statistics.model.Statistics;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.statistics.util.SubcolumnStatisticsUtil;
import org.apache.doris.statistics.util.SubcolumnStatisticsUtil.SubcolumnDescriptor;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.commons.text.StringSubstitutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubcolumnStatisticsTest extends TestWithFeService {

    private Database db;
    private OlapTable complexTbl;
    private OlapTable unsupportedOnlyTbl;

    @Override
    protected void runBeforeAll() throws Exception {
        InternalSchemaInitializer.createDb();
        createDatabase("subcol_stats_db");
        connectContext.setDatabase("subcol_stats_db");
        FeConstants.enableInternalSchemaDb = true;
        createTable("CREATE TABLE t_complex (\n"
                + "    id INT NOT NULL,\n"
                + "    v VARIANT<'userId': INT, 'userid': STRING, 'profile.city': STRING,\n"
                + "              'dyn*': INT, 'tags': ARRAY<INT>> NULL,\n"
                + "    s STRUCT<age: INT, addr: STRUCT<city: VARCHAR(32), zip: INT>, scores: ARRAY<INT>> NULL,\n"
                + "    arr ARRAY<INT> NULL\n"
                + ") DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");
        createTable("CREATE TABLE t_unsupported (\n"
                + "    id INT NOT NULL,\n"
                + "    raw_v VARIANT NULL,\n"
                + "    arr ARRAY<INT> NULL\n"
                + ") DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");
        db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("subcol_stats_db");
        complexTbl = (OlapTable) db.getTableOrAnalysisException("t_complex");
        unsupportedOnlyTbl = (OlapTable) db.getTableOrAnalysisException("t_unsupported");
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testSubcolumnDiscoveryEscapingAndCollisionAvoidance() {
        List<String> discoveredIds = SubcolumnStatisticsUtil.discoverSubcolumnIds(complexTbl);
        Assertions.assertEquals(
                List.of("v.userId", "v.userid", "v.profile.city", "s.age", "s.addr.city", "s.addr.zip"),
                discoveredIds);

        // Case-sensitive VARIANT keys vs case-insensitive STRUCT fields.
        SubcolumnDescriptor upperVariant = SubcolumnStatisticsUtil.resolve(complexTbl, -1L, "v.userId").orElseThrow();
        SubcolumnDescriptor lowerVariant = SubcolumnStatisticsUtil.resolve(complexTbl, -1L, "v.userid").orElseThrow();
        Assertions.assertEquals(Type.INT, upperVariant.leafType());
        Assertions.assertEquals(Type.STRING, lowerVariant.leafType());
        Assertions.assertEquals("CAST(element_at(`v`, 'userId') AS int)", upperVariant.sqlExpr());
        Assertions.assertEquals(
                "CAST(element_at(element_at(`v`, 'profile'), 'city') AS text)",
                SubcolumnStatisticsUtil.resolve(complexTbl, -1L, "v.profile.city").orElseThrow().sqlExpr());

        SubcolumnDescriptor structZip = SubcolumnStatisticsUtil.resolve(complexTbl, -1L, "S.ADDR.ZIP").orElseThrow();
        Assertions.assertEquals("s.addr.zip", structZip.colId());
        Assertions.assertEquals("element_at(element_at(`s`, 'addr'), 'zip')", structZip.sqlExpr());

        // Escaped dots in VARIANT keys (variant_escape_dots_in_keys = true) produce collision-free %2E colId.
        ArrayList<VariantField> escapedFields = new ArrayList<>();
        escapedFields.add(new VariantField("a%2Eb.c", Type.BIGINT, "", PatternType.MATCH_NAME));
        Column escapedRoot = new Column("v", new VariantType(escapedFields), false, null, true, null, "");
        List<SubcolumnDescriptor> escapedDesc = SubcolumnStatisticsUtil.discoverForColumn(escapedRoot, true);
        Assertions.assertEquals(1, escapedDesc.size());
        Assertions.assertEquals("v.a%2Eb.c", escapedDesc.get(0).colId());
        Assertions.assertEquals(List.of("a.b", "c"), escapedDesc.get(0).subPath());
        Assertions.assertEquals(
                "CAST(element_at(element_at(`v`, 'a.b'), 'c') AS bigint)",
                escapedDesc.get(0).sqlExpr());

        // Physical top-level column with colliding name always wins and suppresses synthetic subcolumn.
        OlapTable mockedTable = Mockito.mock(OlapTable.class);
        MaterializedIndexMeta meta = Mockito.mock(MaterializedIndexMeta.class);
        Column physicalColliding = new Column("s.age", Type.BIGINT, false, null, true, null, "");
        StructType structType = new StructType(new StructField("age", Type.INT), new StructField("name", Type.STRING));
        Column structRoot = new Column("s", structType, false, null, true, null, "");
        Mockito.when(meta.getSchema()).thenReturn(List.of(physicalColliding, structRoot));
        Mockito.when(meta.getColumnByName("s.age")).thenReturn(physicalColliding);
        Mockito.when(mockedTable.getBaseIndexId()).thenReturn(1L);
        Mockito.when(mockedTable.getIndexMetaByIndexId(1L)).thenReturn(meta);
        Assertions.assertEquals(
                List.of("s.name"),
                SubcolumnStatisticsUtil.discover(mockedTable, 1L).stream().map(SubcolumnDescriptor::colId).toList());
        Assertions.assertTrue(SubcolumnStatisticsUtil.resolve(meta, mockedTable, "s.age").isEmpty());
    }

    @Test
    public void testAnalyzeShowDropAndAlterCommandsForSubcolumns() throws Exception {
        NereidsParser parser = new NereidsParser();

        AnalyzeTableCommand analyzeAll = (AnalyzeTableCommand) parser.parseSingle("ANALYZE TABLE t_complex");
        analyzeAll.validate(connectContext);
        Assertions.assertEquals(
                Set.of("id", "v.userId", "v.userid", "v.profile.city", "s.age", "s.addr.city", "s.addr.zip"),
                analyzeAll.getColumnNames());

        // Analyzing complex root columns expands them into their discoverable scalar subcolumns.
        AnalyzeTableCommand analyzeRoots = (AnalyzeTableCommand) parser.parseSingle("ANALYZE TABLE t_complex(v, s)");
        analyzeRoots.validate(connectContext);
        Assertions.assertEquals(
                Set.of("v.userId", "v.userid", "v.profile.city", "s.age", "s.addr.city", "s.addr.zip"),
                analyzeRoots.getColumnNames());

        // Explicit backticked subcolumn IDs validate and normalize cleanly.
        AnalyzeTableCommand analyzeSubcols = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE t_complex(`v.profile.city`, `S.ADDR.ZIP`)");
        analyzeSubcols.validate(connectContext);
        Assertions.assertEquals(Set.of("v.profile.city", "s.addr.zip"), analyzeSubcols.getColumnNames());

        // Unsupported complex types without scalar subcolumns still respect ignore_column_with_complex_type.
        boolean prev = connectContext.getSessionVariable().enableAnalyzeComplexTypeColumn;
        try {
            connectContext.getSessionVariable().enableAnalyzeComplexTypeColumn = true;
            AnalyzeTableCommand badComplex = (AnalyzeTableCommand) parser.parseSingle(
                    "ANALYZE TABLE t_unsupported(raw_v)");
            Assertions.assertThrows(AnalysisException.class, () -> badComplex.validate(connectContext));
        } finally {
            connectContext.getSessionVariable().enableAnalyzeComplexTypeColumn = prev;
        }

        // Unknown subcolumn path is rejected.
        AnalyzeTableCommand missingSubcol = (AnalyzeTableCommand) parser.parseSingle(
                "ANALYZE TABLE t_complex(`v.unknown_field`)");
        Assertions.assertThrows(AnalysisException.class, () -> missingSubcol.validate(connectContext));

        // SHOW COLUMN STATS, DROP STATS, and ALTER COLUMN STATS accept subcolumns and complex roots.
        DropStatsCommand dropRoot = (DropStatsCommand) parser.parseSingle("DROP STATS t_complex(s, `v.userId`)");
        dropRoot.validate(connectContext);
        Assertions.assertEquals(Set.of("s.age", "s.addr.city", "s.addr.zip", "v.userId"), dropRoot.getColumnNames());

        AlterColumnStatsCommand alterSubcol = (AlterColumnStatsCommand) parser.parseSingle(
                "ALTER TABLE t_complex MODIFY COLUMN `s.addr.zip` "
                        + "SET STATS ('row_count'='100', 'ndv'='25', 'num_nulls'='5', "
                        + "'min_value'='10000', 'max_value'='99999', 'data_size'='400')");
        alterSubcol.validate(connectContext);
        Assertions.assertEquals("s.addr.zip", alterSubcol.getColumnName());

        ShowColumnStatsCommand showCmd = (ShowColumnStatsCommand) parser.parseSingle(
                "SHOW COLUMN CACHED STATS t_complex(`v.profile.city`, s)");
        Assertions.assertNotNull(showCmd);
    }

    @Test
    public void testSubcolumnFullAndSampleSqlPlansCleanlyAndUsesInSampleBounds() {
        AnalysisInfo variantTaskInfo = new AnalysisInfoBuilder()
                .setJobId(11L)
                .setTaskId(12L)
                .setCatalogId(Env.getCurrentInternalCatalog().getId())
                .setDBId(db.getId())
                .setTblId(complexTbl.getId())
                .setIndexId(-1L)
                .setColName("v.profile.city")
                .setJobType(JobType.MANUAL)
                .setAnalysisMethod(AnalysisMethod.FULL)
                .setAnalysisType(AnalysisType.FUNDAMENTALS)
                .build();
        OlapAnalysisTask variantTask = new OlapAnalysisTask(variantTaskInfo);
        Assertions.assertEquals(Type.STRING, variantTask.col.getType());

        Map<String, String> fullParams = variantTask.buildSqlParams();
        fullParams.put("hotValueCollectCount", "10");
        fullParams.put("subStringColName", variantTask.getStringTypeColName(variantTask.col));
        String fullSql = new StringSubstitutor(fullParams).replace(TableStatisticsSqlBuilder.FULL_ANALYZE_TEMPLATE);
        Assertions.assertTrue(
                fullSql.contains("CAST(element_at(element_at(`v`, 'profile'), 'city') AS text) AS `v.profile.city`"),
                fullSql);

        // Verify Nereids analyzes, rewrites (including VariantSubPathPruning), and optimizes the full SQL.
        var stmtCtx = MemoTestUtils.createStatementContext(connectContext, fullSql);
        CascadesContext cascades = MemoTestUtils.createCascadesContext(stmtCtx, fullSql);
        var bestPlan = PlanChecker.from(cascades).analyze().rewrite().optimize().getBestPlanTree();
        List<PhysicalOlapScan> scans = bestPlan.collectToList(PhysicalOlapScan.class::isInstance);
        Assertions.assertEquals(1, scans.size());

        // Verify subcolumns always prefer in-sample bounds so ColumnMinMaxBatchCollector never issues
        // a metadata min/max query against non-physical column names.
        AnalysisInfo sampleVarInfo = new AnalysisInfoBuilder(variantTaskInfo)
                .setAnalysisMethod(AnalysisMethod.SAMPLE)
                .setSampleRows(1000L)
                .setRowCount(5000L)
                .build();
        AnalysisInfo sampleStructInfo = new AnalysisInfoBuilder(sampleVarInfo)
                .setTaskId(13L)
                .setColName("s.addr.zip")
                .build();
        OlapAnalysisTask sampleVarTask = new OlapAnalysisTask(sampleVarInfo);
        OlapAnalysisTask sampleStructTask = new OlapAnalysisTask(sampleStructInfo);
        try (MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class)) {
            new ColumnMinMaxBatchCollector(List.of(sampleVarTask, sampleStructTask), 16);
            Assertions.assertNull(sampleVarTask.minMaxBatch);
            Assertions.assertNull(sampleStructTask.minMaxBatch);
            Assertions.assertTrue(executors.constructed().isEmpty());
        }

        Column found = StatisticsUtil.findColumn(
                Env.getCurrentInternalCatalog().getId(), db.getId(), complexTbl.getId(), -1L, "s.addr.zip");
        Assertions.assertNotNull(found);
        Assertions.assertEquals(Type.INT, found.getType());
        Assertions.assertTrue(StatisticsUtil.canCollectColumn(found, complexTbl, true, -1L));
    }

    @Test
    public void testCboStatsDerivationForVariantPrunedSlotsAndStructElementAtChains() throws Exception {
        long ctlId = Env.getCurrentInternalCatalog().getId();
        long dbId = db.getId();
        long tblId = complexTbl.getId();
        StatisticsCache statsCache = Env.getCurrentEnv().getStatisticsCache();

        ColumnStatistic cityStats = new ColumnStatisticBuilder(1000.0)
                .setNdv(20.0)
                .setNumNulls(0.0)
                .setAvgSizeByte(8.0)
                .setDataSize(8000.0)
                .setMinExpr(new StringLiteral("Austin"))
                .setMinValue(StatisticsUtil.convertToDouble(Type.VARCHAR, "Austin"))
                .setMaxExpr(new StringLiteral("Zurich"))
                .setMaxValue(StatisticsUtil.convertToDouble(Type.VARCHAR, "Zurich"))
                .build();
        statsCache.syncColStats(new ColStatsData(
                tblId + "--1-v.profile.city", ctlId, dbId, tblId, -1L, "v.profile.city", null, "", cityStats));

        ColumnStatistic zipStats = new ColumnStatisticBuilder(1000.0)
                .setNdv(50.0)
                .setNumNulls(100.0)
                .setAvgSizeByte(4.0)
                .setDataSize(4000.0)
                .setMinExpr(new IntLiteral(10000))
                .setMinValue(10000.0)
                .setMaxExpr(new IntLiteral(90000))
                .setMaxValue(90000.0)
                .build();
        statsCache.syncColStats(new ColStatsData(
                tblId + "--1-s.addr.zip", ctlId, dbId, tblId, -1L, "s.addr.zip", null, "", zipStats));

        ColumnStatistic ageStats = new ColumnStatisticBuilder(1000.0)
                .setNdv(40.0)
                .setNumNulls(0.0)
                .setAvgSizeByte(4.0)
                .setDataSize(4000.0)
                .setMinExpr(new IntLiteral(18))
                .setMinValue(18.0)
                .setMaxExpr(new IntLiteral(65))
                .setMaxValue(65.0)
                .build();
        statsCache.syncColStats(new ColStatsData(
                tblId + "--1-s.age", ctlId, dbId, tblId, -1L, "s.age", null, "", ageStats));

        // 1. Pruned VARIANT subcolumn SlotReference on LogicalOlapScan + CAST(... AS VARCHAR) preserves bounds & NDV.
        LogicalOlapScan scan = new LogicalOlapScan(
                MemoTestUtils.createStatementContext(connectContext, "SELECT 1").getNextRelationId(),
                complexTbl)
                .withColToSubPathsMap(Map.of("v", Set.of(List.of("profile", "city"))));
        StatsCalculator calc = Mockito.spy(new StatsCalculator(null, false, Map.of(), null));
        try {
            var field = StatsCalculator.class.getDeclaredField("connectContext");
            field.setAccessible(true);
            field.set(calc, connectContext);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
        // Seed table row count in TableStatsMeta so scan population uses 1000 rows.
        AnalysisInfo seeded = new AnalysisInfoBuilder()
                .setJobColumns(Set.of())
                .setRowCount(1000L)
                .setJobType(JobType.MANUAL)
                .setUserInject(true)
                .build();
        seeded.addIndexRowCount(complexTbl.getBaseIndexId(), 1000L);
        Env.getCurrentEnv().getAnalysisManager().updateTableStatsForAlterStats(seeded, complexTbl);

        Statistics scanStats = calc.computeOlapScan(scan);
        Assertions.assertEquals(1000.0, scanStats.getRowCount(), 1e-6);

        SlotReference prunedVariantSlot = scan.getOutput().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .filter(sr -> sr.getSubPath().equals(List.of("profile", "city")))
                .findFirst()
                .orElseThrow();
        ColumnStatistic derivedVariantStats = scanStats.findColumnStatistics(prunedVariantSlot);
        Assertions.assertFalse(derivedVariantStats.isUnKnown);
        Assertions.assertEquals(20.0, derivedVariantStats.ndv, 1e-6);

        Cast castCity = new Cast(prunedVariantSlot, VarcharType.createVarcharType(64));
        ColumnStatistic castCityStats = ExpressionEstimation.estimate(castCity, scanStats);
        Assertions.assertFalse(castCityStats.isUnKnown);
        Assertions.assertEquals(20.0, castCityStats.ndv, 1e-6);
        Assertions.assertNotNull(castCityStats.minExpr);
        Assertions.assertEquals("Austin", castCityStats.minExpr.getStringValue());

        // Equality filter on CAST(v['profile']['city'] AS VARCHAR(64)) = 'Berlin' uses 1/ndv = 1000/20 = 50 rows.
        FilterEstimation filterEstimation = new FilterEstimation();
        Statistics filteredCity = filterEstimation.estimate(
                new EqualTo(castCity, new VarcharLiteral("Berlin")), scanStats);
        Assertions.assertEquals(50.0, filteredCity.getRowCount(), 1e-3);

        // Out-of-range equality ('AAA' < 'Austin') estimates 0 rows because StringLiteral bounds survived CAST.
        Statistics outOfRangeCity = filterEstimation.estimate(
                new EqualTo(castCity, new VarcharLiteral("AAA")), scanStats);
        Assertions.assertEquals(0.0, outOfRangeCity.getRowCount(), 1e-6);

        // 2. Same-type DATE cast preservation in castMinMax.
        ColumnStatistic dateSubcol = new ColumnStatisticBuilder(500.0)
                .setNdv(10.0)
                .setNumNulls(0.0)
                .setAvgSizeByte(4.0)
                .setMinExpr(StatisticsUtil.readableValue(Type.DATEV2, "2025-01-01"))
                .setMinValue(StatisticsUtil.convertToDouble(Type.DATEV2, "2025-01-01"))
                .setMaxExpr(StatisticsUtil.readableValue(Type.DATEV2, "2025-01-10"))
                .setMaxValue(StatisticsUtil.convertToDouble(Type.DATEV2, "2025-01-10"))
                .build();
        SlotReference dateVarSlot = new SlotReference("v", org.apache.doris.nereids.types.VariantType.INSTANCE);
        Map<Expression, ColumnStatistic> dateMap = new HashMap<>();
        dateMap.put(dateVarSlot, dateSubcol);
        Statistics dateCtx = new Statistics(500.0, 1, dateMap);
        ColumnStatistic castDateStats = ExpressionEstimation.estimate(new Cast(dateVarSlot, DateV2Type.INSTANCE), dateCtx);
        Assertions.assertNotNull(castDateStats.minExpr);
        Assertions.assertEquals(10.0, castDateStats.ndv, 1e-6);

        // 3. Nested STRUCT ElementAt chain (by field name AND by 1-based ordinal) resolves cached subcolumn stats.
        SlotReference structSlot = scan.getOutput().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .filter(sr -> "s".equals(sr.getName()) && sr.getSubPath().isEmpty())
                .findFirst()
                .orElseThrow();
        ElementAt addrAccess = new ElementAt(structSlot, new VarcharLiteral("addr"));
        ElementAt zipAccess = new ElementAt(addrAccess, new VarcharLiteral("zip"));
        ColumnStatistic estimatedZip = ExpressionEstimation.estimate(zipAccess, scanStats);
        Assertions.assertFalse(estimatedZip.isUnKnown);
        Assertions.assertEquals(50.0, estimatedZip.ndv, 1e-6);
        Assertions.assertEquals(100.0, estimatedZip.numNulls, 1e-6);

        // 1-based ordinal access element_at(s, 1) -> s.age (ndv = 40).
        ElementAt ordinalAgeAccess = new ElementAt(structSlot, new IntegerLiteral(1));
        ColumnStatistic estimatedAge = ExpressionEstimation.estimate(ordinalAgeAccess, scanStats);
        Assertions.assertFalse(estimatedAge.isUnKnown);
        Assertions.assertEquals(40.0, estimatedAge.ndv, 1e-6);

        // Equality filter on s.addr.zip = 20000 uses (1000 - 100) / 50 = 18 rows!
        Statistics filteredZip = filterEstimation.estimate(
                new EqualTo(zipAccess, new IntegerLiteral(20000)), scanStats);
        Assertions.assertEquals(18.0, filteredZip.getRowCount(), 1e-3);

        // Under filtered context (rowCount = 18), numNulls scales to 1.8 and s.age NDV clamps to <= 18.
        ColumnStatistic scaledAgeAfterFilter = ExpressionEstimation.estimate(ordinalAgeAccess, filteredZip);
        Assertions.assertEquals(18.0, scaledAgeAfterFilter.ndv, 1e-3);
        ColumnStatistic dummyInt = ColumnStatistic.createUnknownByDataType(DataType.fromCatalogType(Type.INT));
        Assertions.assertTrue(dummyInt.isUnKnown);
        Assertions.assertEquals(IntegerType.INSTANCE, zipAccess.getDataType());

        // 4. Container-slot unknown-stats suppression when all accessed subcolumns have known stats.
        SlotReference rootVariantSlot = scan.getOutput().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .filter(sr -> "v".equals(sr.getName()) && sr.getSubPath().isEmpty())
                .findFirst()
                .orElseThrow();
        Assertions.assertTrue(StatsCalculator.isContainerSlotCoveredBySubcolumnStats(
                complexTbl, -1L, List.of(prunedVariantSlot), rootVariantSlot,
                colId -> statsCache.getColumnStatistics(ctlId, dbId, tblId, -1L, colId, connectContext)));
        SlotReference wholeVariantSlot = rootVariantSlot.withAccessPaths(
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("v"))), List.of());
        Assertions.assertFalse(StatsCalculator.isContainerSlotCoveredBySubcolumnStats(
                complexTbl, -1L, List.of(wholeVariantSlot), wholeVariantSlot,
                colId -> statsCache.getColumnStatistics(ctlId, dbId, tblId, -1L, colId, connectContext)));

        SlotReference prunedStructSlot = structSlot.withAccessPaths(
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("s", "addr", "zip"))),
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("s", "addr", "zip"))));
        Assertions.assertTrue(StatsCalculator.isContainerSlotCoveredBySubcolumnStats(
                complexTbl, -1L, List.of(prunedStructSlot), prunedStructSlot,
                colId -> statsCache.getColumnStatistics(ctlId, dbId, tblId, -1L, colId, connectContext)));
        // Field-id external connectors (Iceberg/Paimon) store numeric IDs in allAccessPaths while preserving
        // symbolic field names in displayAllAccessPaths / displayPredicateAccessPaths.
        SlotReference fieldIdStructSlot = structSlot.withAccessPaths(
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("3", "5", "7"))),
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("3", "5", "7"))),
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("s", "addr", "zip"))),
                List.of(new ColumnAccessPath(ColumnAccessPathType.DATA, List.of("s", "addr", "zip"))));
        Assertions.assertTrue(StatsCalculator.isContainerSlotCoveredBySubcolumnStats(
                complexTbl, -1L, List.of(fieldIdStructSlot), fieldIdStructSlot,
                colId -> statsCache.getColumnStatistics(ctlId, dbId, tblId, -1L, colId, connectContext)));
        Assertions.assertFalse(StatsCalculator.isContainerSlotCoveredBySubcolumnStats(
                complexTbl, -1L, List.of(prunedStructSlot), prunedStructSlot,
                colId -> ColumnStatistic.UNKNOWN));
    }
}
