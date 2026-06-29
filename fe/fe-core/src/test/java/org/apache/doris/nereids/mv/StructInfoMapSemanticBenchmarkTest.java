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

package org.apache.doris.nereids.mv;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

/**
 * Benchmark for the MV struct-info path on semantically complex SQL shapes.
 */
class StructInfoMapSemanticBenchmarkTest extends SqlTestBase {
    private static final int ORDINARY_EXPLAIN_REPEAT = 10;
    private static final int ORDINARY_STRUCT_REPEAT = 1000;
    private static final int LARGE_EXPLAIN_REPEAT = 3;
    private static final int LARGE_STRUCT_REPEAT = 5000;
    private static final int SUPER_EXPLAIN_REPEAT = 1;
    private static final int SUPER_STRUCT_REPEAT = 1000;
    private static final int SUPER_COMPLEX_BRANCH_COUNT = 24;
    private static final int ORDINARY_MULTI_ALIAS_JOIN_DIM_COUNT = 4;
    private static final int LARGE_MULTI_ALIAS_JOIN_DIM_COUNT = 8;
    private static final int SUPER_MULTI_ALIAS_JOIN_DIM_COUNT = 12;
    private static final int ORDINARY_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT = 4;
    private static final int LARGE_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT = 12;
    private static final int SUPER_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT = 20;
    private static final int LARGE_EXACT_HIT_WRAPPER_STAGE_COUNT = 10;
    private static final int LARGE_EXACT_HIT_POST_STAGE_COUNT = 8;
    private static final int SUPER_EXACT_HIT_WRAPPER_STAGE_COUNT = 48;
    private static final int SUPER_EXACT_HIT_POST_STAGE_COUNT = 40;

    @Test
    void benchmarkSemanticCases() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        installValidRelationManager();

        connectContext.getState().setIsQuery(true);
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().nereidsTimeoutSecond = 600;
        connectContext.getSessionVariable().materializedViewRewriteDurationThresholdMs = 1000000;

        benchmarkOrdinaryMultiAliasTableJoinTargetMvHit();
        benchmarkLargeMultiAliasTableJoinTargetMvHit();
        if (isSuperComplexBenchmarkEnabled() || shouldBenchmarkScale(BenchmarkScale.SUPER)) {
            benchmarkSuperMultiAliasTableJoinTargetMvHit();
        }
        if (shouldRunAdditionalBenchmarkCases()) {
            prepareAliasSelfJoinOverlappingMvsCase();
            benchmarkOrdinaryExactTargetMvHit();
            benchmarkLargeExactTargetMvHit();
            if (isSuperComplexBenchmarkEnabled() || shouldBenchmarkScale(BenchmarkScale.SUPER)) {
                benchmarkSuperExactTargetMvHit();
            }
            benchmarkCommonTableIdConflictLeftJoinTargetMv();
            benchmarkAliasSelfJoinOverlappingMvs();
            benchmarkAliasPermutationChainTargetMv();
            benchmarkNestedCteAggViewLeftJoinTargetMv();
            benchmarkMultiStageCteAggViewTargetMv();
            benchmarkLargeFanoutCteJoinTargetMv();
            benchmarkManyOverlappingMvsTargetMv();
            if (isSuperComplexBenchmarkEnabled() || shouldBenchmarkScale(BenchmarkScale.SUPER)) {
                benchmarkSuperComplexFanoutCteJoinTargetMv();
            }
        }
    }

    private void benchmarkOrdinaryMultiAliasTableJoinTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "ordinary_multi_alias_table_join_target_mv_hit")) {
            return;
        }
        int dimCount = Integer.getInteger("structInfoMapBenchmarkOrdinaryDimCount",
                ORDINARY_MULTI_ALIAS_JOIN_DIM_COUNT);
        prepareMultiAliasTableJoinCase("ordinary", dimCount,
                Integer.getInteger("structInfoMapBenchmarkOrdinaryOverlapMvCount",
                        ORDINARY_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT));
        benchmarkChosenCase(BenchmarkScale.ORDINARY, "ordinary_multi_alias_table_join_target_mv_hit",
                getMultiAliasTableJoinQuerySql("ordinary", dimCount), "bench_mv_multi_alias_ordinary_target",
                ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkLargeMultiAliasTableJoinTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.LARGE, "large_multi_alias_table_join_target_mv_hit")) {
            return;
        }
        int dimCount = Integer.getInteger("structInfoMapBenchmarkLargeDimCount",
                LARGE_MULTI_ALIAS_JOIN_DIM_COUNT);
        prepareMultiAliasTableJoinCase("large", dimCount,
                Integer.getInteger("structInfoMapBenchmarkLargeOverlapMvCount",
                        LARGE_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT));
        benchmarkChosenCase(BenchmarkScale.LARGE, "large_multi_alias_table_join_target_mv_hit",
                getMultiAliasTableJoinQuerySql("large", dimCount), "bench_mv_multi_alias_large_target",
                LARGE_EXPLAIN_REPEAT, LARGE_STRUCT_REPEAT);
    }

    private void benchmarkSuperMultiAliasTableJoinTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.SUPER, "super_multi_alias_table_join_target_mv_hit")) {
            return;
        }
        int dimCount = Integer.getInteger("structInfoMapBenchmarkSuperDimCount",
                SUPER_MULTI_ALIAS_JOIN_DIM_COUNT);
        prepareMultiAliasTableJoinCase("super", dimCount,
                Integer.getInteger("structInfoMapBenchmarkSuperOverlapMvCount",
                        SUPER_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT));
        benchmarkChosenCase(BenchmarkScale.SUPER, "super_multi_alias_table_join_target_mv_hit",
                getMultiAliasTableJoinQuerySql("super", dimCount), "bench_mv_multi_alias_super_target",
                SUPER_EXPLAIN_REPEAT, SUPER_STRUCT_REPEAT);
    }

    private void benchmarkOrdinaryExactTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "ordinary_exact_target_mv_hit")) {
            return;
        }
        benchmarkChosenCase(BenchmarkScale.ORDINARY, "ordinary_exact_target_mv_hit",
                getExactAliasTargetMvHitQuerySql(0, 0), "bench_mv_alias_target",
                ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkLargeExactTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.LARGE, "large_exact_target_mv_hit")) {
            return;
        }
        benchmarkChosenCase(BenchmarkScale.LARGE, "large_exact_target_mv_hit",
                getExactAliasTargetMvHitQuerySql(
                        Integer.getInteger("structInfoMapBenchmarkLargeWrapperStages",
                                LARGE_EXACT_HIT_WRAPPER_STAGE_COUNT),
                        Integer.getInteger("structInfoMapBenchmarkLargePostStages",
                                LARGE_EXACT_HIT_POST_STAGE_COUNT)),
                "bench_mv_alias_target", LARGE_EXPLAIN_REPEAT, LARGE_STRUCT_REPEAT);
    }

    private void benchmarkSuperExactTargetMvHit() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.SUPER, "super_exact_target_mv_hit")) {
            return;
        }
        benchmarkChosenCase(BenchmarkScale.SUPER, "super_exact_target_mv_hit",
                getExactAliasTargetMvHitQuerySql(
                        Integer.getInteger("structInfoMapBenchmarkSuperWrapperStages",
                                SUPER_EXACT_HIT_WRAPPER_STAGE_COUNT),
                        Integer.getInteger("structInfoMapBenchmarkSuperPostStages",
                                SUPER_EXACT_HIT_POST_STAGE_COUNT)),
                "bench_mv_alias_target", SUPER_EXPLAIN_REPEAT, SUPER_STRUCT_REPEAT);
    }

    private void benchmarkCommonTableIdConflictLeftJoinTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "common_tableid_conflict_left_join_target_mv")) {
            return;
        }
        prepareCommonTableIdConflictCase();
        benchmarkCase(BenchmarkScale.ORDINARY, "common_tableid_conflict_left_join_target_mv",
                getCommonTableIdConflictQuerySql(), "bench_mv_id_conflict_target",
                ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkAliasSelfJoinOverlappingMvs() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "alias_self_join_overlapping_mvs")) {
            return;
        }
        prepareAliasSelfJoinOverlappingMvsCase();
        benchmarkCase(BenchmarkScale.ORDINARY, "alias_self_join_overlapping_mvs",
                "SELECT c.id, p.score AS parent_score, a.flag\n"
                        + "FROM bench_alias_node c\n"
                        + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                        + "LEFT JOIN bench_alias_attr a ON c.id = a.id\n"
                        + "WHERE a.flag = 'Y' AND p.kind = 'P'",
                "bench_mv_alias_target", ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkAliasPermutationChainTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "alias_permutation_chain_target_mv")) {
            return;
        }
        prepareAliasPermutationChainCase();
        benchmarkChosenCase(BenchmarkScale.ORDINARY, "alias_permutation_chain_target_mv",
                getAliasPermutationChainQuerySql(), "bench_mv_alias_chain_target",
                ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkNestedCteAggViewLeftJoinTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "nested_cte_agg_view_left_join_target_mv")) {
            return;
        }
        prepareNestedCteAggViewLeftJoinCase();
        benchmarkCase(BenchmarkScale.ORDINARY, "nested_cte_agg_view_left_join_target_mv",
                "WITH raw AS (\n"
                        + "    SELECT dt, k, amount FROM bench_nested_fact WHERE sku_type = '1'\n"
                        + "), agg AS (\n"
                        + "    SELECT dt, k, SUM(amount) AS amount_sum FROM raw GROUP BY dt, k\n"
                        + ")\n"
                        + "SELECT a.dt, a.k, a.amount_sum, d.region\n"
                        + "FROM agg a\n"
                        + "LEFT JOIN v_bench_nested_dim_active d ON a.dt = d.dt AND a.k = d.k",
                "bench_mv_nested_target", ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkMultiStageCteAggViewTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.ORDINARY, "multi_stage_cte_agg_view_target_mv")) {
            return;
        }
        prepareMultiStageCteAggViewCase();
        benchmarkCase(BenchmarkScale.ORDINARY, "multi_stage_cte_agg_view_target_mv",
                "WITH raw AS (\n"
                        + "    SELECT dt, k, category, amount, qty FROM bench_multi_sales WHERE category = 'A'\n"
                        + "), per_item AS (\n"
                        + "    SELECT dt, k, category, SUM(amount) AS amount_sum, SUM(qty) AS qty_sum\n"
                        + "    FROM raw GROUP BY dt, k, category\n"
                        + "), per_brand AS (\n"
                        + "    SELECT p.dt, p.category, i.brand, SUM(p.amount_sum) AS brand_amount,\n"
                        + "           SUM(p.qty_sum) AS brand_qty\n"
                        + "    FROM per_item p\n"
                        + "    LEFT JOIN v_bench_multi_item i ON p.k = i.k AND p.category = i.category\n"
                        + "    GROUP BY p.dt, p.category, i.brand\n"
                        + ")\n"
                        + "SELECT dt, category, brand, brand_amount, brand_qty FROM per_brand",
                "bench_mv_multi_target", ORDINARY_EXPLAIN_REPEAT, ORDINARY_STRUCT_REPEAT);
    }

    private void benchmarkLargeFanoutCteJoinTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.LARGE, "large_fanout_cte_join_target_mv")) {
            return;
        }
        prepareLargeFanoutCteJoinCase();
        benchmarkCase(BenchmarkScale.LARGE, "large_fanout_cte_join_target_mv", getLargeFanoutCteJoinQuerySql(),
                "bench_mv_large_target", LARGE_EXPLAIN_REPEAT, LARGE_STRUCT_REPEAT);
    }

    private void benchmarkManyOverlappingMvsTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.LARGE, "many_overlapping_mvs_target_mv")) {
            return;
        }
        prepareManyOverlappingMvsCase();
        benchmarkChosenCase(BenchmarkScale.LARGE, "many_overlapping_mvs_target_mv",
                getExactTargetMvHitQuerySql(
                        Integer.getInteger("structInfoMapBenchmarkLargeWrapperStages",
                                LARGE_EXACT_HIT_WRAPPER_STAGE_COUNT),
                        Integer.getInteger("structInfoMapBenchmarkLargePostStages",
                                LARGE_EXACT_HIT_POST_STAGE_COUNT)),
                "bench_mv_large_target", LARGE_EXPLAIN_REPEAT, LARGE_STRUCT_REPEAT);
    }

    private void benchmarkSuperComplexFanoutCteJoinTargetMv() throws Exception {
        if (!shouldBenchmark(BenchmarkScale.SUPER, "super_complex_fanout_cte_join_target_mv")) {
            return;
        }
        prepareLargeFanoutCteJoinCase();
        benchmarkCase(BenchmarkScale.SUPER, "super_complex_fanout_cte_join_target_mv",
                getSuperComplexFanoutCteJoinQuerySql(), "bench_mv_large_target",
                SUPER_EXPLAIN_REPEAT, SUPER_STRUCT_REPEAT);
    }

    private void benchmarkCase(BenchmarkScale scale, String caseName, String querySql, String targetMvName,
            int explainRepeat, int structRepeat) throws Exception {
        maybeDumpQuerySql(caseName, querySql);
        int actualExplainRepeat = getBenchmarkRepeat("structInfoMapBenchmarkExplainRepeat", explainRepeat);
        int actualStructRepeat = getBenchmarkRepeat("structInfoMapBenchmarkHotRepeat", structRepeat);
        if (!isHotOnlyBenchmark()) {
            benchmarkExplainCase(scale, caseName, querySql, actualExplainRepeat);
        }
        if (!isExplainOnlyBenchmark()) {
            benchmarkStructInfoCase(scale, caseName, querySql, targetMvName, actualStructRepeat);
        }
    }

    private void benchmarkChosenCase(BenchmarkScale scale, String caseName, String querySql, String targetMvName,
            int explainRepeat, int structRepeat) throws Exception {
        maybeDumpQuerySql(caseName, querySql);
        int actualExplainRepeat = getBenchmarkRepeat("structInfoMapBenchmarkExplainRepeat", explainRepeat);
        int actualStructRepeat = getBenchmarkRepeat("structInfoMapBenchmarkHotRepeat", structRepeat);
        if (!isHotOnlyBenchmark()) {
            benchmarkChosenExplainCase(scale, caseName, querySql, targetMvName, actualExplainRepeat);
        }
        if (!isExplainOnlyBenchmark()) {
            benchmarkStructInfoCase(scale, caseName, querySql, targetMvName, actualStructRepeat);
        }
    }

    private void benchmarkExplainCase(BenchmarkScale scale, String caseName, String querySql, int repeat)
            throws Exception {
        ExplainMeasureResult first = measureExplain(querySql);
        maybeDumpExplain(caseName, "first", first.explain);
        ExplainMeasureResult second = measureExplain(querySql);
        maybeDumpExplain(caseName, "second", second.explain);
        long steadyNanos = 0;
        int planChars = second.planChars;
        for (int i = 0; i < repeat; i++) {
            ExplainMeasureResult steady = measureExplain(querySql);
            maybeDumpExplain(caseName, "steady_" + i, steady.explain);
            steadyNanos += steady.elapsedNanos;
            planChars = steady.planChars;
        }

        appendExplainBenchmarkResult(scale, caseName, nanosToMillis(first.elapsedNanos),
                nanosToMillis(second.elapsedNanos), nanosToMillis(steadyNanos / repeat),
                nanosToMillis(steadyNanos), planChars);
    }

    private void benchmarkChosenExplainCase(BenchmarkScale scale, String caseName, String querySql, String targetMvName,
            int repeat) throws Exception {
        ExplainMeasureResult first = measureExplain(querySql);
        assertTargetMvChosen(caseName, targetMvName, first.explain);
        maybeDumpExplain(caseName, "first", first.explain);
        ExplainMeasureResult second = measureExplain(querySql);
        assertTargetMvChosen(caseName, targetMvName, second.explain);
        maybeDumpExplain(caseName, "second", second.explain);
        long steadyNanos = 0;
        int planChars = second.planChars;
        for (int i = 0; i < repeat; i++) {
            ExplainMeasureResult steady = measureExplain(querySql);
            assertTargetMvChosen(caseName, targetMvName, steady.explain);
            maybeDumpExplain(caseName, "steady_" + i, steady.explain);
            steadyNanos += steady.elapsedNanos;
            planChars = steady.planChars;
        }

        appendExplainBenchmarkResult(scale, caseName, nanosToMillis(first.elapsedNanos),
                nanosToMillis(second.elapsedNanos), nanosToMillis(steadyNanos / repeat),
                nanosToMillis(steadyNanos), planChars);
    }

    private ExplainMeasureResult measureExplain(String querySql) throws Exception {
        long start = System.nanoTime();
        String explain = getSQLPlanOrErrorMsg("EXPLAIN " + querySql);
        return new ExplainMeasureResult(System.nanoTime() - start, explain);
    }

    private void assertTargetMvChosen(String caseName, String targetMvName, String explain) {
        Assertions.assertTrue(explain.contains(targetMvName + " chose"),
                String.format(Locale.ROOT, "case %s should choose target mv %s, explain is:%n%s",
                        caseName, targetMvName, explain));
    }

    private void benchmarkStructInfoCase(BenchmarkScale scale, String caseName, String querySql, String targetMvName,
            int repeat) throws Exception {
        CascadesContext cascadesContext = createCascadesContext(querySql, connectContext);
        PlanChecker.from(cascadesContext)
                .setIsQuery()
                .analyze()
                .rewrite();

        MaterializationContext targetContext = findMaterializationContext(cascadesContext, targetMvName);
        Assertions.assertNotNull(targetContext, "target materialization context should exist: " + targetMvName);
        Plan rootPlan = cascadesContext.getMemo().getRoot().getFirstLogicalExpression().getPlan();
        BitSet mvBaseTableIdSet = getMvBaseTableIdSet(targetContext, cascadesContext);

        MeasureResult first = measure(rootPlan, cascadesContext, mvBaseTableIdSet);
        MeasureResult second = measure(rootPlan, cascadesContext, mvBaseTableIdSet);
        long steadyNanos = 0;
        int structInfoCount = second.structInfoCount;
        for (int i = 0; i < repeat; i++) {
            MeasureResult steady = measure(rootPlan, cascadesContext, mvBaseTableIdSet);
            steadyNanos += steady.elapsedNanos;
            structInfoCount = steady.structInfoCount;
        }

        appendBenchmarkResult(scale, caseName, nanosToMillis(first.elapsedNanos), nanosToMillis(second.elapsedNanos),
                nanosToMillis(steadyNanos / repeat), nanosToMillis(steadyNanos), structInfoCount);
    }

    private BitSet getMvBaseTableIdSet(MaterializationContext targetContext, CascadesContext cascadesContext)
            throws Exception {
        try {
            return (BitSet) MaterializationContext.class
                    .getMethod("getBaseTableIdSet")
                    .invoke(targetContext);
        } catch (NoSuchMethodException noBaseTableIdSetMethod) {
            StatementContext statementContext = cascadesContext.getStatementContext();
            return (BitSet) MaterializationContext.class
                    .getMethod("getCommonTableIdSet", StatementContext.class)
                    .invoke(targetContext, statementContext);
        }
    }

    private boolean shouldBenchmark(BenchmarkScale scale, String caseName) {
        String caseFilter = System.getProperty("structInfoMapBenchmarkCase");
        boolean caseMatched = caseFilter == null || caseFilter.isEmpty()
                || caseName.contains(caseFilter)
                || scale.name().equalsIgnoreCase(caseFilter);
        return caseMatched && shouldBenchmarkScale(scale);
    }

    private boolean shouldBenchmarkScale(BenchmarkScale scale) {
        String scaleFilter = System.getProperty("structInfoMapBenchmarkScale");
        if ("all".equalsIgnoreCase(scaleFilter)) {
            return true;
        }
        if (scaleFilter == null || scaleFilter.isEmpty()) {
            return scale != BenchmarkScale.SUPER || isSuperComplexBenchmarkEnabled();
        }
        return scale.name().equalsIgnoreCase(scaleFilter);
    }

    private boolean isSuperComplexBenchmarkEnabled() {
        String caseFilter = System.getProperty("structInfoMapBenchmarkCase");
        return Boolean.getBoolean("structInfoMapBenchmarkSuperComplex")
                || (caseFilter != null && "super".equalsIgnoreCase(caseFilter))
                || "super".equalsIgnoreCase(System.getProperty("structInfoMapBenchmarkScale"));
    }

    private boolean shouldRunAdditionalBenchmarkCases() {
        String caseFilter = System.getProperty("structInfoMapBenchmarkCase");
        return Boolean.getBoolean("structInfoMapBenchmarkAdditionalCases")
                || (caseFilter != null && !caseFilter.isEmpty());
    }

    private boolean isHotOnlyBenchmark() {
        return Boolean.getBoolean("structInfoMapBenchmarkHotOnly");
    }

    private boolean isExplainOnlyBenchmark() {
        return Boolean.getBoolean("structInfoMapBenchmarkExplainOnly");
    }

    private int getBenchmarkRepeat(String propertyName, int defaultRepeat) {
        Integer commonRepeat = Integer.getInteger("structInfoMapBenchmarkRepeat");
        return Integer.getInteger(propertyName, commonRepeat == null ? defaultRepeat : commonRepeat);
    }

    private MeasureResult measure(Plan plan, CascadesContext cascadesContext, BitSet mvBaseTableIdSet) {
        long start = System.nanoTime();
        List<StructInfo> structInfos = MaterializedViewUtils.extractStructInfoFuzzy(
                plan, plan, cascadesContext, mvBaseTableIdSet);
        return new MeasureResult(System.nanoTime() - start, structInfos.size());
    }

    private MaterializationContext findMaterializationContext(CascadesContext cascadesContext, String mvName) {
        return cascadesContext.getMaterializationContexts().stream()
                .filter(ctx -> mvName.equals(ctx.generateMaterializationIdentifier()
                        .get(ctx.generateMaterializationIdentifier().size() - 1)))
                .findFirst()
                .orElse(null);
    }

    private void maybeDumpQuerySql(String caseName, String querySql) throws IOException {
        String outputDir = System.getProperty("structInfoMapBenchmarkSqlOutputDir");
        if (outputDir == null || outputDir.isEmpty()) {
            return;
        }
        Files.createDirectories(Paths.get(outputDir));
        Files.write(Paths.get(outputDir, caseName + ".sql"), querySql.getBytes(StandardCharsets.UTF_8));
    }

    private void maybeDumpExplain(String caseName, String label, String explain) throws IOException {
        String outputDir = System.getProperty("structInfoMapBenchmarkExplainOutputDir");
        if (outputDir == null || outputDir.isEmpty()) {
            return;
        }
        Files.createDirectories(Paths.get(outputDir));
        Files.write(Paths.get(outputDir, caseName + "_" + label + ".explain"),
                explain.getBytes(StandardCharsets.UTF_8));
    }

    private void appendBenchmarkResult(BenchmarkScale scale, String caseName, double firstMs, double secondMs,
            double steadyAvgMs, double steadyTotalMs, int structInfoCount) throws IOException {
        String line = String.format(Locale.ROOT,
                "STRUCT_INFO_MAP_BENCH scale=%s case=%s firstMs=%.6f secondMs=%.6f "
                        + "steadyAvgMs=%.6f steadyTotalMs=%.3f structInfoCount=%d%n",
                scale.name().toLowerCase(Locale.ROOT), caseName, firstMs, secondMs, steadyAvgMs, steadyTotalMs,
                structInfoCount);
        System.out.print(line);
        String output = System.getProperty("structInfoMapBenchmarkOutput");
        if (output == null || output.isEmpty()) {
            return;
        }
        Files.write(Paths.get(output), line.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void appendExplainBenchmarkResult(BenchmarkScale scale, String caseName, double firstMs, double secondMs,
            double steadyAvgMs, double steadyTotalMs, int planChars) throws IOException {
        String line = String.format(Locale.ROOT,
                "STRUCT_INFO_MAP_EXPLAIN_BENCH scale=%s case=%s firstMs=%.3f secondMs=%.3f "
                        + "steadyAvgMs=%.3f steadyTotalMs=%.3f planChars=%d%n",
                scale.name().toLowerCase(Locale.ROOT), caseName, firstMs, secondMs, steadyAvgMs, steadyTotalMs,
                planChars);
        System.out.print(line);
        String output = System.getProperty("structInfoMapBenchmarkOutput");
        if (output == null || output.isEmpty()) {
            return;
        }
        Files.write(Paths.get(output), line.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private double nanosToMillis(long nanos) {
        return nanos / 1_000_000D;
    }

    private void prepareMultiAliasTableJoinCase(String scaleName, int dimCount, int overlapMvCount)
            throws Exception {
        String prefix = "bench_multi_alias_" + scaleName;
        String mvPrefix = "bench_mv_multi_alias_" + scaleName;
        String factTable = prefix + "_fact";
        String dimTable = prefix + "_dim";
        String attrTable = prefix + "_attr";
        String attrView = "v_" + prefix + "_attr_active";

        dropMvByNereids("drop materialized view if exists " + mvPrefix + "_target");
        dropMvByNereids("drop materialized view if exists " + mvPrefix + "_fact_detail");
        dropMvByNereids("drop materialized view if exists " + mvPrefix + "_attr_active");
        int maxOverlapMvCount = Math.max(overlapMvCount, SUPER_MULTI_ALIAS_JOIN_OVERLAP_MV_COUNT);
        for (int i = 0; i < maxOverlapMvCount; i++) {
            dropMvByNereids("drop materialized view if exists " + mvPrefix + "_overlap_" + i);
        }
        dropView("drop view if exists " + attrView);

        createTable("CREATE TABLE IF NOT EXISTS " + factTable + " (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    sku_type VARCHAR(8),\n"
                + "    category VARCHAR(8),\n"
                + "    amount BIGINT,\n"
                + "    qty BIGINT\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS " + dimTable + " (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    sku_type VARCHAR(8),\n"
                + "    dim_type VARCHAR(16),\n"
                + "    dim_value VARCHAR(32),\n"
                + "    active VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k, sku_type, dim_type)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS " + attrTable + " (\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    category VARCHAR(8),\n"
                + "    attr_type VARCHAR(16),\n"
                + "    brand VARCHAR(32),\n"
                + "    owner_name VARCHAR(32),\n"
                + "    active VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(k, category, attr_type)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        for (int i = 0; i < dimCount; i++) {
            createTable("CREATE TABLE IF NOT EXISTS " + prefix + "_lookup_" + i + " (\n"
                    + "    k VARCHAR(32) NOT NULL,\n"
                    + "    category VARCHAR(8),\n"
                    + "    lookup_value VARCHAR(32)\n"
                    + ")\n"
                    + "DUPLICATE KEY(k, category)\n"
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')");
        }
        createView("CREATE VIEW " + attrView + " AS\n"
                + "SELECT k, category, attr_type, brand, owner_name\n"
                + "FROM " + attrTable + "\n"
                + "WHERE active = 'Y'");

        createMvByNereids("CREATE MATERIALIZED VIEW " + mvPrefix + "_fact_detail\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, sku_type, category, amount, qty\n"
                + "FROM " + factTable + "\n"
                + "WHERE sku_type = '1'");
        mockCandidateMtmv(mvPrefix + "_fact_detail");
        createMvByNereids("CREATE MATERIALIZED VIEW " + mvPrefix + "_attr_active\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT k, category, attr_type, brand, owner_name\n"
                + "FROM " + attrView);
        mockCandidateMtmv(mvPrefix + "_attr_active");

        for (int i = 0; i < overlapMvCount; i++) {
            int dimIndex = i % dimCount;
            createMvByNereids("CREATE MATERIALIZED VIEW " + mvPrefix + "_overlap_" + i + "\n"
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "AS SELECT f.dt, f.k, f.category,\n"
                    + "          d.dim_value AS dim_value_" + dimIndex + ",\n"
                    + "          l.lookup_value AS lookup_value_" + dimIndex + "\n"
                    + "FROM " + factTable + " f\n"
                    + "LEFT JOIN " + dimTable + " d\n"
                    + "    ON f.dt = d.dt AND f.k = d.k AND f.sku_type = d.sku_type\n"
                    + "    AND d.dim_type = 'D" + dimIndex + "'\n"
                    + "LEFT JOIN " + prefix + "_lookup_" + dimIndex + " l\n"
                    + "    ON f.k = l.k AND f.category = l.category\n"
                    + "WHERE f.sku_type = '1'");
            mockCandidateMtmv(mvPrefix + "_overlap_" + i);
        }

        createMvByNereids("CREATE MATERIALIZED VIEW " + mvPrefix + "_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS " + getMultiAliasTableJoinQuerySql(scaleName, dimCount));
        mockCandidateMtmv(mvPrefix + "_target");
    }

    private String getMultiAliasTableJoinQuerySql(String scaleName, int dimCount) {
        String prefix = "bench_multi_alias_" + scaleName;
        String factTable = prefix + "_fact";
        String dimTable = prefix + "_dim";
        String attrView = "v_" + prefix + "_attr_active";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT f.dt, f.k, f.category, f.amount, f.qty");
        for (int i = 0; i < dimCount; i++) {
            sql.append(",\n")
                    .append("       d").append(i).append(".dim_value AS dim_value_").append(i);
        }
        for (int i = 0; i < dimCount; i++) {
            sql.append(",\n")
                    .append("       l").append(i).append(".lookup_value AS lookup_value_").append(i);
        }
        sql.append(",\n")
                .append("       a0.brand AS primary_brand,\n")
                .append("       a1.owner_name AS secondary_owner\n")
                .append("FROM ").append(factTable).append(" f\n");
        for (int i = 0; i < dimCount; i++) {
            sql.append("LEFT JOIN ").append(dimTable).append(" d").append(i).append("\n")
                    .append("    ON f.dt = d").append(i).append(".dt AND f.k = d").append(i).append(".k\n")
                    .append("    AND f.sku_type = d").append(i).append(".sku_type\n")
                    .append("    AND d").append(i).append(".dim_type = 'D").append(i).append("'\n");
        }
        for (int i = 0; i < dimCount; i++) {
            sql.append("LEFT JOIN ").append(prefix).append("_lookup_").append(i).append(" l").append(i).append("\n")
                    .append("    ON f.k = l").append(i).append(".k AND f.category = l").append(i).append(".category\n");
        }
        sql.append("LEFT JOIN ").append(attrView).append(" a0\n")
                .append("    ON f.k = a0.k AND f.category = a0.category AND a0.attr_type = 'A'\n")
                .append("LEFT JOIN ").append(attrView).append(" a1\n")
                .append("    ON f.k = a1.k AND f.category = a1.category AND a1.attr_type = 'B'\n")
                .append("WHERE f.sku_type = '1'");
        return sql.toString();
    }

    private void prepareCommonTableIdConflictCase() throws Exception {
        dropMvByNereids("drop materialized view if exists bench_mv_id_conflict_target");
        dropMvByNereids("drop materialized view if exists bench_mv_id_conflict_dim_full_view_non_double");
        dropMvByNereids("drop materialized view if exists bench_mv_id_conflict_dim_full");
        dropMvByNereids("drop materialized view if exists bench_mv_id_conflict_fact");
        dropView("drop view if exists v_bench_id_conflict_dim_full_non_double");

        createTable("CREATE TABLE IF NOT EXISTS bench_id_conflict_fact_src (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    is_dyn VARCHAR(8),\n"
                + "    sku_type VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_id_conflict_dim_full_base (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    sku_type VARCHAR(8),\n"
                + "    is_dyn VARCHAR(8),\n"
                + "    bu VARCHAR(32),\n"
                + "    mode_flag VARCHAR(8),\n"
                + "    double_flag VARCHAR(8)\n"
                + ")\n"
                + "UNIQUE KEY(dt, k, sku_type, is_dyn)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createView("CREATE VIEW v_bench_id_conflict_dim_full_non_double AS\n"
                + "SELECT dt, k, mode_flag, sku_type\n"
                + "FROM bench_id_conflict_dim_full_base\n"
                + "WHERE double_flag = '0'");

        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_id_conflict_fact\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, is_dyn, sku_type\n"
                + "FROM bench_id_conflict_fact_src\n"
                + "WHERE sku_type = '1'");
        mockCandidateMtmv("bench_mv_id_conflict_fact");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_id_conflict_dim_full\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, bu, is_dyn, sku_type\n"
                + "FROM bench_id_conflict_dim_full_base");
        mockCandidateMtmv("bench_mv_id_conflict_dim_full");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_id_conflict_dim_full_view_non_double\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, mode_flag, sku_type\n"
                + "FROM v_bench_id_conflict_dim_full_non_double");
        mockCandidateMtmv("bench_mv_id_conflict_dim_full_view_non_double");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_id_conflict_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT t.dt, t.k, d0.bu AS out_bu, d1.mode_flag AS out_mode\n"
                + "FROM bench_id_conflict_fact_src t\n"
                + "LEFT JOIN bench_id_conflict_dim_full_base d0\n"
                + "    ON t.dt = d0.dt AND t.k = d0.k\n"
                + "    AND t.sku_type = d0.sku_type AND t.is_dyn = d0.is_dyn\n"
                + "LEFT JOIN v_bench_id_conflict_dim_full_non_double d1\n"
                + "    ON t.dt = d1.dt AND t.k = d1.k AND t.sku_type = d1.sku_type\n"
                + "WHERE t.sku_type = '1'");
        mockCandidateMtmv("bench_mv_id_conflict_target");
    }

    private String getCommonTableIdConflictQuerySql() {
        return "SELECT t.dt, t.k, d0.bu AS out_bu, d1.mode_flag AS out_mode\n"
                + "FROM bench_id_conflict_fact_src t\n"
                + "LEFT JOIN bench_id_conflict_dim_full_base d0\n"
                + "    ON t.dt = d0.dt AND t.k = d0.k\n"
                + "    AND t.sku_type = d0.sku_type AND t.is_dyn = d0.is_dyn\n"
                + "LEFT JOIN v_bench_id_conflict_dim_full_non_double d1\n"
                + "    ON t.dt = d1.dt AND t.k = d1.k AND t.sku_type = d1.sku_type\n"
                + "WHERE t.dt = '2026-02-04' AND t.sku_type = '1'";
    }

    private void prepareAliasSelfJoinOverlappingMvsCase() throws Exception {
        dropMvByNereids("drop materialized view if exists bench_mv_alias_chain_target");
        dropMvByNereids("drop materialized view if exists bench_mv_alias_chain_grandparent");
        dropMvByNereids("drop materialized view if exists bench_mv_alias_chain_parent");
        dropMvByNereids("drop materialized view if exists bench_mv_alias_target");
        dropMvByNereids("drop materialized view if exists bench_mv_alias_child");

        createTable("CREATE TABLE IF NOT EXISTS bench_alias_node (\n"
                + "    id BIGINT NOT NULL,\n"
                + "    parent_id BIGINT,\n"
                + "    score BIGINT,\n"
                + "    kind VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_alias_attr (\n"
                + "    id BIGINT NOT NULL,\n"
                + "    flag VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");

        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_alias_child\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT n.id, n.parent_id, n.score, a.flag\n"
                + "FROM bench_alias_node n\n"
                + "LEFT JOIN bench_alias_attr a ON n.id = a.id\n"
                + "WHERE a.flag = 'Y'");
        mockCandidateMtmv("bench_mv_alias_child");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_alias_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT c.id, p.score AS parent_score, a.flag\n"
                + "FROM bench_alias_node c\n"
                + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                + "LEFT JOIN bench_alias_attr a ON c.id = a.id\n"
                + "WHERE a.flag = 'Y' AND p.kind = 'P'");
        mockCandidateMtmv("bench_mv_alias_target");
    }

    private void prepareAliasPermutationChainCase() throws Exception {
        prepareAliasSelfJoinOverlappingMvsCase();
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_alias_chain_parent\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT c.id, p.id AS parent_id, p.score AS parent_score\n"
                + "FROM bench_alias_node c\n"
                + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                + "WHERE p.kind = 'P'");
        mockCandidateMtmv("bench_mv_alias_chain_parent");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_alias_chain_grandparent\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT c.id, p.id AS parent_id, gp.id AS grandparent_id,\n"
                + "          gp.score AS grandparent_score\n"
                + "FROM bench_alias_node c\n"
                + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                + "LEFT JOIN bench_alias_node gp ON p.parent_id = gp.id\n"
                + "WHERE p.kind = 'P' AND gp.kind = 'G'");
        mockCandidateMtmv("bench_mv_alias_chain_grandparent");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_alias_chain_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS " + getAliasPermutationChainQuerySql());
        mockCandidateMtmv("bench_mv_alias_chain_target");
    }

    private String getAliasPermutationChainQuerySql() {
        return "SELECT c.id, p.score AS parent_score, gp.score AS grandparent_score,\n"
                + "       root.kind AS root_kind, a.flag\n"
                + "FROM bench_alias_node c\n"
                + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                + "LEFT JOIN bench_alias_node gp ON p.parent_id = gp.id\n"
                + "LEFT JOIN bench_alias_node root ON gp.parent_id = root.id\n"
                + "LEFT JOIN bench_alias_attr a ON c.id = a.id\n"
                + "WHERE a.flag = 'Y' AND p.kind = 'P' AND gp.kind = 'G'";
    }

    private void prepareNestedCteAggViewLeftJoinCase() throws Exception {
        dropMvByNereids("drop materialized view if exists bench_mv_nested_target");
        dropMvByNereids("drop materialized view if exists bench_mv_nested_fact_agg");
        dropView("drop view if exists v_bench_nested_dim_active");

        createTable("CREATE TABLE IF NOT EXISTS bench_nested_fact (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    amount BIGINT,\n"
                + "    sku_type VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_nested_dim (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    region VARCHAR(32),\n"
                + "    active VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createView("CREATE VIEW v_bench_nested_dim_active AS\n"
                + "SELECT dt, k, region FROM bench_nested_dim WHERE active = 'Y'");

        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_nested_fact_agg\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, SUM(amount) AS amount_sum\n"
                + "FROM bench_nested_fact\n"
                + "WHERE sku_type = '1'\n"
                + "GROUP BY dt, k");
        mockCandidateMtmv("bench_mv_nested_fact_agg");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_nested_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT f.dt, f.k, SUM(f.amount) AS amount_sum, d.region\n"
                + "FROM bench_nested_fact f\n"
                + "LEFT JOIN v_bench_nested_dim_active d ON f.dt = d.dt AND f.k = d.k\n"
                + "WHERE f.sku_type = '1'\n"
                + "GROUP BY f.dt, f.k, d.region");
        mockCandidateMtmv("bench_mv_nested_target");
    }

    private void prepareMultiStageCteAggViewCase() throws Exception {
        dropMvByNereids("drop materialized view if exists bench_mv_multi_target");
        dropMvByNereids("drop materialized view if exists bench_mv_multi_stage_base");
        dropView("drop view if exists v_bench_multi_item");

        createTable("CREATE TABLE IF NOT EXISTS bench_multi_sales (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    category VARCHAR(8),\n"
                + "    amount BIGINT,\n"
                + "    qty BIGINT\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_multi_item (\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    category VARCHAR(8),\n"
                + "    brand VARCHAR(32)\n"
                + ")\n"
                + "DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createView("CREATE VIEW v_bench_multi_item AS\n"
                + "SELECT k, category, brand FROM bench_multi_item WHERE category = 'A'");

        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_multi_stage_base\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, category, SUM(amount) AS amount_sum, SUM(qty) AS qty_sum\n"
                + "FROM bench_multi_sales\n"
                + "WHERE category = 'A'\n"
                + "GROUP BY dt, k, category");
        mockCandidateMtmv("bench_mv_multi_stage_base");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_multi_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(category) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT s.dt, s.category, i.brand, SUM(s.amount) AS brand_amount,\n"
                + "          SUM(s.qty) AS brand_qty\n"
                + "FROM bench_multi_sales s\n"
                + "LEFT JOIN v_bench_multi_item i ON s.k = i.k AND s.category = i.category\n"
                + "WHERE s.category = 'A'\n"
                + "GROUP BY s.dt, s.category, i.brand");
        mockCandidateMtmv("bench_mv_multi_target");
    }

    private void prepareLargeFanoutCteJoinCase() throws Exception {
        dropMvByNereids("drop materialized view if exists bench_mv_large_target");
        dropManyOverlappingMvs();
        dropMvByNereids("drop materialized view if exists bench_mv_large_join_partial");
        dropMvByNereids("drop materialized view if exists bench_mv_large_attr_active");
        dropMvByNereids("drop materialized view if exists bench_mv_large_dim_full_view_non_double");
        dropMvByNereids("drop materialized view if exists bench_mv_large_dim_active");
        dropMvByNereids("drop materialized view if exists bench_mv_large_fact_agg");
        dropMvByNereids("drop materialized view if exists bench_mv_large_fact_detail");
        dropView("drop view if exists v_bench_large_attr_active");
        dropView("drop view if exists v_bench_large_dim_full_non_double");
        dropView("drop view if exists v_bench_large_dim_active");

        createTable("CREATE TABLE IF NOT EXISTS bench_large_fact (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    sku_type VARCHAR(8),\n"
                + "    is_dyn VARCHAR(8),\n"
                + "    category VARCHAR(8),\n"
                + "    amount BIGINT,\n"
                + "    qty BIGINT\n"
                + ")\n"
                + "DUPLICATE KEY(dt, k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_large_dim (\n"
                + "    dt DATE NOT NULL,\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    sku_type VARCHAR(8),\n"
                + "    is_dyn VARCHAR(8),\n"
                + "    region VARCHAR(32),\n"
                + "    bu VARCHAR(32),\n"
                + "    mode_flag VARCHAR(8),\n"
                + "    double_flag VARCHAR(8),\n"
                + "    active VARCHAR(8)\n"
                + ")\n"
                + "UNIQUE KEY(dt, k, sku_type, is_dyn)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createTable("CREATE TABLE IF NOT EXISTS bench_large_attr (\n"
                + "    k VARCHAR(32) NOT NULL,\n"
                + "    category VARCHAR(8),\n"
                + "    brand VARCHAR(32),\n"
                + "    owner_name VARCHAR(32),\n"
                + "    active VARCHAR(8)\n"
                + ")\n"
                + "DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");

        createView("CREATE VIEW v_bench_large_dim_active AS\n"
                + "SELECT dt, k, sku_type, is_dyn, region, bu, mode_flag\n"
                + "FROM bench_large_dim WHERE active = 'Y'");
        createView("CREATE VIEW v_bench_large_dim_full_non_double AS\n"
                + "SELECT dt, k, sku_type, is_dyn, region, mode_flag\n"
                + "FROM bench_large_dim WHERE double_flag = '0'");
        createView("CREATE VIEW v_bench_large_attr_active AS\n"
                + "SELECT k, category, brand, owner_name\n"
                + "FROM bench_large_attr WHERE active = 'Y'");

        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_fact_detail\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, sku_type, is_dyn, category, amount, qty\n"
                + "FROM bench_large_fact\n"
                + "WHERE sku_type = '1'");
        mockCandidateMtmv("bench_mv_large_fact_detail");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_fact_agg\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, sku_type, is_dyn, category,\n"
                + "          SUM(amount) AS amount_sum, SUM(qty) AS qty_sum\n"
                + "FROM bench_large_fact\n"
                + "WHERE sku_type = '1'\n"
                + "GROUP BY dt, k, sku_type, is_dyn, category");
        mockCandidateMtmv("bench_mv_large_fact_agg");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_dim_full_view_non_double\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT dt, k, sku_type, is_dyn, region, mode_flag\n"
                + "FROM v_bench_large_dim_full_non_double");
        mockCandidateMtmv("bench_mv_large_dim_full_view_non_double");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_join_partial\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT f.dt, f.k, f.category, d.region, a.brand\n"
                + "FROM bench_large_fact f\n"
                + "LEFT JOIN v_bench_large_dim_active d\n"
                + "    ON f.dt = d.dt AND f.k = d.k AND f.sku_type = d.sku_type\n"
                + "    AND f.is_dyn = d.is_dyn\n"
                + "LEFT JOIN v_bench_large_attr_active a\n"
                + "    ON f.k = a.k AND f.category = a.category\n"
                + "WHERE f.sku_type = '1'");
        mockCandidateMtmv("bench_mv_large_join_partial");
        createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_target\n"
                + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT f.dt, f.k, f.category,\n"
                + "          SUM(f.amount) AS amount_sum, SUM(f.qty) AS qty_sum,\n"
                + "          d0.region, d1.mode_flag, d2.bu, a.brand\n"
                + "FROM bench_large_fact f\n"
                + "LEFT JOIN bench_large_dim d0\n"
                + "    ON f.dt = d0.dt AND f.k = d0.k AND f.sku_type = d0.sku_type\n"
                + "    AND f.is_dyn = d0.is_dyn\n"
                + "LEFT JOIN v_bench_large_dim_full_non_double d1\n"
                + "    ON f.dt = d1.dt AND f.k = d1.k AND f.sku_type = d1.sku_type\n"
                + "    AND f.is_dyn = d1.is_dyn\n"
                + "LEFT JOIN v_bench_large_dim_active d2\n"
                + "    ON f.dt = d2.dt AND f.k = d2.k AND f.sku_type = d2.sku_type\n"
                + "    AND f.is_dyn = d2.is_dyn\n"
                + "LEFT JOIN v_bench_large_attr_active a\n"
                + "    ON f.k = a.k AND f.category = a.category\n"
                + "WHERE f.sku_type = '1'\n"
                + "GROUP BY f.dt, f.k, f.category, d0.region, d1.mode_flag, d2.bu, a.brand");
        mockCandidateMtmv("bench_mv_large_target");
    }

    private void prepareManyOverlappingMvsCase() throws Exception {
        prepareLargeFanoutCteJoinCase();
        int overlapMvCount = Integer.getInteger("structInfoMapBenchmarkOverlapMvCount", 12);
        for (int i = 0; i < overlapMvCount; i++) {
            createMvByNereids("CREATE MATERIALIZED VIEW bench_mv_large_overlap_" + i + "\n"
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1')\n"
                    + "AS SELECT f.dt, f.k, f.category,\n"
                    + "          d.region AS region_" + i + ", a.brand AS brand_" + i + "\n"
                    + "FROM bench_large_fact f\n"
                    + "LEFT JOIN bench_large_dim d\n"
                    + "    ON f.dt = d.dt AND f.k = d.k AND f.sku_type = d.sku_type\n"
                    + "    AND f.is_dyn = d.is_dyn\n"
                    + "LEFT JOIN v_bench_large_attr_active a\n"
                    + "    ON f.k = a.k AND f.category = a.category\n"
                    + "WHERE f.sku_type = '1' AND f.category = 'A'");
            mockCandidateMtmv("bench_mv_large_overlap_" + i);
        }
    }

    private void dropManyOverlappingMvs() throws Exception {
        int overlapMvCount = Integer.getInteger("structInfoMapBenchmarkOverlapMvCount", 12);
        for (int i = 0; i < overlapMvCount; i++) {
            dropMvByNereids("drop materialized view if exists bench_mv_large_overlap_" + i);
        }
    }

    private String getLargeFanoutCteJoinQuerySql() {
        return "WITH base_detail AS (\n"
                + "    SELECT dt, k, sku_type, is_dyn, category, amount, qty\n"
                + "    FROM bench_large_fact\n"
                + "    WHERE sku_type = '1' AND dt >= '2026-01-01'\n"
                + "), base_rollup AS (\n"
                + "    SELECT dt, k, sku_type, is_dyn, category,\n"
                + "           SUM(amount) AS amount_sum, SUM(qty) AS qty_sum\n"
                + "    FROM base_detail\n"
                + "    GROUP BY dt, k, sku_type, is_dyn, category\n"
                + "), dim_active AS (\n"
                + "    SELECT dt, k, sku_type, is_dyn, region, bu, mode_flag\n"
                + "    FROM v_bench_large_dim_active\n"
                + "), dim_full_non_double_view AS (\n"
                + "    SELECT dt, k, sku_type, is_dyn, region, mode_flag\n"
                + "    FROM v_bench_large_dim_full_non_double\n"
                + "), attr_active AS (\n"
                + "    SELECT k, category, brand, owner_name\n"
                + "    FROM v_bench_large_attr_active\n"
                + "), joined_1 AS (\n"
                + "    SELECT b.dt, b.k, b.sku_type, b.is_dyn, b.category,\n"
                + "           b.amount_sum, b.qty_sum, d0.region AS region_main,\n"
                + "           d1.mode_flag AS mode_non_double, d2.bu AS active_bu,\n"
                + "           a0.brand AS brand_main, a0.owner_name AS owner_main\n"
                + "    FROM base_rollup b\n"
                + "    LEFT JOIN bench_large_dim d0\n"
                + "        ON b.dt = d0.dt AND b.k = d0.k AND b.sku_type = d0.sku_type\n"
                + "        AND b.is_dyn = d0.is_dyn\n"
                + "    LEFT JOIN dim_full_non_double_view d1\n"
                + "        ON b.dt = d1.dt AND b.k = d1.k AND b.sku_type = d1.sku_type\n"
                + "        AND b.is_dyn = d1.is_dyn\n"
                + "    LEFT JOIN dim_active d2\n"
                + "        ON b.dt = d2.dt AND b.k = d2.k AND b.sku_type = d2.sku_type\n"
                + "        AND b.is_dyn = d2.is_dyn\n"
                + "    LEFT JOIN attr_active a0\n"
                + "        ON b.k = a0.k AND b.category = a0.category\n"
                + "), post_filter AS (\n"
                + "    SELECT dt, k, category, region_main, mode_non_double, active_bu,\n"
                + "           brand_main, owner_main,\n"
                + "           amount_sum, qty_sum, amount_sum + qty_sum AS mix_metric\n"
                + "    FROM joined_1\n"
                + "    WHERE region_main IS NOT NULL OR brand_main IS NOT NULL\n"
                + ")\n"
                + "SELECT dt, k, category, region_main, mode_non_double, active_bu,\n"
                + "       brand_main, owner_main, SUM(amount_sum) AS final_amount, SUM(qty_sum) AS final_qty,\n"
                + "       SUM(mix_metric) AS final_mix\n"
                + "FROM post_filter\n"
                + "GROUP BY dt, k, category, region_main, mode_non_double, active_bu,\n"
                + "         brand_main, owner_main";
    }

    private String getExactTargetMvHitQuerySql(int wrapperStageCount, int postStageCount) {
        if (wrapperStageCount <= 0 && postStageCount <= 0) {
            return getExactTargetMvDirectQuerySql();
        }
        List<String> ctes = new java.util.ArrayList<>();
        String factSource = appendProjectionChain(ctes, "fact_stage",
                "SELECT dt, k, sku_type, is_dyn, category, amount, qty\n"
                        + "FROM bench_large_fact\n"
                        + "WHERE sku_type = '1'",
                "SELECT dt, k, sku_type, is_dyn, category, amount, qty\n"
                        + "FROM %s",
                wrapperStageCount);
        String dimFullSource = appendProjectionChain(ctes, "dim_full_stage",
                "SELECT dt, k, sku_type, is_dyn, region\n"
                        + "FROM bench_large_dim",
                "SELECT dt, k, sku_type, is_dyn, region\n"
                        + "FROM %s",
                wrapperStageCount);
        String dimFullNonDoubleViewSource = appendProjectionChain(ctes, "dim_full_non_double_view_stage",
                "SELECT dt, k, sku_type, is_dyn, mode_flag\n"
                        + "FROM v_bench_large_dim_full_non_double",
                "SELECT dt, k, sku_type, is_dyn, mode_flag\n"
                        + "FROM %s",
                wrapperStageCount);
        String dimActiveSource = appendProjectionChain(ctes, "dim_active_stage",
                "SELECT dt, k, sku_type, is_dyn, bu\n"
                        + "FROM v_bench_large_dim_active",
                "SELECT dt, k, sku_type, is_dyn, bu\n"
                        + "FROM %s",
                wrapperStageCount);
        String attrActiveSource = appendProjectionChain(ctes, "attr_active_stage",
                "SELECT k, category, brand\n"
                        + "FROM v_bench_large_attr_active",
                "SELECT k, category, brand\n"
                        + "FROM %s",
                wrapperStageCount);
        String joinedSource = appendProjectionChain(ctes, "joined_stage",
                "SELECT f.dt, f.k, f.category, f.amount, f.qty,\n"
                        + "       d0.region, d1.mode_flag, d2.bu, a.brand\n"
                        + "FROM " + factSource + " f\n"
                        + "LEFT JOIN " + dimFullSource + " d0\n"
                        + "    ON f.dt = d0.dt AND f.k = d0.k AND f.sku_type = d0.sku_type\n"
                        + "    AND f.is_dyn = d0.is_dyn\n"
                        + "LEFT JOIN " + dimFullNonDoubleViewSource + " d1\n"
                        + "    ON f.dt = d1.dt AND f.k = d1.k AND f.sku_type = d1.sku_type\n"
                        + "    AND f.is_dyn = d1.is_dyn\n"
                        + "LEFT JOIN " + dimActiveSource + " d2\n"
                        + "    ON f.dt = d2.dt AND f.k = d2.k AND f.sku_type = d2.sku_type\n"
                        + "    AND f.is_dyn = d2.is_dyn\n"
                        + "LEFT JOIN " + attrActiveSource + " a\n"
                        + "    ON f.k = a.k AND f.category = a.category",
                "SELECT dt, k, category, amount, qty, region, mode_flag, bu, brand\n"
                        + "FROM %s",
                postStageCount);

        return "WITH " + String.join(",\n", ctes) + "\n"
                + "SELECT dt, k, category,\n"
                + "       SUM(amount) AS amount_sum, SUM(qty) AS qty_sum,\n"
                + "       region, mode_flag, bu, brand\n"
                + "FROM " + joinedSource + "\n"
                + "GROUP BY dt, k, category, region, mode_flag, bu, brand";
    }

    private String getExactTargetMvDirectQuerySql() {
        return "SELECT f.dt, f.k, f.category,\n"
                + "       SUM(f.amount) AS amount_sum, SUM(f.qty) AS qty_sum,\n"
                + "       d0.region, d1.mode_flag, d2.bu, a.brand\n"
                + "FROM bench_large_fact f\n"
                + "LEFT JOIN bench_large_dim d0\n"
                + "    ON f.dt = d0.dt AND f.k = d0.k AND f.sku_type = d0.sku_type\n"
                + "    AND f.is_dyn = d0.is_dyn\n"
                + "LEFT JOIN v_bench_large_dim_full_non_double d1\n"
                + "    ON f.dt = d1.dt AND f.k = d1.k AND f.sku_type = d1.sku_type\n"
                + "    AND f.is_dyn = d1.is_dyn\n"
                + "LEFT JOIN v_bench_large_dim_active d2\n"
                + "    ON f.dt = d2.dt AND f.k = d2.k AND f.sku_type = d2.sku_type\n"
                + "    AND f.is_dyn = d2.is_dyn\n"
                + "LEFT JOIN v_bench_large_attr_active a\n"
                + "    ON f.k = a.k AND f.category = a.category\n"
                + "WHERE f.sku_type = '1'\n"
                + "GROUP BY f.dt, f.k, f.category, d0.region, d1.mode_flag, d2.bu, a.brand";
    }

    private String getExactAliasTargetMvHitQuerySql(int wrapperStageCount, int postStageCount) {
        if (wrapperStageCount <= 0 && postStageCount <= 0) {
            return getExactAliasTargetMvDirectQuerySql();
        }
        List<String> ctes = new java.util.ArrayList<>();
        String childSource = appendProjectionChain(ctes, "child_stage",
                "SELECT id, parent_id, score, kind\n"
                        + "FROM bench_alias_node",
                "SELECT id, parent_id, score, kind\n"
                        + "FROM %s",
                wrapperStageCount);
        String parentSource = appendProjectionChain(ctes, "parent_stage",
                "SELECT id, parent_id, score, kind\n"
                        + "FROM bench_alias_node",
                "SELECT id, parent_id, score, kind\n"
                        + "FROM %s",
                wrapperStageCount);
        String attrSource = appendProjectionChain(ctes, "attr_stage",
                "SELECT id, flag\n"
                        + "FROM bench_alias_attr",
                "SELECT id, flag\n"
                        + "FROM %s",
                wrapperStageCount);
        String joinedSource = appendProjectionChain(ctes, "joined_stage",
                "SELECT c.id, p.score AS parent_score, a.flag\n"
                        + "FROM " + childSource + " c\n"
                        + "LEFT JOIN " + parentSource + " p ON c.parent_id = p.id\n"
                        + "LEFT JOIN " + attrSource + " a ON c.id = a.id\n"
                        + "WHERE a.flag = 'Y' AND p.kind = 'P'",
                "SELECT id, parent_score, flag\n"
                        + "FROM %s",
                postStageCount);
        return "WITH " + String.join(",\n", ctes) + "\n"
                + "SELECT id, parent_score, flag\n"
                + "FROM " + joinedSource;
    }

    private String getExactAliasTargetMvDirectQuerySql() {
        return "SELECT c.id, p.score AS parent_score, a.flag\n"
                + "FROM bench_alias_node c\n"
                + "LEFT JOIN bench_alias_node p ON c.parent_id = p.id\n"
                + "LEFT JOIN bench_alias_attr a ON c.id = a.id\n"
                + "WHERE a.flag = 'Y' AND p.kind = 'P'";
    }

    private String appendProjectionChain(List<String> ctes, String prefix, String seedSql, String projectionSqlTemplate,
            int extraStages) {
        String currentName = prefix + "_0";
        ctes.add(currentName + " AS (\n" + indent(seedSql) + "\n)");
        for (int i = 1; i <= extraStages; i++) {
            String nextName = prefix + "_" + i;
            ctes.add(nextName + " AS (\n"
                    + indent(String.format(Locale.ROOT, projectionSqlTemplate, currentName))
                    + "\n)");
            currentName = nextName;
        }
        return currentName;
    }

    private String indent(String sql) {
        return "    " + sql.replace("\n", "\n    ");
    }

    private String getSuperComplexFanoutCteJoinQuerySql() {
        int branchCount = Integer.getInteger("structInfoMapBenchmarkSuperBranches", SUPER_COMPLEX_BRANCH_COUNT);
        StringBuilder sql = new StringBuilder();
        sql.append("WITH base_detail AS (\n")
                .append("    SELECT dt, k, sku_type, is_dyn, category, amount, qty\n")
                .append("    FROM bench_large_fact\n")
                .append("    WHERE sku_type = '1' AND dt >= '2026-01-01'\n")
                .append("), base_rollup AS (\n")
                .append("    SELECT dt, k, sku_type, is_dyn, category,\n")
                .append("           SUM(amount) AS amount_sum, SUM(qty) AS qty_sum\n")
                .append("    FROM base_detail\n")
                .append("    GROUP BY dt, k, sku_type, is_dyn, category\n")
                .append("), dim_active AS (\n")
                .append("    SELECT dt, k, sku_type, is_dyn, region, bu, mode_flag\n")
                .append("    FROM v_bench_large_dim_active\n")
                .append("), dim_full_non_double_view AS (\n")
                .append("    SELECT dt, k, sku_type, is_dyn, region, mode_flag\n")
                .append("    FROM v_bench_large_dim_full_non_double\n")
                .append("), attr_active AS (\n")
                .append("    SELECT k, category, brand, owner_name\n")
                .append("    FROM v_bench_large_attr_active\n");
        for (int i = 0; i < branchCount; i++) {
            String sourceName = i == 0 ? "base_rollup" : "joined_" + (i - 1);
            sql.append("), branch_source_").append(i).append(" AS (\n")
                    .append("    SELECT br.dt, br.k, br.sku_type, br.is_dyn, br.category,\n")
                    .append("           br.amount_sum, br.qty_sum\n")
                    .append("    FROM ").append(sourceName).append(" br\n")
                    .append("    WHERE (br.amount_sum >= 0 OR br.qty_sum >= 0)\n")
                    .append("), branch_rollup_").append(i).append(" AS (\n")
                    .append("    SELECT dt, k, sku_type, is_dyn, category,\n")
                    .append("           SUM(amount_sum) AS amount_sum,\n")
                    .append("           SUM(qty_sum) AS qty_sum,\n")
                    .append("           SUM(amount_sum + qty_sum) AS branch_mix\n")
                    .append("    FROM branch_source_").append(i).append("\n")
                    .append("    GROUP BY dt, k, sku_type, is_dyn, category\n")
                    .append("), joined_").append(i).append(" AS (\n")
                    .append("    SELECT b.dt, b.k, b.sku_type, b.is_dyn, b.category,\n")
                    .append("           b.amount_sum, b.qty_sum, b.branch_mix, d0.region AS region_main,\n")
                    .append("           d1.mode_flag AS mode_non_double, d2.bu AS active_bu,\n")
                    .append("           a0.brand AS brand_main, a0.owner_name AS owner_main\n")
                    .append("    FROM branch_rollup_").append(i).append(" b\n")
                    .append("    LEFT JOIN bench_large_dim d0\n")
                    .append("        ON b.dt = d0.dt AND b.k = d0.k AND b.sku_type = d0.sku_type\n")
                    .append("        AND b.is_dyn = d0.is_dyn\n")
                    .append("    LEFT JOIN dim_full_non_double_view d1\n")
                    .append("        ON b.dt = d1.dt AND b.k = d1.k AND b.sku_type = d1.sku_type\n")
                    .append("        AND b.is_dyn = d1.is_dyn\n")
                    .append("    LEFT JOIN dim_active d2\n")
                    .append("        ON b.dt = d2.dt AND b.k = d2.k AND b.sku_type = d2.sku_type\n")
                    .append("        AND b.is_dyn = d2.is_dyn\n")
                    .append("    LEFT JOIN attr_active a0\n")
                    .append("        ON b.k = a0.k AND b.category = a0.category\n");
        }
        sql.append("), post_filter AS (\n")
                .append("    SELECT dt, k, category, region_main, mode_non_double, active_bu,\n")
                .append("           brand_main, owner_main, amount_sum, qty_sum,\n")
                .append("           amount_sum + qty_sum + branch_mix AS mix_metric\n")
                .append("    FROM joined_").append(branchCount - 1).append("\n")
                .append("    WHERE region_main IS NOT NULL OR brand_main IS NOT NULL\n")
                .append(")\n")
                .append("SELECT dt, k, category, region_main, mode_non_double, active_bu,\n")
                .append("       brand_main, owner_main, SUM(amount_sum) AS final_amount, SUM(qty_sum) AS final_qty,\n")
                .append("       SUM(mix_metric) AS final_mix\n")
                .append("FROM post_filter\n")
                .append("GROUP BY dt, k, category, region_main, mode_non_double, active_bu,\n")
                .append("         brand_main, owner_main");
        return sql.toString();
    }

    private enum BenchmarkScale {
        ORDINARY,
        LARGE,
        SUPER
    }

    private static class MeasureResult {
        private final long elapsedNanos;
        private final int structInfoCount;

        private MeasureResult(long elapsedNanos, int structInfoCount) {
            this.elapsedNanos = elapsedNanos;
            this.structInfoCount = structInfoCount;
        }
    }

    private static class ExplainMeasureResult {
        private final long elapsedNanos;
        private final String explain;
        private final int planChars;

        private ExplainMeasureResult(long elapsedNanos, String explain) {
            this.elapsedNanos = elapsedNanos;
            this.explain = explain;
            this.planChars = explain.length();
        }
    }
}
