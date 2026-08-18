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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableIndexes;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.DdlException;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MatchPhrase;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CheckScoreUsageTest {
    private static final AtomicLong NEXT_INDEX_ID = new AtomicLong(1);

    @Test
    public void testAcceptsCompatibleCommonGramsPolicyForSelectedSniiIndex() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("domain_analyzer"))
                .thenReturn(true);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body", "body", "domain_analyzer")),
                "body", null);

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("domain_analyzer");
    }

    @Test
    public void testScorePushDownRuleInvokesPolicyAdmission() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body", "body", "missing_analyzer")),
                "body", null);
        LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN = scoreTopN(filter);
        Rule rule = new PushDownScoreTopNIntoOlapScan().buildRules().get(0);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> rule.transform(topN, Mockito.mock(CascadesContext.class)));
            Assertions.assertTrue(exception.getMessage().contains(
                    "Analyzer 'missing_analyzer' does not exist"), exception.getMessage());
        }
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
    }

    @Test
    public void testSearchScorePushDownRuleInvokesPolicyAdmission() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalFilter<LogicalOlapScan> filter = searchScoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        builtInIndex("idx_body", "body"),
                        index("idx_other", "other", "missing_analyzer")),
                "body", "other");
        LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN = scoreTopN(filter);
        Rule rule = new PushDownScoreTopNIntoOlapScan().buildRules().get(0);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> rule.transform(topN, Mockito.mock(CascadesContext.class)));
            Assertions.assertTrue(exception.getMessage().contains(
                    "Analyzer 'missing_analyzer' does not exist"), exception.getMessage());
        }
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
    }

    @Test
    public void testSearchRejectsCommonGramsPolicyOnSecondFieldForV3() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("domain_analyzer"))
                .thenReturn(true);
        LogicalFilter<LogicalOlapScan> filter = searchScoreFilter(
                table(TInvertedIndexFileStorageFormat.V3,
                        builtInIndex("idx_body", "body"),
                        index("idx_other", "other", "domain_analyzer")),
                "body", "other");

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(
                        filter, filter.child(), manager));
        Assertions.assertTrue(exception.getMessage().contains("supported only by SNII"),
                exception.getMessage());
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("domain_analyzer");
    }

    @Test
    public void testRejectsCommonGramsPolicyForSelectedV3Index() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("domain_analyzer"))
                .thenReturn(true);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.V3,
                        index("idx_body", "body", "domain_analyzer")),
                "body", null);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(
                        filter, filter.child(), manager));
        Assertions.assertTrue(exception.getMessage().contains("supported only by SNII"),
                exception.getMessage());
    }

    @Test
    public void testRejectsMissingSelectedAnalyzerPolicy() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body", "body", "missing_analyzer")),
                "body", null);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(
                        filter, filter.child(), manager));
        Assertions.assertTrue(exception.getMessage().contains(
                "Analyzer 'missing_analyzer' does not exist"), exception.getMessage());
    }

    @Test
    public void testRejectsIncompatibleSelectedCommonGramsPolicy() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException(
                "CommonGrams token-filter 'domain_grams' current state is PREPARING"))
                .when(manager).validateAnalyzerUsesCommonGrams("domain_analyzer");
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body", "body", "domain_analyzer")),
                "body", null);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(
                        filter, filter.child(), manager));
        Assertions.assertTrue(exception.getMessage().contains("current state is PREPARING"),
                exception.getMessage());
    }

    @Test
    public void testIgnoresUnselectedIndexWithMissingPolicy() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("plain_analyzer"))
                .thenReturn(false);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.V3,
                        index("idx_body", "body", "plain_analyzer"),
                        index("idx_other", "other", "missing_analyzer")),
                "body", null);

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("plain_analyzer");
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams("missing_analyzer");
    }

    @Test
    public void testExplicitAnalyzerSelectsTargetIndexOnSameColumn() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("target_analyzer"))
                .thenReturn(true);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body_first", "body", "first_analyzer"),
                        index("idx_body_target", "body", "target_analyzer")),
                "body", "target_analyzer");

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("target_analyzer");
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams("first_analyzer");
    }

    @Test
    public void testAnalyzerlessMatchSelectsFirstAnalyzedSiblingIndex() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("first_analyzer"))
                .thenReturn(false);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body_first", "body", "first_analyzer"),
                        index("idx_body_second", "body", "second_analyzer")),
                "body", null);

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("first_analyzer");
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams("second_analyzer");
    }

    @Test
    public void testSearchSelectsFirstAnalyzedSiblingIndex() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(manager.validateAnalyzerUsesCommonGrams("first_analyzer"))
                .thenReturn(false);
        LogicalFilter<LogicalOlapScan> filter = searchScoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII,
                        index("idx_body_first", "body", "first_analyzer"),
                        index("idx_body_second", "body", "second_analyzer")),
                "body");

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("first_analyzer");
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams("second_analyzer");
    }

    @Test
    public void testSelectedIndexWithoutExplicitAnalyzerSkipsPolicyAdmission() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.V3,
                        builtInIndex("idx_body", "body")),
                "body", null);

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams(Mockito.anyString());
    }

    @Test
    public void testNoSelectedIndexFallsBackWithoutPolicyAdmission() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(
                table(TInvertedIndexFileStorageFormat.SNII), "body", null);

        Assertions.assertDoesNotThrow(() -> CheckScoreUsage.checkScoringPolicyAdmission(
                filter, filter.child(), manager));
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams(Mockito.anyString());
    }

    @Test
    public void testMatchWithoutOriginalColumnFallsBackToBeAtRuleLevel() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        LogicalOlapScan scan = newScan(table(TInvertedIndexFileStorageFormat.SNII,
                index("idx_body", "body", "missing_analyzer")));
        SlotReference detachedSlot = new SlotReference("body", StringType.INSTANCE);
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(scan, detachedSlot, "missing_analyzer");

        assertScorePushDownSucceeds(filter, manager);
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams(Mockito.anyString());
    }

    @Test
    public void testMatchWithNonSlotLeftFallsBackToBeAtRuleLevel() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalOlapScan scan = newScan(table(TInvertedIndexFileStorageFormat.SNII,
                index("idx_body", "body", "missing_analyzer")));
        Alias wrappedSlot = new Alias(findSlot(scan, "body"), "wrapped_body");
        LogicalFilter<LogicalOlapScan> filter = scoreFilter(scan, wrappedSlot, null);

        assertScorePushDownSucceeds(filter, manager);
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams(Mockito.anyString());
    }

    @Test
    public void testSkippedMatchDoesNotSuppressAdmissionForLaterMatch() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalOlapScan scan = newScan(table(TInvertedIndexFileStorageFormat.SNII,
                index("idx_other", "other", "missing_analyzer")));
        Alias wrappedSlot = new Alias(findSlot(scan, "body"), "wrapped_body");
        MatchPhrase skippedMatch = new MatchPhrase(
                wrappedSlot, new StringLiteral("alpha beta"), null);
        MatchPhrase admittedMatch = new MatchPhrase(
                findSlot(scan, "other"), new StringLiteral("alpha beta"), null);
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(skippedMatch, admittedMatch), scan);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(filter, scan, manager));
        Assertions.assertTrue(exception.getMessage().contains("missing_analyzer"),
                exception.getMessage());
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
    }

    @Test
    public void testSearchWithNonSlotBindingFallsBackToBeAtRuleLevel() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        LogicalOlapScan scan = newScan(table(TInvertedIndexFileStorageFormat.SNII,
                index("idx_variant", "variant_body", "missing_analyzer")));
        ElementAt variantBinding = new ElementAt(
                findSlot(scan, "variant_body"), new StringLiteral("path"));
        LogicalFilter<LogicalOlapScan> filter = searchScoreFilter(
                scan, "variant_body.path:alpha", ImmutableList.of(variantBinding));

        assertScorePushDownSucceeds(filter, manager);
        Mockito.verify(manager, Mockito.never()).validateAnalyzerUsesCommonGrams(Mockito.anyString());
    }

    @Test
    public void testSkippedSearchBindingDoesNotSuppressAdmissionForLaterField() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        Mockito.doThrow(new DdlException("Analyzer 'missing_analyzer' does not exist"))
                .when(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
        LogicalOlapScan scan = newScan(table(TInvertedIndexFileStorageFormat.SNII,
                index("idx_other", "other", "missing_analyzer")));
        ElementAt variantBinding = new ElementAt(
                findSlot(scan, "variant_body"), new StringLiteral("path"));
        LogicalFilter<LogicalOlapScan> filter = searchScoreFilter(
                scan, "variant_body.path:alpha AND other:alpha",
                ImmutableList.of(variantBinding, findSlot(scan, "other")));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> CheckScoreUsage.checkScoringPolicyAdmission(filter, scan, manager));
        Assertions.assertTrue(exception.getMessage().contains("missing_analyzer"),
                exception.getMessage());
        Mockito.verify(manager).validateAnalyzerUsesCommonGrams("missing_analyzer");
    }

    private static LogicalFilter<LogicalOlapScan> scoreFilter(
            OlapTable table, String columnName, String analyzer) {
        LogicalOlapScan scan = newScan(table);
        return scoreFilter(scan, findSlot(scan, columnName), analyzer);
    }

    private static LogicalFilter<LogicalOlapScan> scoreFilter(
            LogicalOlapScan scan, Expression left, String analyzer) {
        MatchPhrase match = new MatchPhrase(left, new StringLiteral("alpha beta"), analyzer);
        return new LogicalFilter<>(ImmutableSet.of(match), scan);
    }

    private static LogicalFilter<LogicalOlapScan> searchScoreFilter(
            OlapTable table, String... columnNames) {
        LogicalOlapScan scan = newScan(table);
        List<Expression> slotChildren = new ArrayList<>();
        List<String> clauses = new ArrayList<>();
        for (String columnName : columnNames) {
            slotChildren.add(findSlot(scan, columnName));
            clauses.add(columnName + ":alpha");
        }
        return searchScoreFilter(scan, String.join(" AND ", clauses), slotChildren);
    }

    private static LogicalFilter<LogicalOlapScan> searchScoreFilter(
            LogicalOlapScan scan, String dsl, List<Expression> slotChildren) {
        SearchExpression search = new SearchExpression(
                dsl, SearchDslParser.parseDsl(dsl, null), slotChildren);
        return new LogicalFilter<>(ImmutableSet.of(search), scan);
    }

    private static LogicalOlapScan newScan(OlapTable table) {
        return new LogicalOlapScan(
                StatementScopeIdGenerator.newRelationId(), table, ImmutableList.of("db"));
    }

    private static SlotReference findSlot(LogicalOlapScan scan, String columnName) {
        return (SlotReference) scan.getOutput().stream()
                .filter(output -> output.getName().equals(columnName))
                .findFirst()
                .orElseThrow();
    }

    private static LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> scoreTopN(
            LogicalFilter<LogicalOlapScan> filter) {
        Alias scoreAlias = new Alias(new Score(), "score");
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(scoreAlias), filter);
        return new LogicalTopN<>(
                ImmutableList.of(new OrderKey(scoreAlias.toSlot(), false, false)),
                10, 0, project);
    }

    private static void assertScorePushDownSucceeds(
            LogicalFilter<LogicalOlapScan> filter, IndexPolicyMgr manager) {
        LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN = scoreTopN(filter);
        Rule rule = new PushDownScoreTopNIntoOlapScan().buildRules().get(0);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(
                    rule.transform(topN, Mockito.mock(CascadesContext.class)).isEmpty());
        }
    }

    private static OlapTable table(TInvertedIndexFileStorageFormat storageFormat,
            Index... indexes) {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("body", Type.STRING, false, AggregateType.NONE, "", ""),
                new Column("other", Type.STRING, false, AggregateType.NONE, "", ""),
                new Column("variant_body", Type.VARIANT, false, AggregateType.NONE, "", ""));
        OlapTable table = new OlapTable(10, "score_table", false, columns,
                KeysType.DUP_KEYS, new PartitionInfo(), null,
                new TableIndexes(ImmutableList.copyOf(indexes)));
        table.setIndexMeta(-1, "score_table", table.getFullSchema(),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setInvertedIndexFileStorageFormat(storageFormat);
        return table;
    }

    private static Index index(String name, String column, String analyzer) {
        return new Index(NEXT_INDEX_ID.getAndIncrement(), name, ImmutableList.of(column), IndexType.INVERTED,
                Map.of("analyzer", analyzer, "support_phrase", "true"), "");
    }

    private static Index builtInIndex(String name, String column) {
        return new Index(NEXT_INDEX_ID.getAndIncrement(), name, ImmutableList.of(column), IndexType.INVERTED,
                Map.of("parser", "standard", "support_phrase", "true"), "");
    }

}
