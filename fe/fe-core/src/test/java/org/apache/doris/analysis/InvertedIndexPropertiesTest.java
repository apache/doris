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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InvertedIndexPropertiesTest {

    @Test
    public void testRejectsTypeOnlyAndExplicitDefaultPinyinAnalyzers() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("pinyin_type_only")).thenReturn(new IndexPolicy(
                1, "pinyin_type_only", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "pinyin")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_defaults")).thenReturn(new IndexPolicy(
                2, "pinyin_defaults", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_first_letter", "true",
                        "keep_full_pinyin", "true", "keep_original", "false",
                        "ignore_pinyin_offset", "true", "limit_first_letter_length", "16")));
        Mockito.when(policyMgr.getPolicyByName("type_only_analyzer")).thenReturn(new IndexPolicy(
                3, "type_only_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_type_only")));
        Mockito.when(policyMgr.getPolicyByName("defaulted_analyzer")).thenReturn(new IndexPolicy(
                4, "defaulted_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "pinyin_defaults")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        IndexDefinition typeOnly = new IndexDefinition("idx_type_only", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "type_only_analyzer"), "");
        IndexDefinition explicitDefaults = new IndexDefinition("idx_explicit_defaults", false,
                List.of("content"), "INVERTED", Map.of("analyzer", "defaulted_analyzer"), "");

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                    StringType.INSTANCE, List.of(typeOnly, explicitDefaults)));
        }
    }

    @Test
    public void testRejectsAmbiguousOuterCharFiltersForSameAnalyzer() {
        IndexDefinition replaceA = new IndexDefinition("idx_replace_a", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "standard", "char_filter_type", "char_replace",
                        "char_filter_pattern", "a", "char_filter_replacement", "b"), "");
        IndexDefinition replaceX = new IndexDefinition("idx_replace_x", false, List.of("content"),
                "INVERTED", Map.of("analyzer", "standard", "char_filter_type", "char_replace",
                        "char_filter_pattern", "x", "char_filter_replacement", "y"), "");

        Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                StringType.INSTANCE, List.of(replaceA, replaceX)));

        IndexDefinition defaultA = new IndexDefinition("idx_default_a", false, List.of("content"),
                "INVERTED", Map.of("char_filter_type", "char_replace", "char_filter_pattern", "a",
                        "char_filter_replacement", "b"), "");
        IndexDefinition defaultX = new IndexDefinition("idx_default_x", false, List.of("content"),
                "INVERTED", Map.of("char_filter_type", "char_replace", "char_filter_pattern", "x",
                        "char_filter_replacement", "y"), "");
        Assertions.assertFalse(InvertedIndexUtil.canHaveMultipleInvertedIndexes(
                StringType.INSTANCE, List.of(defaultA, defaultX)));
    }

    @Test
    public void testExplicitBuiltinIkSelectsMatchingModeAndLowercase() {
        Column column = new Column("content", PrimitiveType.STRING);
        Index smartNoLowercase = new Index(10, "idx_smart_no_lowercase", List.of("content"),
                IndexType.INVERTED, Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false"), "");
        Index smart = new Index(11, "idx_smart", List.of("content"), IndexType.INVERTED,
                Map.of("parser", "ik", "parser_mode", "ik_smart"), "");
        Index maxWordNoLowercase = new Index(12, "idx_max_word_no_lowercase", List.of("content"),
                IndexType.INVERTED, Map.of("parser", "ik", "parser_mode", "ik_max_word", "lower_case", "false"), "");
        Index maxWord = new Index(13, "idx_max_word", List.of("content"), IndexType.INVERTED,
                Map.of("parser", "ik", "parser_mode", "ik_max_word"), "");
        OlapTable table = new OlapTable();
        table.setIndexes(List.of(smartNoLowercase, smart, maxWordNoLowercase, maxWord));

        Index selected = table.getInvertedIndex(column, List.of(), "ik");
        Assertions.assertSame(maxWord, selected);
        MatchPredicate predicate = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                new StringLiteral("清华大学"), new StringLiteral("清华"), Type.BOOLEAN,
                NullableMode.DEPEND_ON_ARGUMENT, selected, false, "ik");
        TExprNode node = new TExprNode();
        ExprToThriftVisitor.INSTANCE.visitMatchPredicate(predicate, node);
        Assertions.assertEquals("ik", node.getMatchPredicate().getAnalyzerName());
        Assertions.assertEquals("ik_max_word", node.getMatchPredicate().getParserMode());
        Assertions.assertTrue(node.getMatchPredicate().isParserLowercase());
        Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "ik"));
        Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                Map.of("analyzer", "ik", "lower_case", "false"), "ik"));
    }

    @Test
    public void testMatchSelectionPreservesExactAnalyzerSpelling() {
        MatchAny match = new MatchAny(new VarcharLiteral("abc def"), new VarcharLiteral("abc def"), " IK ");
        Assertions.assertAll(
                () -> Assertions.assertEquals("IK", match.getAnalyzer().orElseThrow()),
                () -> Assertions.assertEquals("IK",
                        match.withChildren(match.children()).getAnalyzer().orElseThrow()),
                () -> Assertions.assertEquals("IK",
                        AnalyzerSelector.select(Map.of("analyzer", "IK"), null).analyzer()),
                () -> Assertions.assertEquals("IK",
                        AnalyzerSelector.select(Map.of("analyzer", "IK"), "IK").analyzer()));
    }

    @Test
    public void testAnalyzerMatchingKeepsReplayedCaseDistinctPoliciesSeparate() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                30, "Legacy", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                31, "legacy", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "standard")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "Legacy"), "Legacy"));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "legacy"), "legacy"));
            Assertions.assertAll(
                    () -> Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                            Map.of("analyzer", "Legacy"), "legacy")),
                    () -> Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(
                            Map.of("analyzer", "legacy"), "Legacy")));
        }
    }

    @Test
    public void testMatchThriftKeepsImplicitAndExplicitLegacyIkBindings() {
        Index index = new Index(1, "idx_legacy_ik", List.of("content"),
                IndexType.INVERTED, Map.of("analyzer", "IK"), "");
        for (String analyzer : List.of("", "IK")) {
            MatchPredicate predicate = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                    new StringLiteral("abc def"), new StringLiteral("abc def"), Type.BOOLEAN,
                    NullableMode.DEPEND_ON_ARGUMENT, index, false, analyzer);
            TExprNode node = new TExprNode();
            ExprToThriftVisitor.INSTANCE.visitMatchPredicate(predicate, node);
            Assertions.assertEquals("IK", node.getMatchPredicate().getAnalyzerName());
        }
    }

    @Test
    public void testAnalyzerResolutionKeepsCanonicalBuiltinsAndExactLegacyBindings() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                40, "IK", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "keyword")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals("IK", InvertedIndexUtil.resolveAnalyzerName(" IK "));
            Assertions.assertEquals("ik", InvertedIndexUtil.resolveAnalyzerName("ik"));
            Assertions.assertEquals("standard", InvertedIndexUtil.resolveAnalyzerName(" StAnDaRd "));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "IK"), "IK"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "IK"), "ik"));
            Assertions.assertTrue(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "ik"), "ik"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("analyzer", "ik"), "IK"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "ik"));
            Assertions.assertFalse(InvertedIndexUtil.isAnalyzerMatched(Map.of("parser", "ik"), "IK"));
        }
    }

    private static void assertCheckCharFilterPropertiesThrows(Map<String, String> props, String expectedMessage) {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> InvertedIndexUtil.checkCharFilterProperties(props));
        Assertions.assertTrue(exception.getMessage().contains(expectedMessage), exception.getMessage());
    }

    // --- getInvertedIndexParser ---

    @Test
    public void testGetParserNull() {
        Assertions.assertEquals("none", InvertedIndexProperties.getInvertedIndexParser(null));
    }

    @Test
    public void testGetParserEmpty() {
        Assertions.assertEquals("none", InvertedIndexProperties.getInvertedIndexParser(new HashMap<>()));
    }

    @Test
    public void testGetParserByKey() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "unicode");
        Assertions.assertEquals("unicode", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    @Test
    public void testGetParserByAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("built_in_analyzer", "chinese");
        Assertions.assertEquals("chinese", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    @Test
    public void testGetParserKeyTakesPrecedenceOverAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "standard");
        props.put("built_in_analyzer", "chinese");
        Assertions.assertEquals("standard", InvertedIndexProperties.getInvertedIndexParser(props));
    }

    // --- getInvertedIndexParserMode ---

    @Test
    public void testGetParserModeNull() {
        Assertions.assertEquals("coarse_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(null));
    }

    @Test
    public void testGetParserModeExplicit() {
        Map<String, String> props = new HashMap<>();
        props.put("parser_mode", "fine_grained");
        Assertions.assertEquals("fine_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForIk() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "ik");
        Assertions.assertEquals("ik_smart",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForNonIk() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "unicode");
        Assertions.assertEquals("coarse_grained",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        Assertions.assertEquals("search",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeExplicitForKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "kuromoji");
        props.put("parser_mode", "extended");
        Assertions.assertEquals("extended",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeDefaultForAnalyzerKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "kuromoji");
        Assertions.assertEquals("search",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    @Test
    public void testGetParserModeExplicitForAnalyzerKuromoji() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "kuromoji");
        props.put("parser_mode", "normal");
        Assertions.assertEquals("normal",
                InvertedIndexProperties.getInvertedIndexParserMode(props));
    }

    // --- getInvertedIndexFieldPattern ---

    @Test
    public void testGetFieldPatternNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getInvertedIndexFieldPattern(null));
    }

    @Test
    public void testGetFieldPatternSet() {
        Map<String, String> props = new HashMap<>();
        props.put("field_pattern", "\\d+");
        Assertions.assertEquals("\\d+", InvertedIndexProperties.getInvertedIndexFieldPattern(props));
    }

    // --- getInvertedIndexParserLowercase ---

    @Test
    public void testGetLowercaseNull() {
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexParserLowercase(null));
    }

    @Test
    public void testGetLowercaseDefault() {
        Assertions.assertTrue(
                InvertedIndexProperties.getInvertedIndexParserLowercase(new HashMap<>()));
    }

    @Test
    public void testGetLowercaseFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("lower_case", "false");
        Assertions.assertFalse(InvertedIndexProperties.getInvertedIndexParserLowercase(props));
    }

    // --- getInvertedIndexParserStopwords ---

    @Test
    public void testGetStopwordsNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getInvertedIndexParserStopwords(null));
    }

    @Test
    public void testGetStopwordsSet() {
        Map<String, String> props = new HashMap<>();
        props.put("stopwords", "the,a,an");
        Assertions.assertEquals("the,a,an",
                InvertedIndexProperties.getInvertedIndexParserStopwords(props));
    }

    // --- getPreferredAnalyzer ---

    @Test
    public void testGetPreferredAnalyzerNull() {
        Assertions.assertEquals("", InvertedIndexProperties.getPreferredAnalyzer(null));
    }

    @Test
    public void testGetPreferredAnalyzerEmpty() {
        Assertions.assertEquals("", InvertedIndexProperties.getPreferredAnalyzer(new HashMap<>()));
    }

    @Test
    public void testGetPreferredAnalyzerByAnalyzer() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "my_analyzer");
        Assertions.assertEquals("my_analyzer", InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    @Test
    public void testGetPreferredAnalyzerByNormalizer() {
        Map<String, String> props = new HashMap<>();
        props.put("normalizer", "my_normalizer");
        Assertions.assertEquals("my_normalizer",
                InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    @Test
    public void testGetPreferredAnalyzerPrecedence() {
        Map<String, String> props = new HashMap<>();
        props.put("analyzer", "my_analyzer");
        props.put("normalizer", "my_normalizer");
        Assertions.assertEquals("my_analyzer", InvertedIndexProperties.getPreferredAnalyzer(props));
    }

    // --- getInvertedIndexCharFilter ---

    @Test
    public void testGetCharFilterNull() {
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(null).isEmpty());
    }

    @Test
    public void testGetCharFilterNoType() {
        Map<String, String> props = new HashMap<>();
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterUnknownType() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "unknown_type");
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterCharReplaceNoPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        Assertions.assertTrue(InvertedIndexProperties.getInvertedIndexCharFilter(props).isEmpty());
    }

    @Test
    public void testGetCharFilterCharReplaceWithPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "[0-9]");
        Map<String, String> result = InvertedIndexProperties.getInvertedIndexCharFilter(props);
        Assertions.assertEquals("char_replace", result.get("char_filter_type"));
        Assertions.assertEquals("[0-9]", result.get("char_filter_pattern"));
        Assertions.assertEquals(" ", result.get("char_filter_replacement"));
    }

    @Test
    public void testGetCharFilterCharReplaceWithCustomReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "[0-9]");
        props.put("char_filter_replacement", "_");
        Map<String, String> result = InvertedIndexProperties.getInvertedIndexCharFilter(props);
        Assertions.assertEquals("_", result.get("char_filter_replacement"));
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsEmptyReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must be a single non-empty character");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsMultiCharReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "xyz");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must be a single non-empty character");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsMissingType() {
        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(new HashMap<>()));
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsInvalidType() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "invalid");
        assertCheckCharFilterPropertiesThrows(props, "Invalid 'char_filter_type'");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsMissingPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        assertCheckCharFilterPropertiesThrows(props, "Missing 'char_filter_pattern' for 'char_replace' filter type");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsEmptyPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "");
        assertCheckCharFilterPropertiesThrows(props, "Missing 'char_filter_pattern' for 'char_replace' filter type");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsNonAsciiPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "中");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_pattern' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsLatin1Pattern() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", "é");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_pattern' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsNullReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(props));
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsNonAsciiReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "中");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesRejectsLatin1Replacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "é");
        assertCheckCharFilterPropertiesThrows(props, "'char_filter_replacement' must contain only ASCII characters");
    }

    @Test
    public void testCheckCharFilterPropertiesAllowsSingleAsciiReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "_");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkCharFilterProperties(props));
    }

    @Test
    public void testCheckInvertedIndexParserAllowsDotCharFilterPattern() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "english");
        props.put("char_filter_type", "char_replace");
        props.put("char_filter_pattern", ".");
        props.put("char_filter_replacement", "_");

        Assertions.assertDoesNotThrow(() -> InvertedIndexUtil.checkInvertedIndexParser("c",
                PrimitiveType.VARCHAR, props, TInvertedIndexFileStorageFormat.V2));
    }

    @Test
    public void testPlainCustomAnalyzerBehaviorRemainsUnchanged() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);

        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARIANT,
                        new HashMap<>(Map.of("analyzer", "plain_analyzer")),
                        TInvertedIndexFileStorageFormat.V3)));
    }

    @Test
    public void testResolvedCustomPolicyKeepsExactLegacyNameInIndexProperties() throws Exception {
        IndexPolicyMgr manager = Mockito.mock(IndexPolicyMgr.class);
        IndexPolicy exactPolicy = new IndexPolicy(
                1, "IK_SMART", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "standard"));
        Mockito.when(manager.getPolicyByName("IK_SMART")).thenReturn(exactPolicy);

        Map<String, String> properties = new HashMap<>(Map.of("analyzer", " IK_SMART "));
        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR, properties,
                        TInvertedIndexFileStorageFormat.V3)));

        Assertions.assertEquals("IK_SMART", properties.get("analyzer"));

        Map<String, String> normalizerProperties = new HashMap<>(Map.of("normalizer", " IK_SMART "));
        withIndexPolicyManager(manager, () -> Assertions.assertDoesNotThrow(
                () -> InvertedIndexUtil.checkInvertedIndexParser("c", PrimitiveType.VARCHAR,
                        normalizerProperties, TInvertedIndexFileStorageFormat.V3)));
        Assertions.assertEquals("IK_SMART", normalizerProperties.get("normalizer"));
    }

    // --- buildAnalyzerSqlFragment (migrated from InvertedIndexSqlGeneratorTest) ---

    @Test
    public void testBuildAnalyzerSqlFragmentNullOrBlank() {
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment(null));
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment(""));
        Assertions.assertEquals("", InvertedIndexProperties.buildAnalyzerSqlFragment("   "));
    }

    @Test
    public void testBuildAnalyzerSqlFragmentIdentifier() {
        Assertions.assertEquals(" USING ANALYZER standard",
                InvertedIndexProperties.buildAnalyzerSqlFragment("standard"));
        Assertions.assertEquals(" USING ANALYZER foo_bar",
                InvertedIndexProperties.buildAnalyzerSqlFragment("foo_bar"));
    }

    @Test
    public void testBuildAnalyzerSqlFragmentQuoted() {
        Assertions.assertEquals(" USING ANALYZER 'foo bar'",
                InvertedIndexProperties.buildAnalyzerSqlFragment("foo bar"));
        Assertions.assertEquals(" USING ANALYZER 'O''Reilly'",
                InvertedIndexProperties.buildAnalyzerSqlFragment("O'Reilly"));
    }

    private static void withIndexPolicyManager(IndexPolicyMgr manager, Runnable action) {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(manager);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            action.run();
        }
    }

    // --- norms ---

    @Test
    public void testNormsPropertyAccepted() throws AnalysisException {
        for (String value : new String[] {"true", "false"}) {
            Map<String, String> props = new HashMap<>();
            props.put("parser", "english");
            props.put("norms", value);
            InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.STRING, props,
                    TInvertedIndexFileStorageFormat.V2);
        }
    }

    @Test
    public void testNormsPropertyRejectsOtherValues() {
        Map<String, String> props = new HashMap<>();
        props.put("parser", "english");
        props.put("norms", "yes");
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.STRING, props,
                        TInvertedIndexFileStorageFormat.V2));
        Assertions.assertTrue(exception.getMessage().contains("norms must be true or false"),
                exception.getMessage());
    }

    // The SNII gate in checkInvertedIndexParser sees the parent VARIANT type on a whole-column
    // index and the sub-column type on a field_pattern index. Only the latter can be judged,
    // so VARIANT itself must pass.
    @Test
    public void testSniiAcceptsVariantColumn() throws AnalysisException {
        InvertedIndexUtil.checkInvertedIndexParser("col1", PrimitiveType.VARIANT,
                new HashMap<String, String>(), TInvertedIndexFileStorageFormat.SNII);
    }

}
