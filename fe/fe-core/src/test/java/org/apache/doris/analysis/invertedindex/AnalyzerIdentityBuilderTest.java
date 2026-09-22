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

package org.apache.doris.analysis.invertedindex;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyMgr;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AnalyzerIdentityBuilderTest {

    private Map<String, String> nonEmptyProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("k", "v");
        return properties;
    }

    @Test
    public void testEmptyPropertiesReturnsDefault() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                new HashMap<>(),
                "",
                "",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("__default__", identity);
    }

    @Test
    public void testBuiltInAnalyzerPreferred() {
        Assertions.assertFalse(IndexPolicy.BUILTIN_ANALYZERS.isEmpty());
        Iterator<String> iterator = IndexPolicy.BUILTIN_ANALYZERS.iterator();
        String analyzer = iterator.next();

        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                analyzer,
                "",
                "__default__",
                "none",
                null);
        Assertions.assertEquals(analyzer, identity);
    }

    @Test
    public void testBuiltInNormalizerPreferred() {
        Assertions.assertFalse(IndexPolicy.BUILTIN_NORMALIZERS.isEmpty());
        Iterator<String> iterator = IndexPolicy.BUILTIN_NORMALIZERS.iterator();
        String normalizer = iterator.next();

        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                normalizer,
                "",
                "__default__",
                "none",
                null);
        // BE builds the built-in as a keyword tokenizer with the built-in filter of the same name.
        Assertions.assertEquals(
                IndexPolicyTypeEnum.NORMALIZER.name() + ":token_filter=" + normalizer + ";", identity);
    }

    @Test
    public void testParserNoneReturnsDefault() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                "",
                "none",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("__default__", identity);
    }

    @Test
    public void testParserReturnsParserName() {
        String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                nonEmptyProperties(),
                "",
                "standard",
                "__default__",
                "none",
                null);
        Assertions.assertEquals("standard", identity);
    }

    @Test
    public void testNgramValidationLimitDoesNotChangeAnalyzerIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Map<String, String> tokenizerProps = new HashMap<>();
        tokenizerProps.put(IndexPolicy.PROP_TYPE, "ngram");
        tokenizerProps.put("min_gram", "1");
        tokenizerProps.put("max_gram", "2");
        tokenizerProps.put("max_ngram_diff", "7");
        IndexPolicy tokenizerWithLimit = new IndexPolicy(
                1, "ngram_with_limit", IndexPolicyTypeEnum.TOKENIZER, tokenizerProps);

        Map<String, String> equivalentTokenizerProps = new HashMap<>(tokenizerProps);
        equivalentTokenizerProps.remove("max_ngram_diff");
        IndexPolicy tokenizerWithoutLimit = new IndexPolicy(
                2, "ngram_without_limit", IndexPolicyTypeEnum.TOKENIZER, equivalentTokenizerProps);

        IndexPolicy analyzerWithLimit = analyzerPolicy(3, "analyzer_with_limit", "ngram_with_limit");
        IndexPolicy analyzerWithoutLimit = analyzerPolicy(4, "analyzer_without_limit", "ngram_without_limit");
        Mockito.when(policyMgr.getPolicyByName("ngram_with_limit")).thenReturn(tokenizerWithLimit);
        Mockito.when(policyMgr.getPolicyByName("ngram_without_limit")).thenReturn(tokenizerWithoutLimit);
        Mockito.when(policyMgr.getPolicyByName("analyzer_with_limit")).thenReturn(analyzerWithLimit);
        Mockito.when(policyMgr.getPolicyByName("analyzer_without_limit")).thenReturn(analyzerWithoutLimit);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String identityWithLimit = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "analyzer_with_limit", "", "__default__", "none", null);
            String identityWithoutLimit = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "analyzer_without_limit", "", "__default__", "none", null);
            Assertions.assertEquals(identityWithoutLimit, identityWithLimit);
        }
    }

    @Test
    public void testReplayedInvalidNgramDoesNotBlockValidReplacement() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Map<String, String> invalidProps = new HashMap<>();
        invalidProps.put(IndexPolicy.PROP_TYPE, "ngram");
        invalidProps.put("min_gram", "1");
        invalidProps.put("max_gram", "8");
        IndexPolicy invalidTokenizer = new IndexPolicy(
                10, "replayed_ngram", IndexPolicyTypeEnum.TOKENIZER, invalidProps);

        Map<String, String> replacementProps = new HashMap<>(invalidProps);
        replacementProps.put("max_ngram_diff", "7");
        IndexPolicy replacementTokenizer = new IndexPolicy(
                11, "replacement_ngram", IndexPolicyTypeEnum.TOKENIZER, replacementProps);
        IndexPolicy invalidAnalyzer = analyzerPolicy(12, "replayed_analyzer", "replayed_ngram");
        IndexPolicy replacementAnalyzer = analyzerPolicy(13, "replacement_analyzer", "replacement_ngram");
        policyMgr.replayCreateIndexPolicy(invalidTokenizer);
        policyMgr.replayCreateIndexPolicy(replacementTokenizer);
        policyMgr.replayCreateIndexPolicy(invalidAnalyzer);
        policyMgr.replayCreateIndexPolicy(replacementAnalyzer);

        Assertions.assertTrue(invalidTokenizer.isInvalid());
        Assertions.assertFalse(replacementTokenizer.isInvalid());
        Assertions.assertThrows(DdlException.class,
                () -> policyMgr.validateAnalyzerExists("replayed_analyzer"));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String invalidIdentity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "replayed_analyzer", "", "__default__", "none", null);
            String replacementIdentity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "replacement_analyzer", "", "__default__", "none", null);
            Assertions.assertNotEquals(invalidIdentity, replacementIdentity);
        }
    }

    @Test
    public void testReplayedLegacyLargeNgramAnalyzerRemainsUsable() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Map<String, String> legacyProps = new HashMap<>();
        legacyProps.put(IndexPolicy.PROP_TYPE, "ngram");
        legacyProps.put("min_gram", "2048");
        legacyProps.put("max_gram", "2048");
        IndexPolicy legacyTokenizer = new IndexPolicy(
                20, "legacy_large_ngram", IndexPolicyTypeEnum.TOKENIZER, legacyProps);
        IndexPolicy legacyAnalyzer = analyzerPolicy(
                21, "legacy_large_analyzer", "legacy_large_ngram");

        policyMgr.replayCreateIndexPolicy(legacyTokenizer);
        policyMgr.replayCreateIndexPolicy(legacyAnalyzer);

        Assertions.assertFalse(legacyTokenizer.isInvalid());
        Assertions.assertDoesNotThrow(
                () -> policyMgr.validateAnalyzerExists("legacy_large_analyzer"));
    }

    private IndexPolicy analyzerPolicy(long id, String name, String tokenizer) {
        Map<String, String> properties = new HashMap<>();
        properties.put(IndexPolicy.PROP_TOKENIZER, tokenizer);
        return new IndexPolicy(id, name, IndexPolicyTypeEnum.ANALYZER, properties);
    }

    @Test
    public void testReplayedExactIkAnalyzerUsesCustomIdentity() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(30, "IK", "standard"));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(31, "equivalent_standard", "standard"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String exact = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "IK"), "IK", "none", "__default__", "none", null);
            String equivalent = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "equivalent_standard"), "equivalent_standard",
                    "none", "__default__", "none", null);
            String builtin = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(equivalent, exact),
                    () -> Assertions.assertNotEquals(builtin, exact));
        }
    }

    @Test
    public void testNamedCharReplaceIdentityUsesEffectiveByteSet() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(50, "plain_keyword", "keyword"));
        String[] patterns = {"ab", "ba", "aabx", "x", " "};
        for (int i = 0; i < patterns.length; ++i) {
            String filter = "byte_filter_" + i;
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "char_replace");
            properties.put("pattern", patterns[i]);
            if (i < 4) {
                properties.put("replacement", "x");
            }
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    51 + i, filter, IndexPolicyTypeEnum.CHAR_FILTER, properties));
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    61 + i, "filtered_keyword_" + i, IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", filter)));
        }

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String canonical = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "filtered_keyword_0", "none", "__default__", "none", null);
            String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    nonEmptyProperties(), "plain_keyword", "none", "__default__", "none", null);
            Assertions.assertNotEquals(plain, canonical);
            for (int i = 1; i < patterns.length; ++i) {
                String identity = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "filtered_keyword_" + i, "none", "__default__", "none", null);
                Assertions.assertEquals(i < 3 ? canonical : plain, identity);
            }
        }
    }

    @Test
    public void testNamedCharReplaceIdentityAccountsForIkLowercase() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                70, "ascii_lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(71, "plain_smart", "ik_smart"));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                72, "filtered_smart", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "ascii_lower_a")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "plain_smart", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "filtered_smart", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testNamedCharReplaceContextUsesResolvedTokenizerAndFilterOrder() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                80, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                81, "a_to_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "a", "replacement", "b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                82, "named_max_word", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "ik_max_word")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                83, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "keyword")));
        long analyzerId = 84;
        for (String tokenizer : new String[] {"named_max_word", "ik_smart"}) {
            policyMgr.replayCreateIndexPolicy(analyzerPolicy(analyzerId++, "plain_" + tokenizer, tokenizer));
            policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                    analyzerId++, "filtered_" + tokenizer, IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", tokenizer, "char_filter", "lower_a")));
        }
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                88, "ordered", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "named_max_word", "char_filter", "lower_a,a_to_b")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                89, "later_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "named_max_word", "char_filter", "a_to_b")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (String tokenizer : new String[] {"named_max_word", "ik_smart"}) {
                String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "plain_" + tokenizer, "none", "__default__", "none", null);
                String filtered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                        nonEmptyProperties(), "filtered_" + tokenizer, "none", "__default__", "none", null);
                Assertions.assertEquals("named_max_word".equals(tokenizer), plain.equals(filtered));
            }
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "ordered", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "later_only", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testCaseFoldingCharFilterAbsorbsEarlierReplacement() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                90, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                91, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                92, "compose_only", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfc")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                93, "filtered_fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[a-z]")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                94, "fold_only", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                95, "lower_then_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                96, "compose_only_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "compose_only")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                97, "lower_then_compose", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,compose_only")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                98, "filtered_fold_analyzer", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "filtered_fold")));
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(
                99, "lower_then_filtered_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "char_filter", "lower_a,filtered_fold")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "fold_only", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_fold", "none", "__default__", "none", null));
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "compose_only_analyzer", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_compose", "none", "__default__", "none", null));
            Assertions.assertNotEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "filtered_fold_analyzer", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            nonEmptyProperties(), "lower_then_filtered_fold", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testBuiltinTokenizerIdentityIsCanonicalized() throws Exception {
        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        Assertions.assertEquals("ik_smart",
                resolve.invoke(null, "IK_SMART", IndexPolicyTypeEnum.TOKENIZER));
        Assertions.assertEquals("ik_smart",
                resolve.invoke(null, " IK_SMART ", IndexPolicyTypeEnum.TOKENIZER));
    }

    @Test
    public void testBuiltinFilterIdentitiesAreCanonicalized() throws Exception {
        Method resolveTokenFilters = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveTokenFilterIdentity", String.class);
        resolveTokenFilters.setAccessible(true);
        Method resolveCharFilters = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveCharFilterIdentity", String.class);
        resolveCharFilters.setAccessible(true);

        Assertions.assertEquals("pinyin", resolveTokenFilters.invoke(null, "PINYIN"));
        Assertions.assertEquals("icu_normalizer", resolveCharFilters.invoke(null, "ICU_NORMALIZER"));
        Assertions.assertEquals("lowercase,pinyin",
                resolveTokenFilters.invoke(null, "empty, lowercase, empty, pinyin"));
        Assertions.assertEquals("char_replace",
                resolveCharFilters.invoke(null, "empty, char_replace, empty"));
    }

    @Test
    public void testTypeOnlyIkPolicyMatchesBuiltinIdentity() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("named_ik")).thenReturn(new IndexPolicy(
                1, "named_ik", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(resolve.invoke(null, "ik_smart", IndexPolicyTypeEnum.TOKENIZER),
                    resolve.invoke(null, "named_ik", IndexPolicyTypeEnum.TOKENIZER));
        }
    }

    @Test
    public void testExplicitPinyinDefaultsMatchTypeOnlyIdentity() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("pinyin_type_only")).thenReturn(new IndexPolicy(
                1, "pinyin_type_only", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "pinyin")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_defaults")).thenReturn(new IndexPolicy(
                2, "pinyin_defaults", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_first_letter", "TRUE",
                        "keep_full_pinyin", "true", "keep_original", "FALSE",
                        "ignore_pinyin_offset", "true", "limit_first_letter_length", "016")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_non_default")).thenReturn(new IndexPolicy(
                3, "pinyin_non_default", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_original", "true")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Object typeOnly = resolve.invoke(null, "pinyin_type_only", IndexPolicyTypeEnum.TOKEN_FILTER);
            Assertions.assertEquals(typeOnly,
                    resolve.invoke(null, "pinyin_defaults", IndexPolicyTypeEnum.TOKEN_FILTER));
            Assertions.assertNotEquals(typeOnly,
                    resolve.invoke(null, "pinyin_non_default", IndexPolicyTypeEnum.TOKEN_FILTER));
        }
    }

    @Test
    public void testPinyinInactiveSettingsDoNotChangeIdentity() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("pinyin_default_offsets")).thenReturn(new IndexPolicy(
                1, "pinyin_default_offsets", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_fixed_ignored")).thenReturn(new IndexPolicy(
                2, "pinyin_fixed_ignored", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "fixed_pinyin_offset", "true")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_first_letter_disabled")).thenReturn(new IndexPolicy(
                3, "pinyin_first_letter_disabled", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_first_letter", "false")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_first_letter_inactive_settings"))
                .thenReturn(new IndexPolicy(
                        4, "pinyin_first_letter_inactive_settings", IndexPolicyTypeEnum.TOKEN_FILTER,
                        Map.of("type", "pinyin", "keep_first_letter", "false",
                                "limit_first_letter_length", "32",
                                "keep_none_chinese_in_first_letter", "false")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_none_chinese_disabled")).thenReturn(new IndexPolicy(
                5, "pinyin_none_chinese_disabled", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "pinyin", "keep_none_chinese", "false")));
        Mockito.when(policyMgr.getPolicyByName("pinyin_none_chinese_inactive_settings"))
                .thenReturn(new IndexPolicy(
                        6, "pinyin_none_chinese_inactive_settings", IndexPolicyTypeEnum.TOKEN_FILTER,
                        Map.of("type", "pinyin", "keep_none_chinese", "false",
                                "keep_none_chinese_together", "false",
                                "none_chinese_pinyin_tokenize", "false",
                                "fixed_pinyin_offset", "true")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    resolve.invoke(null, "pinyin_default_offsets", IndexPolicyTypeEnum.TOKEN_FILTER),
                    resolve.invoke(null, "pinyin_fixed_ignored", IndexPolicyTypeEnum.TOKEN_FILTER));
            Assertions.assertEquals(
                    resolve.invoke(null, "pinyin_first_letter_disabled", IndexPolicyTypeEnum.TOKEN_FILTER),
                    resolve.invoke(null, "pinyin_first_letter_inactive_settings",
                            IndexPolicyTypeEnum.TOKEN_FILTER));
            Assertions.assertEquals(
                    resolve.invoke(null, "pinyin_none_chinese_disabled", IndexPolicyTypeEnum.TOKEN_FILTER),
                    resolve.invoke(null, "pinyin_none_chinese_inactive_settings",
                            IndexPolicyTypeEnum.TOKEN_FILTER));
        }
    }

    @Test
    public void testSetValuedComponentSettingsAreCanonicalized() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("basic_ab")).thenReturn(new IndexPolicy(
                1, "basic_ab", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "ab")));
        Mockito.when(policyMgr.getPolicyByName("basic_baba")).thenReturn(new IndexPolicy(
                2, "basic_baba", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "baba")));
        Mockito.when(policyMgr.getPolicyByName("icu_unfiltered")).thenReturn(new IndexPolicy(
                3, "icu_unfiltered", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer")));
        Mockito.when(policyMgr.getPolicyByName("icu_empty_set")).thenReturn(new IndexPolicy(
                4, "icu_empty_set", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[]")));
        Mockito.when(policyMgr.getPolicyByName("icu_ab")).thenReturn(new IndexPolicy(
                5, "icu_ab", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[ab]")));
        Mockito.when(policyMgr.getPolicyByName("icu_ba")).thenReturn(new IndexPolicy(
                6, "icu_ba", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[ba]")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    resolve.invoke(null, "basic_ab", IndexPolicyTypeEnum.TOKENIZER),
                    resolve.invoke(null, "basic_baba", IndexPolicyTypeEnum.TOKENIZER));
            Assertions.assertEquals(
                    resolve.invoke(null, "icu_unfiltered", IndexPolicyTypeEnum.CHAR_FILTER),
                    resolve.invoke(null, "icu_empty_set", IndexPolicyTypeEnum.CHAR_FILTER));
            Assertions.assertEquals(
                    resolve.invoke(null, "icu_ab", IndexPolicyTypeEnum.CHAR_FILTER),
                    resolve.invoke(null, "icu_ba", IndexPolicyTypeEnum.CHAR_FILTER));
        }
    }

    @Test
    public void testCollectionValuedComponentSettingsUseEffectiveValues() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "ngram_ld", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "letter,digit"));
        replayComponent(policyMgr, 2, "ngram_dll", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "digit, letter,letter"));
        replayComponent(policyMgr, 3, "ngram_l", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "letter"));
        replayComponent(policyMgr, 4, "edge_custom_ab", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "edge_ngram", "token_chars", "letter,custom", "custom_token_chars", "ab"));
        replayComponent(policyMgr, 5, "edge_custom_bba", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "edge_ngram", "token_chars", "custom,letter,custom", "custom_token_chars", "bba"));
        replayComponent(policyMgr, 6, "group_ab", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "char_group", "tokenize_on_chars", "[a],[b],[whitespace]"));
        replayComponent(policyMgr, 7, "group_ba", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "char_group", "tokenize_on_chars", "[whitespace], [b],[a],[a]"));
        replayComponent(policyMgr, 8, "protect_ab", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "protected_words", "foo,bar"));
        replayComponent(policyMgr, 9, "protect_ba", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "protected_words", "bar, foo,foo"));
        replayComponent(policyMgr, 10, "types_ab", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "type_table", "[a => DIGIT],[b => ALPHA]"));
        replayComponent(policyMgr, 11, "types_overridden", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "type_table", "[b => ALPHA], [a => ALPHA],[a => DIGIT]"));
        replayComponent(policyMgr, 12, "types_last_alpha", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "type_table", "[a => DIGIT],[a => ALPHA]"));
        replayComponent(policyMgr, 13, "types_last_digit", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter", "type_table", "[a => ALPHA],[a => DIGIT]"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum tokenizer = IndexPolicyTypeEnum.TOKENIZER;
            IndexPolicyTypeEnum filter = IndexPolicyTypeEnum.TOKEN_FILTER;
            Assertions.assertAll(
                    () -> Assertions.assertEquals(resolve.invoke(null, "ngram_ld", tokenizer),
                            resolve.invoke(null, "ngram_dll", tokenizer)),
                    () -> Assertions.assertNotEquals(resolve.invoke(null, "ngram_ld", tokenizer),
                            resolve.invoke(null, "ngram_l", tokenizer)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "edge_custom_ab", tokenizer),
                            resolve.invoke(null, "edge_custom_bba", tokenizer)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "group_ab", tokenizer),
                            resolve.invoke(null, "group_ba", tokenizer)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "protect_ab", filter),
                            resolve.invoke(null, "protect_ba", filter)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "types_ab", filter),
                            resolve.invoke(null, "types_overridden", filter)),
                    () -> Assertions.assertNotEquals(resolve.invoke(null, "types_last_alpha", filter),
                            resolve.invoke(null, "types_last_digit", filter)));
        }
    }

    @Test
    public void testIcuNormalizerModeFollowsSelectedNormalizer() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "nfd_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfd"));
        replayComponent(policyMgr, 2, "nfd_decompose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "NFD", "mode", "decompose"));
        replayComponent(policyMgr, 3, "nfd_compose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfd", "mode", "compose"));
        replayComponent(policyMgr, 4, "nfkd_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfkd"));
        replayComponent(policyMgr, 5, "nfkd_decompose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfkd", "mode", "decompose"));
        replayComponent(policyMgr, 6, "nfc_decompose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfc", "mode", "decompose"));
        replayComponent(policyMgr, 7, "nfkc_decompose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfkc", "mode", "decompose"));
        replayComponent(policyMgr, 8, "nfc_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfc"));
        replayComponent(policyMgr, 9, "fold_decompose", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "mode", "decompose"));
        replayComponent(policyMgr, 10, "fold_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum charFilter = IndexPolicyTypeEnum.CHAR_FILTER;
            Object nfd = resolve.invoke(null, "nfd_default", charFilter);
            Object nfkd = resolve.invoke(null, "nfkd_default", charFilter);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(nfd, resolve.invoke(null, "nfd_decompose", charFilter)),
                    () -> Assertions.assertEquals(nfd, resolve.invoke(null, "nfd_compose", charFilter)),
                    () -> Assertions.assertEquals(nfd, resolve.invoke(null, "nfc_decompose", charFilter)),
                    () -> Assertions.assertEquals(nfkd, resolve.invoke(null, "nfkd_decompose", charFilter)),
                    () -> Assertions.assertEquals(nfkd, resolve.invoke(null, "nfkc_decompose", charFilter)),
                    () -> Assertions.assertNotEquals(nfd, resolve.invoke(null, "nfc_default", charFilter)),
                    () -> Assertions.assertNotEquals(resolve.invoke(null, "fold_default", charFilter),
                            resolve.invoke(null, "fold_decompose", charFilter)));
        }
    }

    @Test
    public void testExplicitEmptyUnicodeSetFilterMatchesAbsentFilter() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "char_default", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        replayComponent(policyMgr, 2, "char_empty_string", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", ""));
        replayComponent(policyMgr, 3, "token_default", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer"));
        replayComponent(policyMgr, 4, "token_empty_string", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", ""));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    resolve.invoke(null, "char_default", IndexPolicyTypeEnum.CHAR_FILTER),
                    resolve.invoke(null, "char_empty_string", IndexPolicyTypeEnum.CHAR_FILTER));
            Assertions.assertEquals(
                    resolve.invoke(null, "token_default", IndexPolicyTypeEnum.TOKEN_FILTER),
                    resolve.invoke(null, "token_empty_string", IndexPolicyTypeEnum.TOKEN_FILTER));
        }
    }

    private static void replayComponent(IndexPolicyMgr policyMgr, long id, String name,
            IndexPolicyTypeEnum type, Map<String, String> properties) {
        policyMgr.replayCreateIndexPolicy(new IndexPolicy(id, name, type, properties));
    }

    private static String namedAnalyzerIdentity(String analyzer) {
        return AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", analyzer), analyzer, "none", "__default__", "none", null);
    }

    private static String namedAnalyzerIdentityWithOuterLowerA(String analyzer) {
        return AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", analyzer, "char_filter_type", "char_replace",
                        "char_filter_pattern", "A", "char_filter_replacement", "a"),
                analyzer, "none", "__default__", "none", null);
    }

    @Test
    public void testKeywordBufferSizeDoesNotChangeIdentity() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "keyword_plain", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "keyword"));
        replayComponent(policyMgr, 2, "keyword_256", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "keyword", "buffer_size", "256"));
        replayComponent(policyMgr, 3, "keyword_512", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "keyword", "buffer_size", "512"));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(4, "keyword_plain_analyzer", "keyword_plain"));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(5, "keyword_256_analyzer", "keyword_256"));
        policyMgr.replayCreateIndexPolicy(analyzerPolicy(6, "keyword_512_analyzer", "keyword_512"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String plain = namedAnalyzerIdentity("keyword_plain_analyzer");
            Assertions.assertAll(
                    () -> Assertions.assertEquals(plain, namedAnalyzerIdentity("keyword_256_analyzer")),
                    () -> Assertions.assertEquals(plain, namedAnalyzerIdentity("keyword_512_analyzer")));
        }
    }

    @Test
    public void testCaseFoldCarriesThroughNonInteractingCharReplace() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a"));
        replayComponent(policyMgr, 2, "x_to_y", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "x", "replacement", "y"));
        replayComponent(policyMgr, 3, "a_to_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "a", "replacement", "b"));
        replayComponent(policyMgr, 4, "upper_a_to_z", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "z"));
        replayComponent(policyMgr, 5, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        String[][] analyzers = {
                {"x_then_fold", "x_to_y,fold"},
                {"lower_x_fold", "lower_a,x_to_y,fold"},
                {"ab_then_fold", "a_to_b,fold"},
                {"lower_ab_fold", "lower_a,a_to_b,fold"},
                {"az_then_fold", "upper_a_to_z,fold"},
                {"lower_az_fold", "lower_a,upper_a_to_z,fold"}};
        long id = 10;
        for (String[] analyzer : analyzers) {
            replayComponent(policyMgr, id++, analyzer[0], IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", analyzer[1]));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("x_then_fold"),
                            namedAnalyzerIdentity("lower_x_fold")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("ab_then_fold"),
                            namedAnalyzerIdentity("lower_ab_fold")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("az_then_fold"),
                            namedAnalyzerIdentity("lower_az_fold")));
        }
    }

    @Test
    public void testOuterCharFilterAbsorbedByCustomCaseFoldingPipeline() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        replayComponent(policyMgr, 2, "x_to_y", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "x", "replacement", "y"));
        replayComponent(policyMgr, 3, "group_on_upper_a", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "char_group", "tokenize_on_chars", "[A]"));
        replayComponent(policyMgr, 4, "ngram_custom_a", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "ngram", "token_chars", "custom", "custom_token_chars", "a"));
        replayComponent(policyMgr, 5, "keyword_lower_1", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "lowercase"));
        replayComponent(policyMgr, 6, "keyword_lower_2", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "token_filter", "lowercase"));
        replayComponent(policyMgr, 7, "keyword_plain", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword"));
        replayComponent(policyMgr, 8, "fold_first", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "fold"));
        replayComponent(policyMgr, 9, "x_then_fold", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "keyword", "char_filter", "x_to_y,fold"));
        replayComponent(policyMgr, 10, "group_lower", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "group_on_upper_a", "token_filter", "lowercase"));
        replayComponent(policyMgr, 11, "ngram_custom_lower", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ngram_custom_a", "token_filter", "lowercase"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("keyword_lower_2"),
                            namedAnalyzerIdentityWithOuterLowerA("keyword_lower_1")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("fold_first"),
                            namedAnalyzerIdentityWithOuterLowerA("fold_first")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("x_then_fold"),
                            namedAnalyzerIdentityWithOuterLowerA("x_then_fold")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("keyword_plain"),
                            namedAnalyzerIdentityWithOuterLowerA("keyword_plain")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("group_lower"),
                            namedAnalyzerIdentityWithOuterLowerA("group_lower")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("ngram_custom_lower"),
                            namedAnalyzerIdentityWithOuterLowerA("ngram_custom_lower")));
        }
    }

    private static String namedNormalizerIdentity(String normalizer) {
        return AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("normalizer", normalizer), normalizer, "none", "__default__", "none", null);
    }

    private static String namedNormalizerIdentityWithOuterLowerA(String normalizer) {
        return AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("normalizer", normalizer, "char_filter_type", "char_replace",
                        "char_filter_pattern", "A", "char_filter_replacement", "a"),
                normalizer, "none", "__default__", "none", null);
    }

    /** Replay the same pinyin settings as a tokenizer and as a token filter under distinct names. */
    private static void replayPinyinPair(IndexPolicyMgr policyMgr, long id, String name,
            Map<String, String> properties) {
        Map<String, String> pinyin = new HashMap<>(properties);
        pinyin.put("type", "pinyin");
        replayComponent(policyMgr, id, name + "_tk", IndexPolicyTypeEnum.TOKENIZER, pinyin);
        replayComponent(policyMgr, id + 100, name + "_tf", IndexPolicyTypeEnum.TOKEN_FILTER, pinyin);
    }

    private static Object pinyinIdentity(Method resolve, String name, IndexPolicyTypeEnum type)
            throws Exception {
        String suffix = type == IndexPolicyTypeEnum.TOKENIZER ? "_tk" : "_tf";
        return resolve.invoke(null, name + suffix, type);
    }

    @Test
    public void testPinyinNoneChineseInJoinedFullPinyinFollowsJoinedGate() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayPinyinPair(policyMgr, 1, "pinyin_plain", Map.of());
        replayPinyinPair(policyMgr, 2, "pinyin_ascii_in_joined",
                Map.of("keep_none_chinese_in_joined_full_pinyin", "true"));
        replayPinyinPair(policyMgr, 3, "pinyin_joined", Map.of("keep_joined_full_pinyin", "true"));
        replayPinyinPair(policyMgr, 4, "pinyin_joined_with_ascii",
                Map.of("keep_joined_full_pinyin", "true", "keep_none_chinese_in_joined_full_pinyin", "true"));
        replayPinyinPair(policyMgr, 5, "pinyin_buffer_only",
                Map.of("keep_first_letter", "false", "keep_full_pinyin", "false",
                        "none_chinese_pinyin_tokenize", "false"));
        replayPinyinPair(policyMgr, 6, "pinyin_buffer_only_ascii",
                Map.of("keep_first_letter", "false", "keep_full_pinyin", "false",
                        "none_chinese_pinyin_tokenize", "false",
                        "keep_none_chinese_in_joined_full_pinyin", "true"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (IndexPolicyTypeEnum type : new IndexPolicyTypeEnum[] {
                    IndexPolicyTypeEnum.TOKENIZER, IndexPolicyTypeEnum.TOKEN_FILTER}) {
                Assertions.assertAll(type.name(),
                        () -> Assertions.assertEquals(pinyinIdentity(resolve, "pinyin_plain", type),
                                pinyinIdentity(resolve, "pinyin_ascii_in_joined", type)),
                        () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_joined", type),
                                pinyinIdentity(resolve, "pinyin_joined_with_ascii", type)));
            }
            // Only the pinyin tokenizer consults the flag for an untokenized ASCII buffer that
            // no other setting emits.
            Assertions.assertAll(
                    () -> Assertions.assertNotEquals(
                            pinyinIdentity(resolve, "pinyin_buffer_only", IndexPolicyTypeEnum.TOKENIZER),
                            pinyinIdentity(resolve, "pinyin_buffer_only_ascii", IndexPolicyTypeEnum.TOKENIZER)),
                    () -> Assertions.assertEquals(
                            pinyinIdentity(resolve, "pinyin_buffer_only", IndexPolicyTypeEnum.TOKEN_FILTER),
                            pinyinIdentity(resolve, "pinyin_buffer_only_ascii", IndexPolicyTypeEnum.TOKEN_FILTER)));
        }
    }

    @Test
    public void testPinyinSeparateNoneChinesePathIgnoresPinyinTokenize() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayPinyinPair(policyMgr, 1, "pinyin_plain", Map.of());
        replayPinyinPair(policyMgr, 2, "pinyin_separate", Map.of("keep_none_chinese_together", "false"));
        replayPinyinPair(policyMgr, 3, "pinyin_separate_untokenized",
                Map.of("keep_none_chinese_together", "false", "none_chinese_pinyin_tokenize", "false",
                        "fixed_pinyin_offset", "true"));
        replayPinyinPair(policyMgr, 4, "pinyin_untokenized", Map.of("none_chinese_pinyin_tokenize", "false"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (IndexPolicyTypeEnum type : new IndexPolicyTypeEnum[] {
                    IndexPolicyTypeEnum.TOKENIZER, IndexPolicyTypeEnum.TOKEN_FILTER}) {
                Assertions.assertAll(type.name(),
                        () -> Assertions.assertEquals(pinyinIdentity(resolve, "pinyin_separate", type),
                                pinyinIdentity(resolve, "pinyin_separate_untokenized", type)),
                        () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_plain", type),
                                pinyinIdentity(resolve, "pinyin_untokenized", type)));
            }
        }
    }

    @Test
    public void testEmptyUnicodeSetIcuNormalizerFoldsCase() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a"));
        replayComponent(policyMgr, 2, "fold_empty", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[]"));
        replayComponent(policyMgr, 3, "fold_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[b]"));
        replayComponent(policyMgr, 4, "fold_bad", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[b"));
        String[][] analyzers = {
                {"fold_empty_only", "fold_empty"},
                {"lower_fold_empty", "lower_a,fold_empty"},
                {"fold_b_only", "fold_b"},
                {"lower_fold_b", "lower_a,fold_b"},
                {"fold_bad_only", "fold_bad"},
                {"lower_fold_bad", "lower_a,fold_bad"}};
        long id = 10;
        for (String[] analyzer : analyzers) {
            replayComponent(policyMgr, id++, analyzer[0], IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", analyzer[1]));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("fold_empty_only"),
                            namedAnalyzerIdentity("lower_fold_empty")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("fold_empty_only"),
                            namedAnalyzerIdentityWithOuterLowerA("fold_empty_only")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("fold_b_only"),
                            namedAnalyzerIdentity("lower_fold_b")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("fold_bad_only"),
                            namedAnalyzerIdentity("lower_fold_bad")));
        }
    }

    @Test
    public void testOuterCharFilterAbsorbedByNormalizerPipeline() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a"));
        replayComponent(policyMgr, 2, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        replayComponent(policyMgr, 3, "norm_lower_1", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase"));
        replayComponent(policyMgr, 4, "norm_lower_2", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase"));
        replayComponent(policyMgr, 5, "norm_lower_a_then_lowercase", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("char_filter", "lower_a", "token_filter", "lowercase"));
        replayComponent(policyMgr, 6, "norm_fold", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("char_filter", "fold"));
        replayComponent(policyMgr, 7, "norm_ascii_1", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "asciifolding"));
        replayComponent(policyMgr, 8, "norm_ascii_2", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "asciifolding"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedNormalizerIdentity("norm_lower_2"),
                            namedNormalizerIdentityWithOuterLowerA("norm_lower_1")),
                    () -> Assertions.assertEquals(namedNormalizerIdentity("norm_lower_2"),
                            namedNormalizerIdentity("norm_lower_a_then_lowercase")),
                    () -> Assertions.assertEquals(namedNormalizerIdentity("norm_fold"),
                            namedNormalizerIdentityWithOuterLowerA("norm_fold")),
                    () -> Assertions.assertNotEquals(namedNormalizerIdentity("norm_ascii_2"),
                            namedNormalizerIdentityWithOuterLowerA("norm_ascii_1")));
        }
    }

    @Test
    public void testOuterCharFilterAbsorbedThroughAsciiTransparentTokenFilters() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "ascii", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding"));
        replayComponent(policyMgr, 2, "ascii_keep", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding", "preserve_original", "true"));
        replayComponent(policyMgr, 3, "nfc_filter", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer", "name", "nfc"));
        replayComponent(policyMgr, 4, "icu_fold", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer"));
        replayComponent(policyMgr, 5, "icu_fold_empty", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[]"));
        replayComponent(policyMgr, 6, "icu_fold_b", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "icu_normalizer", "unicode_set_filter", "[b]"));
        replayComponent(policyMgr, 7, "wd", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "word_delimiter"));
        String[][] analyzers = {
                {"ascii_lower_1", "ascii,lowercase"},
                {"ascii_lower_2", "ascii,lowercase"},
                {"ascii_keep_lower", "ascii_keep,lowercase"},
                {"nfc_lower", "nfc_filter,lowercase"},
                {"icu_fold_only", "icu_fold"},
                {"ascii_icu_fold_empty", "ascii,icu_fold_empty"},
                {"ascii_only", "ascii"},
                {"nfc_only", "nfc_filter"},
                {"icu_fold_b_only", "icu_fold_b"},
                {"wd_lower", "wd,lowercase"}};
        long id = 10;
        for (String[] analyzer : analyzers) {
            replayComponent(policyMgr, id++, analyzer[0], IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "token_filter", analyzer[1]));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("ascii_lower_2"),
                            namedAnalyzerIdentityWithOuterLowerA("ascii_lower_1")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("ascii_keep_lower"),
                            namedAnalyzerIdentityWithOuterLowerA("ascii_keep_lower")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("nfc_lower"),
                            namedAnalyzerIdentityWithOuterLowerA("nfc_lower")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("icu_fold_only"),
                            namedAnalyzerIdentityWithOuterLowerA("icu_fold_only")),
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("ascii_icu_fold_empty"),
                            namedAnalyzerIdentityWithOuterLowerA("ascii_icu_fold_empty")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("ascii_only"),
                            namedAnalyzerIdentityWithOuterLowerA("ascii_only")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("nfc_only"),
                            namedAnalyzerIdentityWithOuterLowerA("nfc_only")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("icu_fold_b_only"),
                            namedAnalyzerIdentityWithOuterLowerA("icu_fold_b_only")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("wd_lower"),
                            namedAnalyzerIdentityWithOuterLowerA("wd_lower")));
        }
    }

    @Test
    public void testExplicitComponentDefaultsMatchBuiltinIdentity() throws Exception {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("asciifolding_defaults")).thenReturn(new IndexPolicy(
                1, "asciifolding_defaults", IndexPolicyTypeEnum.TOKEN_FILTER,
                Map.of("type", "asciifolding", "preserve_original", "FALSE")));
        Mockito.when(policyMgr.getPolicyByName("edge_ngram_defaults")).thenReturn(new IndexPolicy(
                2, "edge_ngram_defaults", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "edge_ngram", "min_gram", "01", "max_gram", "002")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals("asciifolding",
                    resolve.invoke(null, "asciifolding_defaults", IndexPolicyTypeEnum.TOKEN_FILTER));
            Assertions.assertEquals("edge_ngram",
                    resolve.invoke(null, "edge_ngram_defaults", IndexPolicyTypeEnum.TOKENIZER));
        }
    }

    @Test
    public void testNamedEmptyFiltersAreOmittedFromIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("empty_token_filter")).thenReturn(new IndexPolicy(
                1, "empty_token_filter", IndexPolicyTypeEnum.TOKEN_FILTER, Map.of("type", "empty")));
        Mockito.when(policyMgr.getPolicyByName("empty_char_filter")).thenReturn(new IndexPolicy(
                2, "empty_char_filter", IndexPolicyTypeEnum.CHAR_FILTER, Map.of("type", "empty")));
        Mockito.when(policyMgr.getPolicyByName("plain")).thenReturn(new IndexPolicy(
                3, "plain", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("padded")).thenReturn(new IndexPolicy(
                4, "padded", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "ik_smart", "token_filter", "empty_token_filter,empty",
                        "char_filter", "empty,empty_char_filter")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertEquals(
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            Map.of("analyzer", "plain"), "plain", "none", "__default__", "none", null),
                    AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                            Map.of("analyzer", "padded"), "padded", "none", "__default__", "none", null));
        }
    }

    @Test
    public void testOuterCharFilterDistinguishesNamedAnalyzerIdentity() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart")).thenReturn(new IndexPolicy(
                1, "smart", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String plain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart"), "smart", "none", "__default__", "none", null);
            String filtered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "smart", "none", "__default__", "none", null);
            String defaultReplacement = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertNotEquals(plain, filtered);
            Assertions.assertEquals(filtered, defaultReplacement);
        }
    }

    @Test
    public void testOuterCharFilterUsesCanonicalIkBaseAndByteSetSemantics() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart")).thenReturn(new IndexPolicy(
                1, "smart", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("shadowed_tokenizer")).thenReturn(new IndexPolicy(
                2, "shadowed_tokenizer", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Mockito.when(policyMgr.getPolicyByName("shadowed")).thenReturn(new IndexPolicy(
                3, "shadowed", IndexPolicyTypeEnum.ANALYZER,
                Map.of("tokenizer", "shadowed_tokenizer")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String legacyFiltered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "", "ik", "__default__", "none", null);
            String namedFiltered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-", "char_filter_replacement", " "),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(legacyFiltered, namedFiltered);

            String plainSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart"), "smart", "none", "__default__", "none", null);
            String lowercasedByIk = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "AaA", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(plainSmart, lowercasedByIk);

            String reordered = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "_--a", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            String canonical = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart", "char_filter_type", "char_replace",
                            "char_filter_pattern", "-_", "char_filter_replacement", "a"),
                    "smart", "none", "__default__", "none", null);
            Assertions.assertEquals(canonical, reordered);

            String lowerCaseDisabled = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false",
                            "char_filter_type", "char_replace", "char_filter_pattern", "A",
                            "char_filter_replacement", "a"),
                    "", "ik", "__default__", "none", null);
            String lowerCaseDisabledPlain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart", "lower_case", "false"),
                    "", "ik", "__default__", "none", null);
            Assertions.assertNotEquals(lowerCaseDisabledPlain, lowerCaseDisabled);

            String shadowed = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "shadowed", "char_filter_type", "char_replace",
                            "char_filter_pattern", "A", "char_filter_replacement", "a"),
                    "shadowed", "none", "__default__", "none", null);
            String shadowedPlain = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "shadowed"), "shadowed", "none", "__default__", "none", null);
            Assertions.assertNotEquals(shadowedPlain, shadowed);
        }
    }

    @Test
    public void testLegacyIkIdentityMatchesEquivalentCustomAnalyzer() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("smart_analyzer")).thenReturn(new IndexPolicy(
                1, "smart_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Mockito.when(policyMgr.getPolicyByName("max_word_analyzer")).thenReturn(new IndexPolicy(
                2, "max_word_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_max_word")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String customSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart_analyzer"), "smart_analyzer", "none", "__default__", "none", null);
            String customMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "max_word_analyzer"), "max_word_analyzer", "none",
                    "__default__", "none", null);

            Assertions.assertEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik"), "", "ik", "__default__", "none", null));
            Assertions.assertEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_smart"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "parser_mode", "ik_max_word"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null));
            Assertions.assertNotEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "char_filter_type", "char_replace", "char_filter_pattern", "-"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customSmart, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik", "lower_case", "false"), "", "ik",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik", "char_filter_type", "char_replace", "char_filter_pattern", "-"), "ik", "none",
                    "__default__", "none", null));
            Assertions.assertNotEquals(customMaxWord, AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "ik", "lower_case", "false"), "ik", "none",
                    "__default__", "none", null));
        }
    }

    @Test
    public void testLegacyIkIdentityIgnoresShadowingTokenizerPolicy() {
        IndexPolicyMgr policyMgr = Mockito.mock(IndexPolicyMgr.class);
        Mockito.when(policyMgr.getPolicyByName("ik_smart")).thenReturn(new IndexPolicy(
                1, "ik_smart", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "standard")));
        Mockito.when(policyMgr.getPolicyByName("smart_analyzer")).thenReturn(new IndexPolicy(
                2, "smart_analyzer", IndexPolicyTypeEnum.ANALYZER, Map.of("tokenizer", "ik_smart")));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String shadowedCustom = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("analyzer", "smart_analyzer"), "smart_analyzer", "none",
                    "__default__", "none", null);
            String legacy = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                    Map.of("parser", "ik"), "", "ik", "__default__", "none", null);
            Assertions.assertNotEquals(shadowedCustom, legacy);
        }
    }

    @Test
    public void testDisabledLowercaseIkModesHaveDistinctIdentities() {
        String analyzerMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", "ik", "lower_case", "false"), "ik", "none",
                "__default__", "none", null);
        String legacySmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik", "lower_case", "false"), "", "ik",
                "__default__", "none", null);
        String legacyMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik", "parser_mode", "ik_max_word", "lower_case", "false"),
                "", "ik", "__default__", "none", null);
        String lowercaseMaxWord = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("analyzer", "ik"), "ik", "none", "__default__", "none", null);
        String lowercaseSmart = AnalyzerIdentityBuilder.buildAnalyzerIdentity(
                Map.of("parser", "ik"), "", "ik", "__default__", "none", null);

        Assertions.assertNotEquals(legacySmart, analyzerMaxWord);
        Assertions.assertEquals(legacyMaxWord, analyzerMaxWord);
        Assertions.assertNotEquals(lowercaseMaxWord, analyzerMaxWord);
        Assertions.assertNotEquals(lowercaseSmart, legacySmart);
    }

    private static Method resolveComponentIdentityMethod() throws Exception {
        Method resolve = AnalyzerIdentityBuilder.class.getDeclaredMethod(
                "resolveComponentIdentity", String.class, IndexPolicyTypeEnum.class);
        resolve.setAccessible(true);
        return resolve;
    }

    @Test
    public void testPinyinTokenizerTrimWhitespaceOnlyAffectsOriginalCandidate() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayPinyinPair(policyMgr, 1, "pinyin_plain", Map.of());
        replayPinyinPair(policyMgr, 2, "pinyin_untrimmed", Map.of("trim_whitespace", "false"));
        replayPinyinPair(policyMgr, 3, "pinyin_original", Map.of("keep_original", "true"));
        replayPinyinPair(policyMgr, 4, "pinyin_original_untrimmed",
                Map.of("keep_original", "true", "trim_whitespace", "false"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum tokenizer = IndexPolicyTypeEnum.TOKENIZER;
            IndexPolicyTypeEnum filter = IndexPolicyTypeEnum.TOKEN_FILTER;
            Assertions.assertAll(
                    () -> Assertions.assertEquals(pinyinIdentity(resolve, "pinyin_plain", tokenizer),
                            pinyinIdentity(resolve, "pinyin_untrimmed", tokenizer)),
                    () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_original", tokenizer),
                            pinyinIdentity(resolve, "pinyin_original_untrimmed", tokenizer)),
                    () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_plain", filter),
                            pinyinIdentity(resolve, "pinyin_untrimmed", filter)));
        }
    }

    @Test
    public void testPinyinDedupFlagIgnoredWhenAtMostOneCandidateIsEmitted() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        Map<String, String> joinedOnly = Map.of("keep_first_letter", "false", "keep_full_pinyin", "false",
                "keep_none_chinese", "false", "keep_joined_full_pinyin", "true");
        replayPinyinPair(policyMgr, 1, "pinyin_joined_only", joinedOnly);
        Map<String, String> joinedOnlyDedup = new HashMap<>(joinedOnly);
        joinedOnlyDedup.put("remove_duplicated_term", "true");
        replayPinyinPair(policyMgr, 2, "pinyin_joined_only_dedup", joinedOnlyDedup);
        Map<String, String> nothing = Map.of("keep_first_letter", "false", "keep_full_pinyin", "false",
                "keep_none_chinese", "false");
        replayPinyinPair(policyMgr, 3, "pinyin_nothing", nothing);
        Map<String, String> nothingDedup = new HashMap<>(nothing);
        nothingDedup.put("remove_duplicated_term", "true");
        replayPinyinPair(policyMgr, 4, "pinyin_nothing_dedup", nothingDedup);
        Map<String, String> fullPinyin = new HashMap<>(joinedOnly);
        fullPinyin.put("keep_full_pinyin", "true");
        replayPinyinPair(policyMgr, 5, "pinyin_full", fullPinyin);
        Map<String, String> fullPinyinDedup = new HashMap<>(fullPinyin);
        fullPinyinDedup.put("remove_duplicated_term", "true");
        replayPinyinPair(policyMgr, 6, "pinyin_full_dedup", fullPinyinDedup);
        Map<String, String> separateChinese = new HashMap<>(joinedOnly);
        separateChinese.put("keep_separate_chinese", "true");
        replayPinyinPair(policyMgr, 7, "pinyin_chinese", separateChinese);
        Map<String, String> separateChineseDedup = new HashMap<>(separateChinese);
        separateChineseDedup.put("remove_duplicated_term", "true");
        replayPinyinPair(policyMgr, 8, "pinyin_chinese_dedup", separateChineseDedup);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (IndexPolicyTypeEnum type : new IndexPolicyTypeEnum[] {
                    IndexPolicyTypeEnum.TOKENIZER, IndexPolicyTypeEnum.TOKEN_FILTER}) {
                Assertions.assertAll(type.name(),
                        () -> Assertions.assertEquals(pinyinIdentity(resolve, "pinyin_joined_only", type),
                                pinyinIdentity(resolve, "pinyin_joined_only_dedup", type)),
                        () -> Assertions.assertEquals(pinyinIdentity(resolve, "pinyin_nothing", type),
                                pinyinIdentity(resolve, "pinyin_nothing_dedup", type)),
                        () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_full", type),
                                pinyinIdentity(resolve, "pinyin_full_dedup", type)),
                        () -> Assertions.assertNotEquals(pinyinIdentity(resolve, "pinyin_chinese", type),
                                pinyinIdentity(resolve, "pinyin_chinese_dedup", type)));
            }
        }
    }

    @Test
    public void testCharReplaceReplacementByteDoesNotBlockCaseFold() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a"));
        replayComponent(policyMgr, 2, "x_to_upper_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "Ax", "replacement", "A"));
        replayComponent(policyMgr, 3, "upper_a_to_b", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "Ab", "replacement", "b"));
        replayComponent(policyMgr, 4, "fold", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "icu_normalizer"));
        String[][] analyzers = {
                {"x_upper_fold", "x_to_upper_a,fold"},
                {"lower_x_upper_fold", "lower_a,x_to_upper_a,fold"},
                {"upper_b_fold", "upper_a_to_b,fold"},
                {"lower_upper_b_fold", "lower_a,upper_a_to_b,fold"}};
        long id = 10;
        for (String[] analyzer : analyzers) {
            replayComponent(policyMgr, id++, analyzer[0], IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", analyzer[1]));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(namedAnalyzerIdentity("x_upper_fold"),
                            namedAnalyzerIdentity("lower_x_upper_fold")),
                    () -> Assertions.assertNotEquals(namedAnalyzerIdentity("upper_b_fold"),
                            namedAnalyzerIdentity("lower_upper_b_fold")));
        }
    }

    @Test
    public void testFilteredCaseFoldAbsorbsReplacementOfCodePointInsideSet() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "lower_a", IndexPolicyTypeEnum.CHAR_FILTER,
                Map.of("type", "char_replace", "pattern", "A", "replacement", "a"));
        String[][] folds = {
                {"fold_upper_a", "[A]"},
                {"fold_upper_b", "[B]"},
                {"fold_lower_a", "[a]"},
                {"fold_both_a", "[Aa]"},
                {"fold_upper_a_mark", "[A\\u0301]"}};
        long id = 10;
        for (String[] fold : folds) {
            replayComponent(policyMgr, id++, fold[0], IndexPolicyTypeEnum.CHAR_FILTER,
                    Map.of("type", "icu_normalizer", "unicode_set_filter", fold[1]));
            replayComponent(policyMgr, id++, fold[0] + "_tf", IndexPolicyTypeEnum.TOKEN_FILTER,
                    Map.of("type", "icu_normalizer", "unicode_set_filter", fold[1]));
            replayComponent(policyMgr, id++, fold[0] + "_only", IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", fold[0]));
            replayComponent(policyMgr, id++, "lower_" + fold[0], IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "char_filter", "lower_a," + fold[0]));
            replayComponent(policyMgr, id++, fold[0] + "_tf_only", IndexPolicyTypeEnum.ANALYZER,
                    Map.of("tokenizer", "keyword", "token_filter", fold[0] + "_tf"));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            for (String fold : new String[] {"fold_upper_a", "fold_both_a"}) {
                Assertions.assertAll(fold,
                        () -> Assertions.assertEquals(namedAnalyzerIdentity(fold + "_only"),
                                namedAnalyzerIdentity("lower_" + fold)),
                        () -> Assertions.assertEquals(namedAnalyzerIdentity(fold + "_only"),
                                namedAnalyzerIdentityWithOuterLowerA(fold + "_only")),
                        () -> Assertions.assertEquals(namedAnalyzerIdentity(fold + "_tf_only"),
                                namedAnalyzerIdentityWithOuterLowerA(fold + "_tf_only")));
            }
            // Outside the set nothing folds, and a set holding a combining mark but not the
            // lower-case letter changes which span the mark is normalized with.
            for (String fold : new String[] {"fold_upper_b", "fold_lower_a", "fold_upper_a_mark"}) {
                Assertions.assertAll(fold,
                        () -> Assertions.assertNotEquals(namedAnalyzerIdentity(fold + "_only"),
                                namedAnalyzerIdentity("lower_" + fold)),
                        () -> Assertions.assertNotEquals(namedAnalyzerIdentity(fold + "_only"),
                                namedAnalyzerIdentityWithOuterLowerA(fold + "_only")),
                        () -> Assertions.assertNotEquals(namedAnalyzerIdentity(fold + "_tf_only"),
                                namedAnalyzerIdentityWithOuterLowerA(fold + "_tf_only")));
            }
        }
    }

    @Test
    public void testWordDelimiterTypeTableDropsRulesRestatingBeClassification() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        String[][] tables = {
                {"wd_absent", null},
                {"wd_b_digit", "[b => DIGIT]"},
                {"wd_a_lower_b_digit", "[a => LOWER],[b => DIGIT]"},
                {"wd_ascii_defaults_b_digit", "[A => UPPER],[1 => DIGIT],[_ => SUBWORD_DELIM],[b => DIGIT]"},
                {"wd_a_lower", "[a => LOWER]"},
                {"wd_a_alpha", "[a => ALPHA]"},
                {"wd_a_digit", "[a => DIGIT]"},
                {"wd_a_b_lower", "[a => LOWER],[b => LOWER]"},
                {"wd_latin_lower_b_digit", "[é => LOWER],[b => DIGIT]"}};
        long id = 1;
        for (String[] table : tables) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "word_delimiter");
            if (table[1] != null) {
                properties.put("type_table", table[1]);
            }
            replayComponent(policyMgr, id++, table[0], IndexPolicyTypeEnum.TOKEN_FILTER, properties);
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum filter = IndexPolicyTypeEnum.TOKEN_FILTER;
            Object absent = resolve.invoke(null, "wd_absent", filter);
            Object digitB = resolve.invoke(null, "wd_b_digit", filter);
            Object lowerA = resolve.invoke(null, "wd_a_lower", filter);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(digitB, resolve.invoke(null, "wd_a_lower_b_digit", filter)),
                    () -> Assertions.assertEquals(digitB,
                            resolve.invoke(null, "wd_ascii_defaults_b_digit", filter)),
                    () -> Assertions.assertNotEquals(digitB,
                            resolve.invoke(null, "wd_latin_lower_b_digit", filter)),
                    // BE seeds an explicit table from u_charType but its default table from
                    // u_isULowercase/u_isUUppercase/u_isdigit, which differ for Latin-1 code points.
                    () -> Assertions.assertNotEquals(absent, lowerA),
                    () -> Assertions.assertNotEquals(absent, resolve.invoke(null, "wd_a_b_lower", filter)),
                    () -> Assertions.assertNotEquals(lowerA, resolve.invoke(null, "wd_a_alpha", filter)),
                    () -> Assertions.assertNotEquals(lowerA, resolve.invoke(null, "wd_a_digit", filter)),
                    () -> Assertions.assertNotEquals(absent, resolve.invoke(null, "wd_a_digit", filter)));
        }
    }

    @Test
    public void testBasicExtraCharsIgnoreAlphanumerics() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "basic_plain", IndexPolicyTypeEnum.TOKENIZER, Map.of("type", "basic"));
        replayComponent(policyMgr, 2, "basic_alnum", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "A0z"));
        replayComponent(policyMgr, 3, "basic_dash", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "-"));
        replayComponent(policyMgr, 4, "basic_dash_alnum", IndexPolicyTypeEnum.TOKENIZER,
                Map.of("type", "basic", "extra_chars", "A-0"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum tokenizer = IndexPolicyTypeEnum.TOKENIZER;
            Object plain = resolve.invoke(null, "basic_plain", tokenizer);
            Object dash = resolve.invoke(null, "basic_dash", tokenizer);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(plain, resolve.invoke(null, "basic_alnum", tokenizer)),
                    () -> Assertions.assertEquals(dash, resolve.invoke(null, "basic_dash_alnum", tokenizer)),
                    () -> Assertions.assertNotEquals(plain, dash));
        }
    }

    @Test
    public void testNgramCustomTokenCharsCoveredByNamedClassesAreDropped() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        String[][] tokenizers = {
                {"ngram_letter", "letter", null},
                {"ngram_letter_custom_a", "letter,custom", "A"},
                {"ngram_letter_custom_dash", "letter,custom", "-"},
                {"ngram_letter_custom_a_dash", "custom,letter", "A-"},
                {"ngram_letter_custom_latin", "letter,custom", "é"},
                {"ngram_digit", "digit", null},
                {"ngram_digit_custom_a", "digit,custom", "A"},
                {"ngram_classes", "digit,punctuation,symbol", null},
                {"ngram_classes_custom", "digit,punctuation,symbol,custom", "7-$"}};
        long id = 1;
        for (String[] tokenizer : tokenizers) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "ngram");
            properties.put("token_chars", tokenizer[1]);
            if (tokenizer[2] != null) {
                properties.put("custom_token_chars", tokenizer[2]);
            }
            replayComponent(policyMgr, id++, tokenizer[0], IndexPolicyTypeEnum.TOKENIZER, properties);
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum tokenizer = IndexPolicyTypeEnum.TOKENIZER;
            Object letter = resolve.invoke(null, "ngram_letter", tokenizer);
            Object letterDash = resolve.invoke(null, "ngram_letter_custom_dash", tokenizer);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(letter, resolve.invoke(null, "ngram_letter_custom_a", tokenizer)),
                    () -> Assertions.assertEquals(letterDash,
                            resolve.invoke(null, "ngram_letter_custom_a_dash", tokenizer)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "ngram_classes", tokenizer),
                            resolve.invoke(null, "ngram_classes_custom", tokenizer)),
                    () -> Assertions.assertNotEquals(letter, letterDash),
                    () -> Assertions.assertNotEquals(letter,
                            resolve.invoke(null, "ngram_letter_custom_latin", tokenizer)),
                    () -> Assertions.assertNotEquals(resolve.invoke(null, "ngram_digit", tokenizer),
                            resolve.invoke(null, "ngram_digit_custom_a", tokenizer)));
        }
    }

    @Test
    public void testCharGroupLiteralsCoveredByCategoriesAreDropped() throws Exception {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        String[][] tokenizers = {
                {"group_letter", "[letter]"},
                {"group_letter_a", "[letter],[A]"},
                {"group_letter_dash", "[letter],[-]"},
                {"group_letter_latin", "[letter],[é]"},
                {"group_digit", "[digit]"},
                {"group_digit_a", "[digit],[A]"},
                {"group_classes", "[digit],[punctuation],[symbol]"},
                {"group_classes_literals", "[7],[digit],[-],[punctuation],[$],[symbol]"}};
        long id = 1;
        for (String[] tokenizer : tokenizers) {
            replayComponent(policyMgr, id++, tokenizer[0], IndexPolicyTypeEnum.TOKENIZER,
                    Map.of("type", "char_group", "tokenize_on_chars", tokenizer[1]));
        }
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        Method resolve = resolveComponentIdentityMethod();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            IndexPolicyTypeEnum tokenizer = IndexPolicyTypeEnum.TOKENIZER;
            Object letter = resolve.invoke(null, "group_letter", tokenizer);
            Assertions.assertAll(
                    () -> Assertions.assertEquals(letter, resolve.invoke(null, "group_letter_a", tokenizer)),
                    () -> Assertions.assertEquals(resolve.invoke(null, "group_classes", tokenizer),
                            resolve.invoke(null, "group_classes_literals", tokenizer)),
                    () -> Assertions.assertNotEquals(letter, resolve.invoke(null, "group_letter_dash", tokenizer)),
                    () -> Assertions.assertNotEquals(letter,
                            resolve.invoke(null, "group_letter_latin", tokenizer)),
                    () -> Assertions.assertNotEquals(resolve.invoke(null, "group_digit", tokenizer),
                            resolve.invoke(null, "group_digit_a", tokenizer)));
        }
    }

    @Test
    public void testBuiltinNormalizerIdentityMatchesEquivalentCustomNormalizer() {
        IndexPolicyMgr policyMgr = new IndexPolicyMgr();
        replayComponent(policyMgr, 1, "norm_lower", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "lowercase"));
        replayComponent(policyMgr, 2, "norm_ascii", IndexPolicyTypeEnum.NORMALIZER,
                Map.of("token_filter", "asciifolding"));
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getIndexPolicyMgr()).thenReturn(policyMgr);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            String custom = namedNormalizerIdentity("norm_lower");
            Assertions.assertAll(
                    () -> Assertions.assertEquals(custom, namedNormalizerIdentity("lowercase")),
                    () -> Assertions.assertEquals(custom, namedNormalizerIdentityWithOuterLowerA("lowercase")),
                    () -> Assertions.assertNotEquals(namedNormalizerIdentity("norm_ascii"),
                            namedNormalizerIdentity("lowercase")));
        }
    }
}
